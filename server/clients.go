package server

import (
	"context"
	"encoding/binary"
	"etcd-light/shared"
	"log"
	"net"
	"sync/atomic"

	"github.com/google/uuid"
)

// handleClientConnection manages a single client connection with dedicated channels for processing
// This function runs for the lifetime of a client connection and handles all client communication
func (s *Server) handleClientConnection(conn net.Conn) {
	// Buffers for message processing
	buffer := make([]byte, shared.MaxBatchSize) // Main message buffer
	leaderBuffer := make([]byte, 1)             // Leader status response buffer

	// Initialize client structure with communication channels
	client := shared.Client{
		Connection:     conn,                       // Network connection to client
		Channel:        make(chan func(), 1000000), // General task channel (high capacity)
		ProposeChannel: make(chan func(), 100000),  // Raft proposal channel (medium capacity)
	}

	// Start goroutine for processing general client tasks
	go func() {
		for task := range client.Channel {
			task() // Execute the task
		}
	}()

	// Start goroutine for processing Raft proposals
	go func() {
		for task := range client.ProposeChannel {
			task() // Execute the proposal task
		}
	}()

	// Main message processing loop
	for {
		// Read message length prefix (4 bytes)
		if err := shared.Read(conn, buffer[:4]); err != nil {
			return // Connection closed or error
		}

		// Decode message length
		amount := binary.LittleEndian.Uint32(buffer[:4])

		// Read the actual message data
		if err := shared.Read(conn, buffer[:amount]); err != nil {
			log.Printf("Error reading message: %v", err)
			return // Connection error
		}

		// Handle leader status queries
		if buffer[0] == shared.OpLeader {
			// Check if this node is currently the Raft leader
			if s.node.Status().Lead == s.config.ID {
				leaderBuffer[0] = 1 // This node is leader
			} else {
				leaderBuffer[0] = 0 // This node is follower
			}

			// Send leader status response
			err := shared.Write(conn, leaderBuffer)
			if err != nil {
				panic(err) // Critical write failure
			}
			continue // Continue to next message
		}

		// Process regular client messages
		s.handleClientMessage(client, buffer[:amount])
	}
}

// handleClientMessage processes individual client messages and routes them appropriately
// Handles both read and write operations with Raft consensus where needed
func (s *Server) handleClientMessage(client shared.Client, data []byte) {
	messageId := uuid.New()        // Generate unique ID for message tracking
	ownerId := uint32(s.config.ID) // This node's ID

	size := len(data) + 21 // Calculate message size (data + overhead)
	op := data[0]          // Extract operation type

	// Handle read operations (OpReadMemory or OpRead)
	if op == shared.OpReadMemory || op == shared.OpRead {
		// Create message copy with Raft message wrapper
		dataCopy := make([]byte, shared.MessageSize(data)+1)
		dataCopy[0] = shared.OpMessage
		shared.PackMessage(messageId, ownerId, data, dataCopy[1:])

		// Store pending read for response tracking
		s.senders.Store(messageId, shared.PendingRead{Client: client, Data: dataCopy})

		size = 21 // Fixed size for read index requests
		buffer := s.GetBuffer(size)
		binary.LittleEndian.PutUint32(buffer[0:4], uint32(size))
		copy(buffer[5:21], messageId[:16])

		// If this node is the leader, handle read index consensus
		if s.leader == ownerId {
			buffer[4] = shared.OpReadIndexReq

			// Register read index in global store
			readIndexLock.Lock()
			readIndexes[messageId] = &ReadIndexStore{Acks: 1, Proposer: ownerId}
			readIndexLock.Unlock()

			// Broadcast read index request to all peers
			for peerIndex := range s.peerAddresses {
				if peerIndex == int(s.config.ID-1) {
					continue // Skip self
				}
				// Round-robin connection selection
				connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIndex], 1) % uint32(s.flags.NumPeerConnections)
				peer := s.peerConnections[peerIndex][connIdx]

				// Create buffer copy for each peer
				bufferCopy := s.GetBuffer(21)
				copy(bufferCopy, buffer[:21])

				// Send to peer via channel
				peer.Channel <- func() {
					if err := shared.Write(*peer.Connection, bufferCopy[:21]); err != nil {
						log.Printf("Write error to peer %d: %v", s.leader, err)
					}
					s.PutBuffer(bufferCopy)
				}
			}
			s.PutBuffer(buffer)
		} else {
			// Forward read index request to leader
			buffer[4] = shared.OpReadIndexForward

			// Select connection to leader
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[s.leader-1], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[s.leader-1][connIdx]

			// Send forwarded request
			peer.Channel <- func() {
				if err := shared.Write(*peer.Connection, buffer[:size]); err != nil {
					log.Printf("Write error to peer %d: %v", s.leader, err)
				}
				s.PutBuffer(buffer)
			}
		}
	} else {
		// Handle write operations
		s.senders.Store(messageId, client) // Store client for response

		if s.leader == ownerId {
			// This node is leader - propose to Raft cluster
			dataCopy := make([]byte, shared.MessageSize(data)+1)
			dataCopy[0] = shared.OpMessage
			shared.PackMessage(messageId, ownerId, data, dataCopy[1:])

			// Submit proposal to Raft via dedicated channel
			client.ProposeChannel <- func() {
				if err := s.node.Propose(context.TODO(), dataCopy); err != nil {
					log.Printf("Propose error: %v", err)
				}
			}
		} else {
			// Forward write operation to leader
			size += 4 // Add forwarding overhead
			buffer := s.GetBuffer(shared.ForwardSize(data) + shared.PacketHeaderSize)
			sendBuffer := shared.PackForwardPacket(messageId, ownerId, data, buffer)

			// Select connection to leader
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[s.leader-1], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[s.leader-1][connIdx]

			// Send forwarded write operation
			peer.Channel <- func() {
				if err := shared.Write(*peer.Connection, sendBuffer); err != nil {
					log.Printf("Write error to peer %d: %v", s.leader, err)
				}
				s.PutBuffer(buffer)
			}
		}
	}
}

// respondToClient sends responses back to clients after operation completion
// Handles both read responses (with data) and write acknowledgments
func (s *Server) respondToClient(op byte, id uuid.UUID, data []byte) {
	// Handle read operation responses
	if op == shared.OpRead || op == shared.OpReadMemory {
		// Retrieve and remove pending read from tracking map
		senderAny, ok := s.senders.LoadAndDelete(id)
		if !ok {
			log.Printf("No sender found for read id %v", id)
			return
		}
		request := senderAny.(shared.PendingRead)

		// Prepare read response packet
		buffer := s.GetBuffer(shared.ReadResponseSize(data) + shared.PacketHeaderSize)
		buffer = shared.PackReadResponsePacket(op, data, buffer)

		// Send response via client's channel
		request.Client.Channel <- func() {
			if err := shared.Write(request.Client.Connection, buffer); err != nil {
				log.Printf("Write error: %v", err)
			}
			s.PutBuffer(buffer)
		}
	} else if op == shared.OpWrite || op == shared.OpWriteMemory {
		// Handle write operation acknowledgments
		senderAny, ok := s.senders.LoadAndDelete(id)
		if !ok {
			log.Printf("No sender found for write id %v", id)
			return
		}
		request := senderAny.(shared.Client)

		// Send write acknowledgment
		request.Channel <- func() {
			buffer := s.GetBuffer(shared.PacketHeaderSize)
			buffer = shared.PackWriteResponsePacket(op, buffer)
			if err := shared.Write(request.Connection, buffer); err != nil {
				log.Printf("Write error: %v", err)
			}
			s.PutBuffer(buffer)
		}
	}
}

// startClientListener starts the TCP listener for client connections
// This is the main entry point for client communication with the server
func (s *Server) startClientListener() {
	// Create TCP listener on configured address
	listener, err := net.Listen("tcp", s.flags.ClientListenAddress)
	if err != nil {
		panic(err) // Critical failure - cannot accept clients
	}

	// Main acceptance loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err) // Critical failure - listener broken
		}

		// Optimize TCP for low latency
		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err) // Critical failure - cannot optimize connection
		}

		// Handle each client connection in separate goroutine
		go s.handleClientConnection(conn)
	}
}
