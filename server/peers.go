package server

import (
	"context"
	"encoding/binary"
	"etcd-light/shared"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func GroupBatches(messages []raftpb.Message) map[uint64][]*shared.MessageBatch {
	grouped := make(map[uint64][]*shared.MessageBatch)
	for i := range messages {
		msg := messages[i]
		msgSize := msg.Size() + 4
		batches := grouped[msg.To]
		var lastBatch *shared.MessageBatch
		if len(batches) > 0 {
			lastBatch = batches[len(batches)-1]
		}
		if lastBatch == nil || lastBatch.TotalSize+msgSize > shared.MaxBatchSize {
			newBatch := &shared.MessageBatch{
				TotalSize: msgSize,
				Messages:  []raftpb.Message{msg},
			}
			batches = append(batches, newBatch)
		} else {
			lastBatch.Messages = append(lastBatch.Messages, msg)
			lastBatch.TotalSize += msgSize
		}
		grouped[msg.To] = batches
	}
	return grouped
}

func (s *Server) processMessages(msgs []raftpb.Message) {
	grouped := GroupBatches(msgs)
	for recipient, batches := range grouped {
		for _, batch := range batches {
			buffer := s.GetBuffer(shared.MessageBatchSize(batch) + shared.PacketHeaderSize)
			sendBuffer := shared.PackMessageBatchPacket(batch, buffer)
			peerIdx := recipient - 1
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[peerIdx][connIdx]
			peer.Channel <- func() {
				if err := shared.Write(*peer.Connection, sendBuffer); err != nil {
					log.Printf("Write error to peer %d: %v", peerIdx+1, err)
				}
				s.PutBuffer(buffer)
			}
		}
	}
}

type ReadIndexStore struct {
	Proposer uint32
	Acks     int
}

var readIndexLock = &sync.Mutex{}
var readIndexes = make(map[uuid.UUID]*ReadIndexStore)
var pendingReadRequestLock = &sync.Mutex{}
var pendingReadRequests = make(map[int][]shared.PendingRead)

func (s *Server) Trigger(index uint64) {
	pendingReadRequestLock.Lock()
	toDelete := make([]int, 0)
	for readIndex, values := range pendingReadRequests {
		if readIndex <= int(index) {
			for _, value := range values {
				keySize := binary.LittleEndian.Uint32(value.Data[26:30])
				key := value.Data[30 : keySize+30]
				index, err := strconv.Atoi(string(key))
				if err != nil {
					panic(err)
				}
				s.applyChannels[(index*s.flags.NumDbs)/s.flags.MaxDbIndex] <- value.Data
			}
			toDelete = append(toDelete, readIndex)
		}
	}
	for _, value := range toDelete {
		delete(pendingReadRequests, value)
	}
	pendingReadRequestLock.Unlock()
}

func (s *Server) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	stepChannel := make(chan func(), 1000000)

	go func() {
		for step := range stepChannel {
			step()
		}
	}()

	peerIndexBuffer := make([]byte, 4)
	if err := shared.Read(conn, peerIndexBuffer); err != nil {
		return
	}
	peerIndex := binary.LittleEndian.Uint32(peerIndexBuffer)
	log.Printf("Got connection from peer %d", peerIndex)

	readBuffer := make([]byte, shared.MaxBatchSize+1000)
	for {
		totalSize := shared.ReadPacket(conn, readBuffer)
		op := readBuffer[0]
		if op == shared.OpForward {
			//TODO: There wasn't an easy way to free this from the pool if we use s.GetBuffer() so we just copy
			buffer := make([]byte, totalSize)
			copy(buffer, readBuffer[:totalSize])
			s.proposeChannel <- func() {
				if err := s.node.Propose(context.TODO(), buffer[:totalSize]); err != nil {
					log.Printf("Propose error: %v", err)
				}
			}
		} else if op == shared.OpMessageBatch {
			messages := shared.UnpackMessageBatchPacket(readBuffer[1:])
			for _, message := range messages {
				if message.Type == raftpb.MsgHeartbeat {
					s.leader = uint32(message.From)
				} else if message.Type == raftpb.MsgHeartbeatResp {
					s.leader = uint32(s.config.ID)
				}
				stepChannel <- func() {
					if err := s.node.Step(context.TODO(), message); err != nil {
						log.Printf("Step error: %v", err)
					}
				}
			}
		} else if op == shared.OpReadIndexForward {
			messageId := uuid.UUID(readBuffer[1:17])

			size := 21
			buffer := s.GetBuffer(size)
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(size)-4)
			copy(buffer[5:21], messageId[:16])

			if s.leader == uint32(s.config.ID) {
				buffer[4] = shared.OpReadIndexReq
				readIndexLock.Lock()
				readIndexes[messageId] = &ReadIndexStore{Acks: 1, Proposer: peerIndex + 1}
				readIndexLock.Unlock()
				for peerIdx := range s.peerAddresses {
					if peerIdx == int(s.config.ID-1) {
						continue
					}
					connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
					peer := s.peerConnections[peerIdx][connIdx]
					bufferCopy := s.GetBuffer(21)
					copy(bufferCopy, buffer[:21])
					peer.Channel <- func() {
						if err := shared.Write(*peer.Connection, bufferCopy[:21]); err != nil {
							log.Printf("Write error to peer %d: %v", s.leader, err)
						}
						s.PutBuffer(bufferCopy)
					}
				}
				s.PutBuffer(buffer)
			} else {
				fmt.Printf("Unexpected read index request when we are not the leader!\n")
			}
		} else if op == shared.OpReadIndexReq {
			messageId := uuid.UUID(readBuffer[1:17])
			size := 21
			buffer := s.GetBuffer(size)
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(size-4))
			buffer[4] = shared.OpReadIndexAck
			copy(buffer[5:21], messageId[:16])
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIndex], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[peerIndex][connIdx]
			peer.Channel <- func() {
				if err := shared.Write(*peer.Connection, buffer[:21]); err != nil {
					log.Printf("Write error to peer %d: %v", s.leader, err)
				}
				s.PutBuffer(buffer)
			}
		} else if op == shared.OpReadIndexAck {
			messageId := uuid.UUID(readBuffer[1:17])
			removed := false
			readIndexLock.Lock()
			readIndexStore, exists := readIndexes[messageId]
			if !exists {
				readIndexLock.Unlock()
				continue
			}
			readIndexStore.Acks++
			if readIndexStore.Acks >= len(s.peerAddresses)/2+1 {
				removed = true
				delete(readIndexes, messageId)
			}
			readIndexLock.Unlock()
			if removed {
				if s.leader == readIndexStore.Proposer {
					value, ok := s.senders.Load(messageId)
					if !ok {
						fmt.Printf("Unexpected read index resp from leader %d!\n", messageId)
					}
					pendingRead := value.(shared.PendingRead)
					commitIndex := atomic.LoadUint32(&s.commitIndex)
					pendingReadRequestLock.Lock()
					if pendingReadRequests[int(commitIndex)] == nil {
						pendingReadRequests[int(commitIndex)] = make([]shared.PendingRead, 0)
					}
					pendingReadRequests[int(commitIndex)] = append(pendingReadRequests[int(commitIndex)], pendingRead)
					pendingReadRequestLock.Unlock()
					s.Trigger(uint64(atomic.LoadUint32(&s.commitIndex)))
				} else {
					buffer := s.GetBuffer(25)
					connIdx := atomic.AddUint32(&s.peerConnRoundRobins[readIndexStore.Proposer-1], 1) % uint32(s.flags.NumPeerConnections)
					peer := s.peerConnections[readIndexStore.Proposer-1][connIdx]
					binary.LittleEndian.PutUint32(buffer[0:4], uint32(25)-4)
					copy(buffer[5:21], messageId[:16])
					binary.LittleEndian.PutUint32(buffer[21:25], atomic.LoadUint32(&s.commitIndex))
					buffer[4] = shared.OpReadIndexResp
					//fmt.Printf("Writing out response then!\n")

					peer.Channel <- func() {
						if err := shared.Write(*peer.Connection, buffer[:25]); err != nil {
							log.Printf("Write error to peer %d: %v", s.leader, err)
						}
						s.PutBuffer(buffer)
					}
				}
			}
		} else if op == shared.OpReadIndexResp {
			messageId := uuid.UUID(readBuffer[1:17])
			commitIndex := binary.LittleEndian.Uint32(readBuffer[17:21])
			value, ok := s.senders.Load(messageId)

			if !ok {
				fmt.Printf("Unexpected read index resp from leader %d!\n", messageId)
			}
			pendingRead := value.(shared.PendingRead)
			pendingReadRequestLock.Lock()
			if pendingReadRequests[int(commitIndex)] == nil {
				pendingReadRequests[int(commitIndex)] = make([]shared.PendingRead, 0)
			}
			pendingReadRequests[int(commitIndex)] = append(pendingReadRequests[int(commitIndex)], pendingRead)
			pendingReadRequestLock.Unlock()

			s.Trigger(uint64(atomic.LoadUint32(&s.commitIndex)))
		} else {
			panic(fmt.Sprintf("Unknown op: %v", op))
		}
	}
}

func (s *Server) connectToPeer(peerIdx, connIdx int) {
	for {
		conn, err := net.Dial("tcp", s.peerAddresses[peerIdx])
		if err != nil {
			//fmt.Printf("Error connecting to peer %d: %v\n", peerIdx, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		fmt.Printf("Connected to peer %d\n", connIdx)

		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err)
		}

		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, uint32(s.flags.NodeIndex))
		if err := shared.Write(conn, bytes); err != nil {
			panic(err)
		}

		peerConn := shared.PeerConnection{
			Connection: &conn,
			Channel:    make(chan func(), 1000000),
		}

		go func() {
			for task := range peerConn.Channel {
				task()
			}
		}()
		s.peerConnections[peerIdx][connIdx] = peerConn
		break
	}
}

func (s *Server) setupPeerConnections() {
	numPeers := len(s.peerAddresses)
	s.peerConnections = make([][]shared.PeerConnection, numPeers)
	s.peerConnRoundRobins = make([]uint32, numPeers)

	for p := range numPeers {
		if p == s.flags.NodeIndex {
			continue
		}
		s.peerConnRoundRobins[p] = 0
		s.peerConnections[p] = make([]shared.PeerConnection, s.flags.NumPeerConnections)
		for c := range s.flags.NumPeerConnections {
			fmt.Printf("Trying to connect to peer %d\n", p)
			s.connectToPeer(p, c)
			fmt.Printf("Peer %d connected\n", p)
		}
	}
}

func (s *Server) startPeerListener() {
	listener, err := net.Listen("tcp", s.flags.PeerListenAddress)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err)
		}

		go s.handlePeerConnection(conn)
	}
}
