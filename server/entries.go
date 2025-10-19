package server

import (
	"etcd-light/shared"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// processNormalCommitEntry processes a normal (non-configuration change) committed Raft entry.
// It handles write operations by routing them to the appropriate apply channels and optionally
// responding to clients for fast-path writes.
//
// Parameters:
//   - entry: The Raft log entry to process, expected to contain at least 8 bytes of data
//
// Behavior:
//   - Extracts message ID, owner index, and operation type from entry data
//   - Atomically updates the server's commit index
//   - For write operations, routes the entry to the appropriate database channel
//   - If fast-path writes are enabled and this node owns the entry, responds immediately to client
//   - Triggers any pending operations waiting for this commit index
func (s *Server) processNormalCommitEntry(entry raftpb.Entry) {
	// Ensure entry has sufficient data (minimum 8 bytes for message unpacking)
	if len(entry.Data) >= 8 {
		// Unpack message into component parts
		messageId, ownerIndex, data := shared.UnpackMessage(entry.Data[1:])
		op := data[0] // First byte indicates operation type

		// Atomically update the server's commit index to reflect this committed entry
		atomic.SwapUint32(&s.commitIndex, uint32(entry.Index))

		// Handle write operations (both regular writes and memory writes)
		if op == shared.OpWrite || op == shared.OpWriteMemory {
			var key []byte
			// Extract key based on operation type
			if op == shared.OpWriteMemory {
				key, _ = shared.UnpackWriteMemoryPacket(data[1:])
			} else {
				key, _ = shared.UnpackWritePacket(data[1:])
			}

			// Convert key to integer index for channel routing
			index, err := strconv.Atoi(string(key))
			if err != nil {
				panic(err) // Key format error is considered fatal
			}

			// Route entry to appropriate apply channel using hash-based distribution
			channelIndex := (index * s.flags.NumDbs) / s.flags.MaxDbIndex
			s.applyChannels[channelIndex] <- entry.Data

			// Fast-path optimization: respond immediately if this node owns the entry
			if s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
				s.respondToClient(op, messageId, nil)
			}
		}

		// Notify any waiting operations that this index has been committed
		s.Trigger(entry.Index)
	}
}

// processEntries processes and persists a batch of Raft log entries to storage.
// It handles both main storage appends and optional Write-Ahead Log (WAL) persistence.
//
// Parameters:
//   - entries: Slice of Raft log entries to process and persist
//
// Behavior:
//   - Always appends entries to main storage
//   - If not in memory-only mode, additionally writes to WAL files:
//   - Groups entries by WAL file index for parallel writing
//   - Uses buffer pooling for efficient memory usage
//   - Writes entries concurrently to multiple WAL files
//   - Ensures durability with fsync operations
//   - Waits for all WAL writes to complete before returning
func (s *Server) processEntries(entries []raftpb.Entry) {
	// Only process if there are actual entries to handle
	if len(entries) > 0 {
		// Step 1: Append all entries to main storage
		if err := s.storage.Append(entries); err != nil {
			log.Printf("Append entries error: %v", err)
		}

		// Step 2: Handle Write-Ahead Log persistence if not in memory-only mode
		if !(s.flags.Memory) {
			// Group entries by WAL file index for parallel processing
			var grouped = make(map[uint64][]raftpb.Entry)
			group := sync.WaitGroup{}
			entryCount := 0

			// Organize normal entries into WAL file groups
			for _, e := range entries {
				if e.Type == raftpb.EntryNormal {
					// Distribute entries across WAL files using modulo hashing
					walIndex := e.Index % uint64(s.flags.WalFileCount)
					grouped[walIndex] = append(grouped[walIndex], e)
					entryCount++
				}
			}

			// Early return if no normal entries to process
			if entryCount == 0 {
				return
			}

			// Set up wait group for concurrent WAL writes
			group.Add(len(grouped))

			// Process each WAL file group concurrently
			for walIndex := range grouped {
				walEntries := grouped[walIndex]
				slot := s.walSlots[walIndex] // Get WAL file handle for this index

				// Calculate total buffer size needed for all entries in this group
				size := 0
				for entryIndex := range walEntries {
					size += walEntries[entryIndex].Size()
				}

				// Acquire buffer from pool for efficient memory usage
				buffer := s.GetBuffer(size)

				// Marshal all entries into the buffer
				size = 0
				for entryIndex := range walEntries {
					entry := walEntries[entryIndex]
					entrySize, err := entry.MarshalTo(buffer[size:])
					if err != nil {
						panic(err) // Marshaling errors are fatal
					}
					size += entrySize
				}

				// Launch concurrent goroutine for WAL file writing
				go func() {
					defer group.Done() // Signal completion when done

					// Write buffer to WAL file with retry logic for partial writes
					count := 0
					for {
						wrote, err := slot.File.Write(buffer[count:size])
						if err != nil {
							panic(err) // I/O errors during write are fatal
						}
						count += wrote
						if count == size {
							break // Complete write achieved
						}
					}

					// Return buffer to pool for reuse
					s.PutBuffer(buffer)

					// Ensure data is flushed to disk for durability
					if s.flags.Fsync {
						err := syscall.Fsync(int(slot.File.Fd()))
						if err != nil {
							_ = fmt.Errorf("error syncing wal file: %v", err)
							// Note: fsync errors are logged but not considered fatal
						}
					}
				}()
			}

			// Wait for all concurrent WAL writes to complete
			group.Wait()

			// Clean up grouped map to free memory
			for k := range grouped {
				delete(grouped, k)
			}
		}
	}
}

// processCommittedEntries processes a batch of committed Raft entries by routing them
// to the appropriate handler based on entry type.
//
// Parameters:
//   - entries: Slice of committed Raft entries to process
//
// Entry Type Handling:
//   - EntryConfChange: Configuration changes are routed to processConfChange
//   - EntryNormal: Normal operations are routed to processNormalCommitEntry
//
// This function serves as the main dispatcher for committed log entries.
func (s *Server) processCommittedEntries(entries []raftpb.Entry) {
	// Process each committed entry based on its type
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryConfChange:
			// Handle cluster configuration changes (membership changes)
			s.processConfChange(entry)
		case raftpb.EntryNormal:
			// Handle normal application operations (writes, reads, etc.)
			s.processNormalCommitEntry(entry)
		}
	}
}
