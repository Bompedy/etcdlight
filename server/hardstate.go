package server

import (
	"fmt"
	"syscall"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (s *Server) processHardState(hs raftpb.HardState) {
	// Only process non-empty Hard State
	if !raft.IsEmptyHardState(hs) {

		// Persist to disk if not running in memory-only mode
		if !(s.flags.Memory) {
			// Get buffer for serialization
			buffer := s.GetBuffer(hs.Size())

			// Marshal Hard State to buffer
			size, err := hs.MarshalTo(buffer)
			if err != nil {
				panic(err)
			}

			// Write to hard state file with retry logic
			count := int64(0)
			for {
				wrote, err := s.hardstateFile.WriteAt(buffer[count:size], count)
				if err != nil {
					panic(err)
				}
				count += int64(wrote)
				if count == int64(size) {
					break
				}
			}

			if s.flags.Fsync {
				fd := int(s.hardstateFile.Fd())
				err = syscall.Fsync(fd)
				if err != nil {
					_ = fmt.Errorf("error syncing hard state file: %v", err)
				}
			}

			// Return buffer to pool
			s.PutBuffer(buffer)
		}

		// Store in memory storage (always, regardless of mode)
		err := s.storage.SetHardState(hs)
		if err != nil {
			panic(err)
		}
	}
}
