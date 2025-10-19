package server

import (
	"etcd-light/shared"
	"fmt"
	"strconv"
	"syscall"

	"go.etcd.io/bbolt"
)

// Package-level constants
var (
	keyBucketName = []byte("key") // Default bucket name for BoltDB storage
)

// Get retrieves a value from BoltDB by key using a read-only transaction
// Parameters:
//   - db: BoltDB database instance
//   - key: Key to look up
//
// Returns:
//   - []byte: Retrieved value or nil if not found
//   - error: Any error that occurred during the operation
func Get(db *bbolt.DB, key []byte) ([]byte, error) {
	// Begin read-only transaction
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}

	// Retrieve value from key bucket
	value := tx.Bucket(keyBucketName).Get(key)

	// Rollback read-only transaction (no changes to commit)
	if err := tx.Rollback(); err != nil {
		return value, err // Return value even if rollback fails
	}

	return value, nil
}

// Put stores a key-value pair in BoltDB using a writable transaction
// Parameters:
//   - db: BoltDB database instance
//   - key: Key to store
//   - value: Value to store
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Note: Panics on transaction errors for critical failures
func Put(db *bbolt.DB, key []byte, value []byte) error {
	// Begin writable transaction
	tx, err := db.Begin(true)
	if err != nil {
		panic(err) // Critical failure - cannot proceed
	}

	// Store key-value pair in bucket
	writeError := tx.Bucket(keyBucketName).Put(key, value)

	// Commit transaction to persist changes
	if err := tx.Commit(); err != nil {
		panic(err) // Critical failure - data not persisted
	}

	return writeError
}

// DbHandler handles database operations for a specific database instance
// This function runs in a dedicated goroutine and processes operations from a channel
// Parameters:
//   - channel: Communication channel for receiving database operations
//   - dbIndex: Unique identifier for this database instance
func (s *Server) DbHandler(channel chan []byte, dbIndex int) {
	// In-memory cache for fast access
	memoryDb := make(map[int][]byte)

	// BoltDB configuration for optimized performance
	bopts := &bbolt.Options{}
	bopts.NoSync = false                       // Ensure data durability
	bopts.NoGrowSync = false                   // Sync on growth
	bopts.NoFreelistSync = true                // Faster writes, potential risk on crash
	bopts.FreelistType = bbolt.FreelistMapType // Map-based freelist for performance
	bopts.MmapFlags = syscall.MAP_POPULATE     // Pre-populate page cache
	bopts.Mlock = false                        // Don't lock memory
	bopts.InitialMmapSize = 10737418240        // 10GB initial mmap size
	bopts.PageSize = 0                         // Use default page size

	// Open or create BoltDB database file
	boltDb, err := bbolt.Open(fmt.Sprintf("db.%d", dbIndex), 0600, bopts)
	if err != nil {
		panic(err)
	}

	// Initialize database bucket
	tx, err := boltDb.Begin(true)
	if err != nil {
		panic(err)
	}

	_, err = tx.CreateBucketIfNotExists(keyBucketName)
	if err != nil {
		panic(err)
	}

	if err := tx.Commit(); err != nil {
		panic(err)
	}

	// Main event loop - processes operations from channel
	for {
		select {
		case data := <-channel:
			// Parse incoming message
			messageId, ownerIndex, messageData := shared.UnpackMessage(data[1:])
			op := messageData[0] // Operation type

			// Handle different operation types
			switch op {
			case shared.OpWriteMemory:
				// Write to memory cache only
				key, value := shared.UnpackWriteMemoryPacket(messageData[1:])
				index, err := strconv.Atoi(string(key))
				if err != nil {
					panic(err)
				}
				memoryDb[index] = value

				// Acknowledge write if needed
				if !s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
					s.respondToClient(shared.OpWriteMemory, messageId, nil)
				}

			case shared.OpWrite:
				// Write to both memory cache and persistent storage
				key, value := shared.UnpackWritePacket(messageData[1:])
				index, err := strconv.Atoi(string(key))
				if err != nil {
					panic(err)
				}
				memoryDb[index] = value

				// Persist to BoltDB
				err = Put(boltDb, key, value)
				if err != nil {
					panic(err)
				}

				// Acknowledge write if needed
				if !s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
					s.respondToClient(shared.OpWrite, messageId, nil)
				}

			default:
				// Only process read operations if we own the data
				if ownerIndex == uint32(s.config.ID) {
					switch op {
					case shared.OpReadMemory:
						// Read from memory cache
						key := shared.UnpackReadMemoryPacket(messageData[1:])
						index, err := strconv.Atoi(string(key))
						if err != nil {
							panic(err)
						}
						value := memoryDb[index]
						if value == nil {
							fmt.Println("No key found")
						}
						s.respondToClient(shared.OpReadMemory, messageId, value)

					case shared.OpRead:
						// Read from persistent storage
						key := shared.UnpackReadPacket(messageData[1:])
						value, err := Get(boltDb, key)
						if err != nil {
							panic(err)
						}
						s.respondToClient(shared.OpRead, messageId, value)

					default:
						fmt.Printf("1 Unknown op %v\n", op)
					}
				}
			}
		}
	}
}
