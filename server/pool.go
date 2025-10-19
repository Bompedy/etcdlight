package server

import "etcd-light/shared"

// Pools manages multiple object pools for different buffer sizes to handle various
// message types efficiently in a high-performance networking environment.
// This system minimizes garbage collection pressure and memory allocation overhead
// by reusing buffers across different size categories.
type Pools struct {
	pool50           *shared.Pool[[]byte] // 50 bytes - Small control messages, headers (Capacity: 500,000)
	pool1500         *shared.Pool[[]byte] // 1,500 bytes - Standard MTU-sized packets (Capacity: 200,000)
	pool15000        *shared.Pool[[]byte] // 15,000 bytes - Medium-sized batches (Capacity: 5,000)
	pool50000        *shared.Pool[[]byte] // 50,000 bytes - Large messages (Capacity: 1,000)
	pool150000       *shared.Pool[[]byte] // 150,000 bytes - Very large payloads (Capacity: 1,000)
	poolMaxBatchSize *shared.Pool[[]byte] // MaxBatchSize - Maximum batch operations (Capacity: 1,000)
}

// GetBuffer retrieves an appropriately sized buffer from the pool hierarchy based on required capacity.
// Allocation Logic:
//
//	required < 50 bytes    → pool50 (50 bytes)
//	required < 1,500 bytes → pool1500 (1,500 bytes)
//	required < 15,000 bytes → pool15000 (15,000 bytes)
//	required < 50,000 bytes → pool150000 (150,000 bytes) *NOTE: Potential bug - should use pool50000
//	required < 150,000 bytes → pool150000 (150,000 bytes)
//	required ≥ 150,000 bytes → poolMaxBatchSize (MaxBatchSize)
//
// Parameters:
//
//	required int - The minimum buffer size needed
//
// Returns:
//
//	[]byte - A buffer from the appropriate pool, guaranteed to be at least the required size
//
// Usage:
//
//	buffer := s.GetBuffer(1000) // Returns 1,500 byte buffer from pool1500
func (s *Server) GetBuffer(required int) []byte {
	if required < 50 {
		return s.pools.pool50.Get()
	} else if required < 1500 {
		return s.pools.pool1500.Get()
	} else if required < 15000 {
		return s.pools.pool15000.Get()
	} else if required < 50000 {
		return s.pools.pool150000.Get() // NOTE: Potential bug - should likely be pool50000
	} else if required < 150000 {
		return s.pools.pool150000.Get()
	} else {
		return s.pools.poolMaxBatchSize.Get()
	}
}

// PutBuffer returns a buffer to the appropriate pool based on its actual capacity.
// Return Logic:
//
//	buffer length = 50 bytes      → pool50
//	buffer length = 1,500 bytes   → pool1500
//	buffer length = 15,000 bytes  → pool15000
//	buffer length = 50,000 bytes  → pool50000
//	buffer length = 150,000 bytes → pool150000
//	other sizes                   → poolMaxBatchSize
//
// Parameters:
//
//	buffer []byte - The buffer to return to the pool
//
// Important:
//   - Buffers must be returned to maintain pool efficiency
//   - Method matches based on exact buffer length, not content size
//
// Usage:
//
//	s.PutBuffer(buffer) // Returns buffer to appropriate pool
func (s *Server) PutBuffer(buffer []byte) {
	if len(buffer) == 50 {
		s.pools.pool50.Put(buffer)
	} else if len(buffer) == 1500 {
		s.pools.pool1500.Put(buffer)
	} else if len(buffer) == 15000 {
		s.pools.pool15000.Put(buffer)
	} else if len(buffer) == 50000 {
		s.pools.pool50000.Put(buffer)
	} else if len(buffer) == 150000 {
		s.pools.pool150000.Put(buffer)
	} else {
		s.pools.poolMaxBatchSize.Put(buffer)
	}
}

// initPool initializes all buffer pools with their respective sizes and capacities.
// Capacity Strategy:
//   - High-frequency pools (50b, 1500b): Large capacities (500K, 200K) for common operations
//   - Medium-frequency pools: Moderate capacities (5K) for balanced use cases
//   - Low-frequency pools: Smaller capacities (1K) for rare large operations
//
// Usage:
//
//	Called during server initialization to set up the buffer pooling system
func (s *Server) initPool() {
	s.pools = &Pools{
		pool50: shared.NewPool(500000, func() []byte {
			return make([]byte, 50)
		}),
		pool1500: shared.NewPool(200000, func() []byte {
			return make([]byte, 1500)
		}),
		pool15000: shared.NewPool(5000, func() []byte {
			return make([]byte, 15000)
		}),
		pool50000: shared.NewPool(1000, func() []byte {
			return make([]byte, 50000)
		}),
		pool150000: shared.NewPool(1000, func() []byte {
			return make([]byte, 150000)
		}),
		poolMaxBatchSize: shared.NewPool(1000, func() []byte {
			return make([]byte, shared.MaxBatchSize)
		}),
	}
}
