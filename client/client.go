package client

import (
	"bytes"
	"encoding/binary"
	"etcd-light/shared" // Custom networking package
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Global counter for tracking completed operations across all goroutines
var completedOps uint32

// Client represents a benchmarking client that performs read/write operations
// against multiple server nodes and measures performance metrics.
type Client struct {
	Addresses      []string // List of server TCP addresses (e.g., ["localhost:8080", "localhost:8081"])
	TotalAddresses int      // Number of server addresses
	DataSize       int      // Size of data payload in bytes for each operation
	NumOps         int      // Total number of operations to perform across all clients
	ReadRatio      float64  // Fraction of read operations (0.0 = all writes, 1.0 = all reads)
	NumClients     int      // Number of concurrent client goroutines
	NumClientOps   int      // Number of operations per client goroutine
	IsReadMemory   bool     // If true, read from memory; if false, read from disk
	IsWriteMemory  bool     // If true, write to memory; if false, write to disk

	// Network connections and data
	Connections  []net.Conn // TCP connections to servers (NumClients * TotalAddresses)
	Keys         [][]byte   // Keys for operations (e.g., ["0", "1", "2", ...])
	WarmupValues [][]byte   // Initial values used during warmup phase
	UpdateValues [][]byte   // Updated values used during benchmark phase
}

// StartClient is the main entry point for the benchmarking client.
// It parses command line arguments, initializes the client, and runs the benchmark.
//
// Usage:
//
//	-addresses string    comma-separated TCP addresses (required)
//	-data-size int      payload size in bytes (required)
//	-ops int            total operations per client (required)
//	-read-ratio float   fraction of reads 0-1 (default 0.5)
//	-clients int        number of concurrent clients (default 1)
//	-read-mem bool      read from memory (default false)
//	-write-mem bool     write to memory (default false)
//	-find-leader bool   auto-detect leader node (default false)
//
// Example:
//
//	go run client.go -addresses "localhost:8080,localhost:8081" -data-size 1024 -ops 10000 -read-ratio 0.8 -clients 4
func StartClient(args []string) {
	// Define command line flags
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	var (
		addressesCSV  string
		dataSize      int
		numOps        int
		readRatio     float64
		numClients    int
		isReadMemory  bool
		isWriteMemory bool
		isFindLeader  bool
	)

	fs.StringVar(&addressesCSV, "addresses", "", "comma‑separated TCP addresses")
	fs.IntVar(&dataSize, "data-size", 0, "payload size in bytes")
	fs.IntVar(&numOps, "ops", 0, "total operations per client")
	fs.Float64Var(&readRatio, "read-ratio", 0.5, "fraction of reads 0–1")
	fs.IntVar(&numClients, "clients", 1, "number of concurrent clients")
	fs.BoolVar(&isReadMemory, "read-mem", false, "read from memory")
	fs.BoolVar(&isWriteMemory, "write-mem", false, "write to memory")
	fs.BoolVar(&isFindLeader, "find-leader", false, "check the provided addresses and find the leader node")

	// Parse command line arguments
	err := fs.Parse(args)
	if err != nil {
		panic(err)
	}

	// Validate required arguments
	if addressesCSV == "" {
		log.Fatalf("-addresses is required")
	}
	if dataSize <= 0 {
		log.Fatalf("-data-size must be > 0")
	}
	if numOps <= 0 {
		log.Fatalf("-ops must be > 0")
	}
	if readRatio < 0 || readRatio > 1 {
		log.Fatalf("-read-ratio must be between 0 and 1")
	}
	if numClients <= 0 {
		log.Fatalf("-clients must be > 0")
	}

	// Parse comma-separated addresses
	addresses := strings.Split(addressesCSV, ",")
	totalAddresses := len(addresses)

	// Leader discovery mode: find which node is the leader in the cluster
	if isFindLeader {
		connections := make([]net.Conn, totalAddresses)
		leaderGroup := sync.WaitGroup{}
		leaderIndex := -1
		leaderGroup.Add(1)

		// Try all addresses to find the leader
		for i := range totalAddresses {
			var connection net.Conn
			// Retry connection until successful
			for {
				connection, err = net.Dial("tcp", addresses[i])
				if err != nil {
					time.Sleep(250 * time.Millisecond)
					continue
				}
				break
			}
			connections[i] = connection

			// Goroutine to check if this node is the leader
			go func() {
				buffer := make([]byte, 5)

				for {
					// Send leader detection packet
					err = shared.Write(connection, shared.PackLeaderPacket(buffer))
					if err != nil {
						break
					}
					// Read response (1 = leader, 0 = not leader)
					err = shared.Read(connection, buffer[:1])
					if err != nil {
						break
					}
					if buffer[0] == byte(1) {
						leaderGroup.Done()
						leaderIndex = i
						break
					}
					time.Sleep(250 * time.Millisecond)
				}
			}()
		}
		leaderGroup.Wait()

		// Clean up connections and use only the leader address
		for i := range connections {
			connections[i].Close()
		}

		leaderAddress := addresses[leaderIndex]
		addresses = make([]string, 1)
		addresses[0] = leaderAddress
		totalAddresses = 1
		fmt.Printf("%d\n", leaderIndex)
	}

	// Calculate operations per client
	numClientOps := numOps / (numClients * totalAddresses)

	// Initialize client structure
	client := &Client{
		Addresses:      addresses,
		TotalAddresses: totalAddresses,
		DataSize:       dataSize,
		NumOps:         numClientOps * numClients * totalAddresses,
		ReadRatio:      readRatio,
		NumClients:     numClients,
		NumClientOps:   numClientOps,
		Connections:    make([]net.Conn, numClients*totalAddresses),
		Keys:           make([][]byte, numOps),
		WarmupValues:   make([][]byte, numOps),
		UpdateValues:   make([][]byte, numOps),
		IsReadMemory:   isReadMemory,
		IsWriteMemory:  isWriteMemory,
	}

	// Initialize test data
	for i := 0; i < numOps; i++ {
		client.Keys[i] = []byte(fmt.Sprintf("%d", i))                  // Key: "0", "1", "2", ...
		client.WarmupValues[i] = []byte(strings.Repeat("x", dataSize)) // Warmup value: "xxxx..."
		client.UpdateValues[i] = []byte(strings.Repeat("z", dataSize)) // Update value: "zzzz..."
	}

	// Execute benchmark workflow
	client.Connect()   // Establish TCP connections
	client.Warmup()    // Pre-populate data
	client.Benchmark() // Run performance benchmark
}

// Connect establishes TCP connections to all server addresses.
// It creates NumClients connections per server address and enables TCP NoDelay for low latency.
// Panics if any connection fails.
func (client *Client) Connect() {
	connected := 0
	// Create connections for each address and each client
	for i := range client.TotalAddresses {
		for range client.NumClients {
			connection, err := net.Dial("tcp", client.Addresses[i])
			if err != nil {
				panic(err)
			}
			// Disable Nagle's algorithm for lower latency
			err = connection.(*net.TCPConn).SetNoDelay(true)
			if err != nil {
				panic(err)
			}
			client.Connections[connected] = connection
			connected++
		}
	}

	// Verify all connections were established
	if connected != client.NumClients*client.TotalAddresses {
		panic(fmt.Sprintf("%d clients connected", connected))
	}
}

// Benchmark executes the main performance measurement phase.
// It spawns multiple client goroutines that perform read/write operations,
// measures latency and throughput, and outputs performance statistics.
func (client *Client) Benchmark() {
	var benchmarkBar sync.WaitGroup
	benchmarkBar.Add(1)
	completedOps = 0

	// Progress bar goroutine
	go func() {
		defer benchmarkBar.Done()
		ProgressBar("Benchmark", client.NumOps)
	}()

	// Latency tracking arrays
	clientReadTimes := make([]int, client.NumOps)
	clientWriteTimes := make([]int, client.NumOps)
	clientTimes := make([]int, client.NumOps)

	// Synchronization for client goroutines
	group := sync.WaitGroup{}
	group.Add(client.NumClients * client.TotalAddresses)

	// Operation counters
	var count uint32
	var readCount uint32
	var writeCount uint32

	start := time.Now().UnixMilli()

	// Launch client goroutines
	for i := range client.Connections {
		go func(i int, connection net.Conn) {
			buffer := make([]byte, client.DataSize+128) // Reusable buffer for network I/O

			for c := range client.NumClientOps {
				// Determine if this operation is a read or write
				isRead := rand.Float64() < client.ReadRatio
				key := client.Keys[i*client.NumClientOps+c]
				warmupValue := client.WarmupValues[i*client.NumClientOps+c]
				updateValue := client.UpdateValues[i*client.NumClientOps+c]
				value := warmupValue

				var sendBuffer []byte

				// Prepare the appropriate network packet
				if !isRead {
					// Write operation
					value = updateValue
					if client.IsWriteMemory {
						sendBuffer = shared.PackWriteMemoryPacket(key, value, buffer)
					} else {
						sendBuffer = shared.PackWritePacket(key, value, buffer)
					}
				} else {
					// Read operation
					if client.IsReadMemory {
						sendBuffer = shared.PackReadMemoryPacket(key, buffer)
					} else {
						sendBuffer = shared.PackReadPacket(key, buffer)
					}
				}

				// Measure operation latency
				begin := time.Now().UnixMicro()

				// Send request
				err := shared.Write(connection, sendBuffer)
				if err != nil {
					panic(err)
				}

				// Receive response
				shared.ReadPacket(connection, buffer)

				// Validate response type
				if !isRead {
					if client.IsWriteMemory && buffer[0] != shared.OpWriteMemory {
						panic(fmt.Sprintf("Memory Write operation failed, expected %v, got %v", shared.OpWriteMemory, buffer[0]))
					}
					if !client.IsWriteMemory && buffer[0] != shared.OpWrite {
						panic(fmt.Sprintf("Disk Write operation failed, expected %v, got %v", shared.OpWrite, buffer[0]))
					}
				} else {
					if client.IsReadMemory && buffer[0] != shared.OpReadMemory {
						panic(fmt.Sprintf("Memory Read operation failed, expected %v, got %v", shared.OpReadMemory, buffer[0]))
					}

					if !client.IsReadMemory && buffer[0] != shared.OpRead {
						panic(fmt.Sprintf("Disk Read operation failed, expected %v, got %v", shared.OpRead, buffer[0]))
					}

					// Validate read response
					valueSize := binary.LittleEndian.Uint32(buffer[1:5])
					if valueSize == 0 {
						panic(fmt.Sprintf("GOT A NULL READ FOR KEY: %s", string(key)))
					} else {
						// Note: Value validation is commented out for performance
						// if !bytes.Equal(buffer[5:5+valueSize], value) {
						// 	panic(fmt.Sprintf("GOT A WRONG VALUE FOR KEY: %s, warmup-%s, returned-%s",
						// 		string(key), string(value), string(buffer[4:4+valueSize])))
						// }
					}
				}

				end := time.Now().UnixMicro()

				// Record latency metrics
				nextCount := atomic.AddUint32(&count, 1)
				clientTimes[nextCount-1] = int(end - begin)

				if isRead {
					nextReadCount := atomic.AddUint32(&readCount, 1)
					clientReadTimes[nextReadCount-1] = int(end - begin)
				} else if !isRead {
					nextWriteCount := atomic.AddUint32(&writeCount, 1)
					clientWriteTimes[nextWriteCount-1] = int(end - begin)
				}
				atomic.AddUint32(&completedOps, 1)
			}
			group.Done()
		}(i, client.Connections[i])
	}

	// Wait for all operations to complete
	group.Wait()
	end := time.Now().UnixMilli()
	benchmarkBar.Wait()

	// Calculate throughput in Mbps
	bitsPerOp := client.DataSize * 8
	mbps := ((float32(client.NumOps) * float32(bitsPerOp)) / 1.0e6) / (float32(end-start) / 1.0e3)

	// Write results to file
	file, err := os.Create("output")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if err = binary.Write(file, binary.LittleEndian, mbps); err != nil {
		panic(err)
	}
	for _, f := range clientTimes {
		if err = binary.Write(file, binary.LittleEndian, int32(f)); err != nil {
			panic(err)
		}
	}

	// Display final results
	client.displayResults(start, end, clientTimes, clientWriteTimes, clientReadTimes,
		int(count), int(writeCount), int(readCount))
}

// Warmup pre-populates the storage system with initial data before benchmarking.
// This ensures that read operations during the benchmark have data to read.
func (client *Client) Warmup() {
	warmup := sync.WaitGroup{}
	warmup.Add(client.NumClients * client.TotalAddresses)

	var warmupBar sync.WaitGroup
	warmupBar.Add(1)

	// Progress bar for warmup phase
	go func() {
		defer warmupBar.Done()
		ProgressBar("Warmup", client.NumOps+(client.NumClients*client.TotalAddresses))
	}()

	// Perform warmup operations
	for i := range client.Connections {
		go func(i int, connection net.Conn) {
			buffer := make([]byte, client.DataSize+128)

			// Write all initial values
			for c := range client.NumClientOps {
				key := client.Keys[i*client.NumClientOps+c]
				warmupValue := client.WarmupValues[i*client.NumClientOps+c]
				value := warmupValue
				sendBuffer := shared.PackWriteMemoryPacket(key, value, buffer)

				err := shared.Write(connection, sendBuffer)
				if err != nil {
					panic(err)
				}

				shared.ReadPacket(connection, buffer)
				if client.IsWriteMemory && buffer[0] != shared.OpWriteMemory {
					panic(fmt.Sprintf("Memory Write operation failed, expected %v, got %v", shared.OpWriteMemory, buffer[0]))
				}
				if !client.IsWriteMemory && buffer[0] != shared.OpWrite {
					panic(fmt.Sprintf("Disk Write operation failed, expected %v, got %v", shared.OpWrite, buffer[0]))
				}
				atomic.AddUint32(&completedOps, 1)
			}

			// Verify one read operation to ensure data is accessible
			key := client.Keys[0]
			warmupValue := client.WarmupValues[0]

			var sendBuffer []byte
			if client.IsReadMemory {
				sendBuffer = shared.PackReadMemoryPacket(key, buffer)
			} else {
				sendBuffer = shared.PackReadPacket(key, buffer)
			}
			err := shared.Write(connection, sendBuffer)
			if err != nil {
				panic(err)
			}

			shared.ReadPacket(connection, buffer)

			if client.IsReadMemory && buffer[0] != shared.OpReadMemory {
				panic(fmt.Sprintf("Memory Read operation failed, expected %v, got %v", shared.OpReadMemory, buffer[0]))
			}

			if !client.IsReadMemory && buffer[0] != shared.OpRead {
				panic(fmt.Sprintf("Memory Read operation failed, expected %v, got %v", shared.OpRead, buffer[0]))
			}

			// Validate the read response
			valueSize := binary.LittleEndian.Uint32(buffer[1:5])
			if valueSize == 0 {
				panic(fmt.Sprintf("GOT A NULL READ FOR KEY: %s", string(key)))
			} else {
				if !bytes.Equal(buffer[5:5+valueSize], warmupValue) {
					panic(fmt.Sprintf("GOT A WRONG VALUE FOR KEY: %s, warmup-%s, returned-%s",
						string(key), string(warmupValue), string(buffer[4:4+valueSize])))
				}
			}

			atomic.AddUint32(&completedOps, 1)
			warmup.Done()
		}(i, client.Connections[i])
	}

	warmup.Wait()
	warmupBar.Wait()
}

// displayResults calculates and prints comprehensive performance statistics
// including throughput, average latency, and percentile latencies.
func (client *Client) displayResults(
	start int64,
	end int64,
	clientTimes []int,
	clientWriteTimes []int,
	clientReadTimes []int,
	count int,
	writeCount int,
	readCount int,
) {
	// Sort latency arrays for percentile calculations
	sort.Ints(clientTimes[:count])
	sort.Ints(clientWriteTimes[:writeCount])
	sort.Ints(clientReadTimes[:readCount])

	// Calculate statistics for all operations
	avgAll := 0
	maxAll := math.MinInt32
	minAll := math.MaxInt32
	for i := range count {
		timeAll := clientTimes[i]
		avgAll += timeAll
		if timeAll > maxAll {
			maxAll = timeAll
		}
		if timeAll < minAll {
			minAll = timeAll
		}
	}
	avgAll /= count

	// Calculate statistics for write operations
	avgWrite := 0
	maxWrite := math.MinInt32
	minWrite := math.MaxInt32
	for i := range writeCount {
		timeWrite := clientWriteTimes[i]
		avgWrite += timeWrite
		if timeWrite > maxWrite {
			maxWrite = timeWrite
		}
		if timeWrite < minWrite {
			minWrite = timeWrite
		}
	}
	if writeCount > 0 {
		avgWrite /= writeCount
	}

	// Calculate statistics for read operations
	avgRead := 0
	maxRead := math.MinInt32
	minRead := math.MaxInt32
	for i := range readCount {
		timeRead := clientReadTimes[i]
		avgRead += timeRead
		if timeRead > maxRead {
			maxRead = timeRead
		}
		if timeRead < minRead {
			minRead = timeRead
		}
	}
	if readCount > 0 {
		avgRead /= readCount
	}

	// Display results
	fmt.Printf("\nBenchmark complete!\n")
	fmt.Printf("Connections: %d\n", client.NumClients*client.TotalAddresses)
	fmt.Printf("Data Size: %d\n", client.DataSize)

	if writeCount > 0 && readCount > 0 {
		fmt.Printf("All - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			count, int(float32(count)/(float32(end-start)/1000.0)), avgAll, minAll, maxAll,
			clientTimes[int(float32(count)*0.5)],
			clientTimes[int(float32(count)*0.9)],
			clientTimes[int(float32(count)*0.95)],
			clientTimes[int(float32(count)*0.99)],
			clientTimes[int(float32(count)*0.999)],
			clientTimes[int(float32(count)*0.9999)],
		)
	}
	if writeCount > 0 {
		fmt.Printf("Update - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			writeCount, int(float32(writeCount)/(float32(end-start)/1000.0)), avgWrite, minWrite, maxWrite,
			clientWriteTimes[int(float32(writeCount)*0.5)],
			clientWriteTimes[int(float32(writeCount)*0.9)],
			clientWriteTimes[int(float32(writeCount)*0.95)],
			clientWriteTimes[int(float32(writeCount)*0.99)],
			clientWriteTimes[int(float32(writeCount)*0.999)],
			clientWriteTimes[int(float32(writeCount)*0.9999)],
		)
	}
	if readCount > 0 {
		fmt.Printf("Read - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			readCount, int(float32(readCount)/(float32(end-start)/1000.0)), avgRead, minRead, maxRead,
			clientReadTimes[int(float32(readCount)*0.5)],
			clientReadTimes[int(float32(readCount)*0.9)],
			clientReadTimes[int(float32(readCount)*0.95)],
			clientReadTimes[int(float32(readCount)*0.99)],
			clientReadTimes[int(float32(readCount)*0.999)],
			clientReadTimes[int(float32(readCount)*0.9999)],
		)
	}
}

// ProgressBar displays a real-time progress bar in the console.
// It shows the percentage complete and the number of operations finished.
//
// Parameters:
//   - title: The title to display before the progress bar
//   - numOps: Total number of operations to complete
func ProgressBar(title string, numOps int) {
	barWidth := 50
	for {
		completed := atomic.LoadUint32(&completedOps)

		// Ensure we don't exceed 100%
		if completed > uint32(numOps) {
			completed = uint32(numOps)
		}

		// Calculate progress percentage
		percent := float64(completed) / float64(numOps) * 100
		filled := int(float64(barWidth) * float64(completed) / float64(numOps))
		if filled > barWidth {
			filled = barWidth
		}
		if filled < 0 {
			filled = 0
		}

		// Create visual bar
		bar := "[" + strings.Repeat("=", filled) + strings.Repeat(" ", barWidth-filled) + "]"
		fmt.Printf("\r%s Progress: %s %.2f%% (%d/%d)", title, bar, percent, completed, numOps)

		// Exit when complete
		if completed >= uint32(numOps) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}
