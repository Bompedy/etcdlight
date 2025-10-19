package server

import (
	"etcd-light/shared"
	"flag"
	"fmt"
	"log"
	"math"
	_ "net/http/pprof" // Import for side effects (pprof endpoints)
	"os"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"time"
)

// Server represents the main Raft server instance managing consensus, storage, and networking
type Server struct {
	senders             sync.Map                  // Thread-safe map for tracking pending client requests by UUID
	peerAddresses       []string                  // List of all peer addresses in the cluster
	node                raft.Node                 // The core Raft consensus node
	storage             *raft.MemoryStorage       // In-memory Raft log storage
	config              *raft.Config              // Raft configuration parameters
	peerConnections     [][]shared.PeerConnection // 2D array: [peer_index][connection_index]
	peerConnRoundRobins []uint32                  // Round-robin counters for peer connection selection
	walSlots            []shared.WalSlot          // Write-ahead log file slots for durability
	walBulkFile         *os.File                  // Bulk WAL operations file
	hardstateFile       *os.File                  // Persistent Raft hard state file
	applyChannels       []chan []byte             // Channels for applying committed entries to databases
	flags               *ServerFlags              // Server configuration flags
	pools               *Pools                    // Buffer pools for memory management
	applyIndex          uint64                    // Last applied log index
	leader              uint32                    // Current leader ID
	waiters             map[uint64][][]byte       // Map of log index to waiting client data
	stepChannel         chan func()               // Channel for Raft step operations
	proposeChannel      chan func()               // Channel for Raft proposal operations
	readIndexChannel    chan func()               // Channel for read index operations
	commitIndex         uint32                    // Current commit index
}

// ServerFlags contains all configuration parameters for the server
type ServerFlags struct {
	Profile             bool   // Enable performance profiling
	NodeIndex           int    // This node's index in the peer list
	NumPeerConnections  int    // Number of connections to maintain to each peer
	PeerListenAddress   string // Address to listen for peer connections
	ClientListenAddress string // Address to listen for client connections
	PeerAddressesString string // Comma-separated list of all peer addresses
	WalFileCount        int    // Number of WAL files to use for log segmentation
	Fsync               bool   // File sync flags: "none", "fsync"
	Memory              bool   // Use memory-only mode (no disk persistence)
	FastPathWrites      bool   // Skip waiting for log application before responding
	NumDbs              int    // Number of database instances for partitioning
	MaxDbIndex          int    // Maximum index for database key partitioning
}

// setupRaft initializes the Raft consensus module with configuration and peers
func (s *Server) setupRaft() {
	s.storage = raft.NewMemoryStorage()
	s.config = &raft.Config{
		ID:              uint64(s.flags.NodeIndex + 1), // Raft IDs are 1-based
		ElectionTick:    10,                            // Number of ticks before election timeout
		HeartbeatTick:   1,                             // Number of ticks between heartbeats
		Storage:         s.storage,
		MaxSizePerMsg:   math.MaxUint32, // Maximum message size (unlimited)
		MaxInflightMsgs: 1000000,        // Maximum inflight append messages
	}

	fmt.Printf("Peer addresses: %v\n", s.peerAddresses)

	// Initialize peer configuration for Raft cluster
	peers := make([]raft.Peer, len(s.peerAddresses))
	for i := range peers {
		fmt.Printf("Peer index: %d\n", i)
		peers[i].ID = uint64(i + 1) // Assign 1-based IDs to all peers
	}

	// Start the Raft node with initial peer configuration
	s.node = raft.StartNode(s.config, peers)
}

// processConfChange applies configuration changes to the Raft cluster
func (s *Server) processConfChange(entry raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Printf("Unmarshal conf change error: %v", err)
		return
	}
	s.node.ApplyConfChange(cc)
}

// processReady processes all components of a Raft Ready structure
func (s *Server) processReady(rd raft.Ready) {
	s.processHardState(rd.HardState)               // Persist hard state
	s.processEntries(rd.Entries)                   // Append entries to log
	s.processMessages(rd.Messages)                 // Send messages to peers
	s.processCommittedEntries(rd.CommittedEntries) // Apply committed entries
	s.node.Advance()                               // Signal Raft that we've processed the Ready
}

// run is the main event loop for the Raft server
func (s *Server) run() {
	ticker := time.NewTicker(150 * time.Millisecond) // Raft tick interval
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.node.Tick() // Advance Raft logical clock
		case rd := <-s.node.Ready():
			s.processReady(rd) // Process Raft state changes
		}
	}
}

// NewServer creates and initializes a new Server instance with the given configuration
func NewServer(serverFlags *ServerFlags) *Server {
	s := &Server{
		peerAddresses:    strings.Split(serverFlags.PeerAddressesString, ","),
		walSlots:         make([]shared.WalSlot, serverFlags.WalFileCount),
		applyChannels:    make([]chan []byte, serverFlags.NumDbs),
		flags:            serverFlags,
		waiters:          make(map[uint64][][]byte),
		proposeChannel:   make(chan func(), 1000000), // High-capacity channels
		stepChannel:      make(chan func(), 1000000),
		readIndexChannel: make(chan func(), 1000000),
		leader:           uint32(serverFlags.NodeIndex + 1),
	}

	// Start worker goroutines for processing different operation types
	go func() {
		for task := range s.proposeChannel {
			task() // Execute Raft proposal tasks
		}
	}()

	go func() {
		for task := range s.stepChannel {
			task() // Execute Raft step tasks
		}
	}()

	go func() {
		for task := range s.readIndexChannel {
			task() // Execute read index tasks
		}
	}()

	s.initPool() // Initialize buffer pools

	// Initialize WAL (Write-Ahead Log) files
	for i := 0; i < serverFlags.WalFileCount; i++ {
		f, err := os.OpenFile(strconv.Itoa(i), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
		s.walSlots[i] = shared.WalSlot{File: f, Mutex: &sync.Mutex{}}
	}

	// Initialize hard state persistence file
	hardstateFile, err := os.OpenFile("hardstate", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	s.hardstateFile = hardstateFile

	// Initialize bulk WAL file for larger operations
	bulkFile, err := os.OpenFile("bulk", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	s.walBulkFile = bulkFile

	// Initialize database handlers with their own channels
	for i := range s.applyChannels {
		channel := make(chan []byte, 1000000) // High-capacity apply channel
		s.applyChannels[i] = channel
		go s.DbHandler(channel, i) // Start database handler goroutine
	}

	s.setupRaft()              // Initialize Raft consensus
	go s.startPeerListener()   // Start listening for peer connections
	go s.startClientListener() // Start listening for client connections
	s.setupPeerConnections()   // Establish connections to other peers

	return s
}

// StartServer is the main entry point that parses flags and starts the server
func StartServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)

	// Define command-line flags
	var (
		NodeIndex           = fs.Int("node", 0, "node index")
		NumPeerConnections  = fs.Int("peer-connections", 0, "number of peer connections")
		PeerListenAddress   = fs.String("peer-listen", "", "peer listen address")
		ClientListenAddress = fs.String("client-listen", "", "client listen address")
		PeerAddressesString = fs.String("peer-addresses", "", "comma-separated peer addresses")
		WalFileCount        = fs.Int("wal-file-count", 1, "wal file count")
		Fsync               = fs.Bool("fsync", true, "enable fsyncing wal and hardstate files")
		Memory              = fs.Bool("memory", false, "use Memory")
		FastPathWrites      = fs.Bool("fast-path-writes", false, "Skip waiting to apply ")
		NumDbs              = fs.Int("num-dbs", 0, "number of bolt databases")
		MaxDbIndex          = fs.Int("max-db-index", 0, "maximum index for database to partition from")
		Profile             = fs.Bool("profile", false, "enable profiling")
	)

	// Parse command-line arguments
	err := fs.Parse(args)
	if err != nil {
		panic(err)
	}

	// Create ServerFlags struct from parsed values
	flags := &ServerFlags{
		Profile:             *Profile,
		NodeIndex:           *NodeIndex,
		NumPeerConnections:  *NumPeerConnections,
		PeerListenAddress:   *PeerListenAddress,
		ClientListenAddress: *ClientListenAddress,
		PeerAddressesString: *PeerAddressesString,
		WalFileCount:        *WalFileCount,
		Fsync:               *Fsync,
		Memory:              *Memory,
		FastPathWrites:      *FastPathWrites,
		NumDbs:              *NumDbs,
		MaxDbIndex:          *MaxDbIndex,
	}

	// Validate required configuration parameters
	if *NumDbs <= 0 {
		log.Fatalf("-num-dbs must be > 0")
	}
	if *MaxDbIndex <= 0 {
		log.Fatalf("-max-db-index must be > 0")
	}
	if *NumPeerConnections <= 0 {
		log.Fatalf("-peer-connections must be > 0")
	}
	if *PeerListenAddress == "" {
		log.Fatalf("-peer-listen is required")
	}
	if *ClientListenAddress == "" {
		log.Fatalf("-client-listen is required")
	}
	if *PeerAddressesString == "" {
		log.Fatalf("-peer-addresses is required (comma‑separated list)")
	}

	// Validate node index against peer list
	peers := strings.Split(*PeerAddressesString, ",")
	if *NodeIndex < 0 || *NodeIndex >= len(peers) {
		log.Fatalf("-node (%d) out of range; only %d peer addresses supplied",
			*NodeIndex, len(peers))
	}

	if *WalFileCount <= 0 {
		log.Fatalf("-wal-file-count must be > 0")
	}

	// Clean up working directory and initialize
	//err = shared.WipeWorkingDirectory()
	//if err != nil {
	//	panic(err)
	//}

	// Start profiling if enabled
	if flags.Profile {
		startProfiling()
	}

	// Create and run the server
	NewServer(flags).run()
}
