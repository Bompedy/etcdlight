package shared

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const MaxBatchSize = (1024 * 1024) * 75
const PacketHeaderSize = 5

const OpWrite = 0
const OpWriteMemory = 1
const OpRead = 2
const OpReadMemory = 3
const OpLeader = 4
const OpForward = 5
const OpMessage = 6
const OpMessageBatch = 7
const OpReadIndexForward = 8
const OpReadIndexResp = 9
const OpReadIndexAck = 10
const OpReadIndexReq = 11

type MessageBatch struct {
	TotalSize int
	Messages  []raftpb.Message
}

func WriteSize(key []byte, value []byte) int {
	return 8 + len(key) + len(value)
}
func WriteMemorySize(key []byte, value []byte) int {
	return 8 + len(key) + len(value)
}
func ReadSize(key []byte) int {
	return 4 + len(key)
}
func ReadMemorySize(key []byte) int {
	return 4 + len(key)
}
func ForwardSize(data []byte) int {
	return 25 + len(data)
}
func MessageBatchSize(batch *MessageBatch) int {
	return batch.TotalSize + 4
}
func ReadResponseSize(data []byte) int {
	return 4 + len(data)
}
func MessageSize(data []byte) int {
	return 24 + len(data)
}

func PackWritePacket(key []byte, value []byte, buffer []byte) []byte {
	size := WriteSize(key, value)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size)+1)
	buffer[4] = OpWrite
	binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
	copy(buffer[9:], key)
	binary.LittleEndian.PutUint32(buffer[9+len(key):], uint32(len(value)))
	copy(buffer[13+len(key):], value)
	return buffer[:size+PacketHeaderSize]
}
func UnpackWritePacket(buffer []byte) ([]byte, []byte) {
	keySize := binary.LittleEndian.Uint32(buffer)
	key := buffer[4 : 4+keySize]
	valueSize := binary.LittleEndian.Uint32(buffer[4+keySize : 8+keySize])
	value := buffer[8+keySize : 8+keySize+valueSize]
	return key, value
}

func PackWriteMemoryPacket(key []byte, value []byte, buffer []byte) []byte {
	size := WriteMemorySize(key, value)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size)+1)
	buffer[4] = OpWriteMemory
	binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
	copy(buffer[9:], key)
	binary.LittleEndian.PutUint32(buffer[9+len(key):], uint32(len(value)))
	copy(buffer[13+len(key):], value)
	return buffer[:size+PacketHeaderSize]
}
func UnpackWriteMemoryPacket(buffer []byte) ([]byte, []byte) {
	keySize := binary.LittleEndian.Uint32(buffer)
	key := buffer[4 : 4+keySize]
	valueSize := binary.LittleEndian.Uint32(buffer[4+keySize : 8+keySize])
	value := buffer[8+keySize : 8+keySize+valueSize]
	return key, value
}

func PackReadPacket(key []byte, buffer []byte) []byte {
	size := ReadSize(key)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size)+1)
	buffer[4] = OpRead
	binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
	copy(buffer[9:], key)
	return buffer[:size+PacketHeaderSize]
}
func UnpackReadPacket(buffer []byte) []byte {
	keySize := binary.LittleEndian.Uint32(buffer)
	key := buffer[4 : 4+keySize]
	return key
}

func PackReadMemoryPacket(key []byte, buffer []byte) []byte {
	size := ReadMemorySize(key)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size)+1)
	buffer[4] = OpReadMemory
	binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
	copy(buffer[9:], key)
	return buffer[:size+PacketHeaderSize]
}
func UnpackReadMemoryPacket(buffer []byte) []byte {
	keySize := binary.LittleEndian.Uint32(buffer)
	key := buffer[4 : 4+keySize]
	return key
}

func PackLeaderPacket(buffer []byte) []byte {
	binary.LittleEndian.PutUint32(buffer[:4], 1)
	buffer[4] = OpLeader
	return buffer[:PacketHeaderSize]
}

//	func Message(messageId uuid.UUID, ownerId uint32, data []byte, buffer []byte) []byte {
//		total := MessageSize(data)
//		copy(buffer[:16], messageId[:16])
//		binary.LittleEndian.PutUint32(buffer[16:21], ownerId)
//		binary.LittleEndian.PutUint32(buffer[20:25], uint32(len(data)))
//		copy(buffer[24:total], data)
//		return buffer[:total]
//	}
func PackForwardPacket(messageId uuid.UUID, ownerId uint32, data []byte, buffer []byte) []byte {
	size := ForwardSize(data)
	total := size + PacketHeaderSize
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size)+1)
	buffer[4] = OpForward
	copy(buffer[5:21], messageId[:16])
	binary.LittleEndian.PutUint32(buffer[21:25], ownerId)
	binary.LittleEndian.PutUint32(buffer[25:29], uint32(len(data)))
	copy(buffer[29:total], data)
	return buffer[:total]
}
func UnpackForwardPacket(buffer []byte) (uuid.UUID, uint32, []byte) {
	messageId, err := uuid.FromBytes(buffer[:16])
	if err != nil {
		panic(err)
	}
	ownerId := binary.LittleEndian.Uint32(buffer[16:])
	dataSize := binary.LittleEndian.Uint32(buffer[20:])
	data := buffer[24 : 24+dataSize]
	return messageId, ownerId, data
}

func PackMessageBatchPacket(batch *MessageBatch, buffer []byte) []byte {
	size := MessageBatchSize(batch)
	binary.LittleEndian.PutUint32(buffer[0:4], uint32(size+1))
	buffer[4] = OpMessageBatch
	binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(batch.Messages)))
	offset := 9
	for _, msg := range batch.Messages {
		msgSize := msg.Size()
		binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(msgSize))
		size, err := msg.MarshalTo(buffer[offset+4:])
		if err != nil {
			panic(err)
		}
		if size != msg.Size() {
			fmt.Printf("Message size is different: %d - %d\n", size, msgSize)
		}
		offset += msgSize + 4
	}
	return buffer[:size+PacketHeaderSize]
}
func UnpackMessageBatchPacket(buffer []byte) []raftpb.Message {
	batchSize := binary.LittleEndian.Uint32(buffer)
	messages := make([]raftpb.Message, 0)
	offset := uint32(4)
	for range batchSize {
		msgSize := binary.LittleEndian.Uint32(buffer[offset : offset+4])
		offset += 4
		var msg raftpb.Message
		if err := msg.Unmarshal(buffer[offset : offset+msgSize]); err != nil {
			panic(fmt.Sprintf("Error unmarshaling message: %v", err))
		}
		messages = append(messages, msg)
		offset += msgSize
	}
	return messages
}
func PackReadResponsePacket(op byte, data []byte, buffer []byte) []byte {
	size := ReadResponseSize(data)
	total := size + PacketHeaderSize
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size)+1)
	buffer[4] = op
	binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(data)))
	copy(buffer[9:total], data)
	return buffer[:total]
}
func PackWriteResponsePacket(op byte, buffer []byte) []byte {
	binary.LittleEndian.PutUint32(buffer[:4], 1)
	buffer[4] = op
	return buffer[:PacketHeaderSize]
}

func PackMessage(messageId uuid.UUID, ownerId uint32, data []byte, buffer []byte) []byte {
	total := MessageSize(data)
	copy(buffer[:16], messageId[:16])
	binary.LittleEndian.PutUint32(buffer[16:21], ownerId)
	binary.LittleEndian.PutUint32(buffer[20:25], uint32(len(data)))
	copy(buffer[24:total], data)
	return buffer[:total]
}
func UnpackMessage(buffer []byte) (uuid.UUID, uint32, []byte) {
	messageId, err := uuid.FromBytes(buffer[:16])
	if err != nil {
		panic(err)
	}
	ownerId := binary.LittleEndian.Uint32(buffer[16:])
	dataSize := binary.LittleEndian.Uint32(buffer[20:])
	data := buffer[24 : 24+dataSize]
	return messageId, ownerId, data
}

type Pool[T any] struct {
	Head     uint32
	Size     uint32
	Elements []T
	Lock     sync.Mutex
	New      func() T
}

func (pool *Pool[T]) put(element T) {
	if pool.Head >= pool.Size {
		nextElements := make([]T, pool.Size*2)
		copy(nextElements[:pool.Size], pool.Elements)
		pool.Elements = nextElements
		pool.Size *= 2
	}
	pool.Elements[pool.Head] = element
	pool.Head++
}

func (pool *Pool[T]) Get() T {
	pool.Lock.Lock()
	if pool.Head == 0 {
		pool.put(pool.New())
	}
	pool.Head--
	result := pool.Elements[pool.Head]
	pool.Lock.Unlock()
	return result
}

func (pool *Pool[T]) Put(element T) {
	pool.Lock.Lock()
	pool.put(element)
	pool.Lock.Unlock()
}

func NewPool[T any](initialSize uint32, new func() T) *Pool[T] {
	pool := &Pool[T]{
		Head:     initialSize,
		Size:     initialSize,
		Elements: make([]T, initialSize),
		Lock:     sync.Mutex{},
		New:      new,
	}
	for i := uint32(0); i < initialSize; i++ {
		pool.Elements[i] = new()
	}
	return pool
}

type Client struct {
	Connection     net.Conn
	Channel        chan func()
	ProposeChannel chan func()
}

type PendingRead struct {
	Client Client
	Data   []byte
}

func ReadPacket(connection net.Conn, buffer []byte) int {
	err := Read(connection, buffer[:4])
	if err != nil {
		log.Fatalf("Error reading packet: %v", err)
	}
	size := binary.LittleEndian.Uint32(buffer[:4])
	err = Read(connection, buffer[:size])
	if err != nil {
		log.Fatalf("Error reading packet: %v", err)
	}
	return int(size)
}

func Read(connection net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := connection.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

func Write(connection net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := connection.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

type ClientRequest struct {
	Connection net.Conn
	WriteLock  *sync.Mutex
}

type PeerConnection struct {
	Connection *net.Conn
	Channel    chan func()
}

type WalSlot struct {
	File  *os.File
	Mutex *sync.Mutex
}

func WipeWorkingDirectory() error {
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}
	exeBase := filepath.Base(exePath)

	files, err := os.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.Name() == exeBase {
			continue // Skip the running executable
		}

		err := os.RemoveAll(file.Name())
		if err != nil {
			return fmt.Errorf("failed to remove %s: %w", file.Name(), err)
		}
	}

	return nil
}
