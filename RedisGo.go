package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ValueType represents the type of a Redis value
type ValueType int

const (
	String ValueType = iota
	List
	// We'll add Set, Hash, etc. later
)

// Value represents a value in our Redis clone
type Value struct {
	Type ValueType
	Data interface{}
}

// KVStore represents our in-memory database
type KVStore struct {
	mu        sync.RWMutex
	store     map[string]Value
	dbFile    string
	saveTimer *time.Timer
}

// NewKVStore creates a new key-value store
func NewKVStore(dbFile string) *KVStore {
	kv := &KVStore{
		store:  make(map[string]Value),
		dbFile: dbFile,
	}
	
	// Register types for gob encoding
	gob.Register([]string{})
	
	// Load data from file if it exists
	kv.Load()
	
	// Start background saving
	kv.startBackgroundSave()
	
	return kv
}

// Save persists the database to disk
func (kv *KVStore) Save() error {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	
	tempFile := kv.dbFile + ".tmp"
	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(kv.store)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to encode data: %w", err)
	}
	
	err = file.Close()
	if err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	
	// Atomically replace the old file with the new one
	return os.Rename(tempFile, kv.dbFile)
}

// Load loads the database from disk
func (kv *KVStore) Load() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	file, err := os.Open(kv.dbFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, not an error
			return nil
		}
		return fmt.Errorf("failed to open database file: %w", err)
	}
	defer file.Close()
	
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&kv.store)
	if err != nil {
		return fmt.Errorf("failed to decode data: %w", err)
	}
	
	return nil
}

// startBackgroundSave starts a background saving process
func (kv *KVStore) startBackgroundSave() {
	// Save every 2 minutes
	kv.saveTimer = time.AfterFunc(2*time.Minute, func() {
		err := kv.Save()
		if err != nil {
			log.Printf("Error saving database: %v", err)
		} else {
			log.Println("Database saved successfully")
		}
		kv.startBackgroundSave() // Schedule next save
	})
}

// GetString retrieves a string value by key
func (kv *KVStore) GetString(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	
	val, exists := kv.store[key]
	if !exists || val.Type != String {
		return "", false
	}
	
	return val.Data.(string), true
}

// SetString stores a string key-value pair
func (kv *KVStore) SetString(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	kv.store[key] = Value{
		Type: String,
		Data: value,
	}
}

// LPush adds values to the start of a list
func (kv *KVStore) LPush(key string, values ...string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	val, exists := kv.store[key]
	var list []string
	
	if !exists {
		list = []string{}
	} else if val.Type != List {
		// If the key exists but is not a list, overwrite it
		list = []string{}
	} else {
		list = val.Data.([]string)
	}
	
	// Add values to the beginning of the list
	newList := make([]string, len(values)+len(list))
	for i, v := range values {
		newList[i] = v
	}
	copy(newList[len(values):], list)
	
	kv.store[key] = Value{
		Type: List,
		Data: newList,
	}
	
	return len(newList)
}

// RPush adds values to the end of a list
func (kv *KVStore) RPush(key string, values ...string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	val, exists := kv.store[key]
	var list []string
	
	if !exists {
		list = []string{}
	} else if val.Type != List {
		// If the key exists but is not a list, overwrite it
		list = []string{}
	} else {
		list = val.Data.([]string)
	}
	
	// Add values to the end of the list
	list = append(list, values...)
	
	kv.store[key] = Value{
		Type: List,
		Data: list,
	}
	
	return len(list)
}

// LPop removes and returns the first element from a list
func (kv *KVStore) LPop(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	val, exists := kv.store[key]
	if !exists || val.Type != List {
		return "", false
	}
	
	list := val.Data.([]string)
	if len(list) == 0 {
		return "", false
	}
	
	// Remove the first element
	item := list[0]
	list = list[1:]
	
	// Update the list in the store
	if len(list) == 0 {
		delete(kv.store, key)
	} else {
		kv.store[key] = Value{
			Type: List,
			Data: list,
		}
	}
	
	return item, true
}

// RPop removes and returns the last element from a list
func (kv *KVStore) RPop(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	val, exists := kv.store[key]
	if !exists || val.Type != List {
		return "", false
	}
	
	list := val.Data.([]string)
	if len(list) == 0 {
		return "", false
	}
	
	// Remove the last element
	lastIdx := len(list) - 1
	item := list[lastIdx]
	list = list[:lastIdx]
	
	// Update the list in the store
	if len(list) == 0 {
		delete(kv.store, key)
	} else {
		kv.store[key] = Value{
			Type: List,
			Data: list,
		}
	}
	
	return item, true
}

// LRange returns a range of elements from a list
func (kv *KVStore) LRange(key string, start, stop int) []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	
	val, exists := kv.store[key]
	if !exists || val.Type != List {
		return []string{}
	}
	
	list := val.Data.([]string)
	length := len(list)
	
	// Handle negative indices (counting from the end)
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	
	if stop < 0 {
		stop = length + stop
	}
	
	// Bounds checking
	if start >= length || start > stop {
		return []string{}
	}
	
	if stop >= length {
		stop = length - 1
	}
	
	// Return the range
	return list[start : stop+1]
}

// Delete removes a key
func (kv *KVStore) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	_, exists := kv.store[key]
	if exists {
		delete(kv.store, key)
		return true
	}
	return false
}

// RESP Protocol constants
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

// parseRESP parses a RESP message from the reader
func parseRESP(reader *bufio.Reader) ([]string, error) {
	// Read the first byte to determine the type
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	
	if firstByte != Array {
		// Put the byte back
		reader.UnreadByte()
		// Try to read a simple line command
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return strings.Fields(strings.TrimSpace(line)), nil
	}
	
	// Read array length
	lenStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	lenStr = strings.TrimSpace(lenStr)
	length, err := strconv.Atoi(lenStr)
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", lenStr)
	}
	
	// Read each element in the array
	result := make([]string, length)
	for i := 0; i < length; i++ {
		// Each element must be a bulk string
		typeByte, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		
		if typeByte != BulkString {
			return nil, fmt.Errorf("expected bulk string, got %c", typeByte)
		}
		
		// Read string length
		lenStr, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenStr = strings.TrimSpace(lenStr)
		strLen, err := strconv.Atoi(lenStr)
		if err != nil {
			return nil, fmt.Errorf("invalid string length: %s", lenStr)
		}
		
		// Read the string data
		if strLen < 0 {
			// Null string
			result[i] = ""
			continue
		}
		
		data := make([]byte, strLen+2) // +2 for the trailing \r\n
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, err
		}
		result[i] = string(data[:strLen])
	}
	
	return result, nil
}

// serializeRESP writes a RESP response
func serializeRESP(w io.Writer, value interface{}) error {
	switch v := value.(type) {
	case string:
		// Simple string
		_, err := fmt.Fprintf(w, "+%s\r\n", v)
		return err
		
	case error:
		// Error
		_, err := fmt.Fprintf(w, "-%s\r\n", v.Error())
		return err
		
	case int:
		// Integer
		_, err := fmt.Fprintf(w, ":%d\r\n", v)
		return err
		
	case []byte:
		// Bulk string
		if v == nil {  // nil (null) bulk string
			_, err := w.Write([]byte("$-1\r\n"))
			return err
		}
		_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v), v)
		return err
		
	case []string:
		// Array of strings
		_, err := fmt.Fprintf(w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		
		for _, item := range v {
			// Each item is a bulk string
			err := serializeRESP(w, []byte(item))
			if err != nil {
				return err
			}
		}
		return nil
		
	case []interface{}:
		// Array
		if v == nil {  // nil array
			_, err := w.Write([]byte("*-1\r\n"))
			return err
		}
		
		_, err := fmt.Fprintf(w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		
		for _, item := range v {
			err := serializeRESP(w, item)
			if err != nil {
				return err
			}
		}
		return nil
		
	default:
		return fmt.Errorf("unsupported type: %T", value)
	}
}

func handleConnection(conn net.Conn, kv *KVStore) {
	defer conn.Close()
	
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	
	for {
		// Parse the command using RESP protocol
		args, err := parseRESP(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error parsing command:", err.Error())
			serializeRESP(writer, fmt.Errorf("ERR parsing error: %v", err))
			writer.Flush()
			continue
		}
		
		if len(args) == 0 {
			continue
		}
		
		cmd := strings.ToUpper(args[0])
		
		switch cmd {
		case "GET":
			if len(args) != 2 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'get' command"))
				writer.Flush()
				continue
			}
			
			val, exists := kv.GetString(args[1])
			if !exists {
				serializeRESP(writer, []byte(nil))
			} else {
				serializeRESP(writer, []byte(val))
			}
			
		case "SET":
			if len(args) != 3 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'set' command"))
				writer.Flush()
				continue
			}
			
			kv.SetString(args[1], args[2])
			serializeRESP(writer, "OK")
			
		case "DEL":
			if len(args) != 2 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'del' command"))
				writer.Flush()
				continue
			}
			
			if kv.Delete(args[1]) {
				serializeRESP(writer, 1)
			} else {
				serializeRESP(writer, 0)
			}
			
		case "LPUSH":
			if len(args) < 3 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'lpush' command"))
				writer.Flush()
				continue
			}
			
			count := kv.LPush(args[1], args[2:]...)
			serializeRESP(writer, count)
			
		case "RPUSH":
			if len(args) < 3 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'rpush' command"))
				writer.Flush()
				continue
			}
			
			count := kv.RPush(args[1], args[2:]...)
			serializeRESP(writer, count)
			
		case "LPOP":
			if len(args) != 2 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'lpop' command"))
				writer.Flush()
				continue
			}
			
			val, exists := kv.LPop(args[1])
			if !exists {
				serializeRESP(writer, []byte(nil))
			} else {
				serializeRESP(writer, []byte(val))
			}
			
		case "RPOP":
			if len(args) != 2 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'rpop' command"))
				writer.Flush()
				continue
			}
			
			val, exists := kv.RPop(args[1])
			if !exists {
				serializeRESP(writer, []byte(nil))
			} else {
				serializeRESP(writer, []byte(val))
			}
			
		case "LRANGE":
			if len(args) != 4 {
				serializeRESP(writer, fmt.Errorf("ERR wrong number of arguments for 'lrange' command"))
				writer.Flush()
				continue
			}
			
			start, err := strconv.Atoi(args[2])
			if err != nil {
				serializeRESP(writer, fmt.Errorf("ERR value is not an integer or out of range"))
				writer.Flush()
				continue
			}
			
			stop, err := strconv.Atoi(args[3])
			if err != nil {
				serializeRESP(writer, fmt.Errorf("ERR value is not an integer or out of range"))
				writer.Flush()
				continue
			}
			
			values := kv.LRange(args[1], start, stop)
			serializeRESP(writer, values)
			
		case "SAVE":
			err := kv.Save()
			if err != nil {
				serializeRESP(writer, fmt.Errorf("ERR %v", err))
			} else {
				serializeRESP(writer, "OK")
			}
			
		case "PING":
			serializeRESP(writer, "PONG")
			
		case "QUIT":
			serializeRESP(writer, "OK")
			writer.Flush()
			return
			
		default:
			serializeRESP(writer, fmt.Errorf("ERR unknown command '%s'", cmd))
		}
		
		writer.Flush()
	}
}

func main() {
	fmt.Println("Starting Redis clone server on port 6379...")
	
	// Create the key-value store with persistence
	kv := NewKVStore("redis-clone.db")
	
	// Start the server
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error starting server:", err.Error())
		return
	}
	defer listener.Close()
	
	fmt.Println("Server is ready to accept connections")
	
	// Handle graceful shutdown
	shutdownCh := make(chan os.Signal, 1)
	go func() {
		<-shutdownCh
		fmt.Println("Shutting down server...")
		
		// Save the database before exiting
		err := kv.Save()
		if err != nil {
			fmt.Printf("Error saving database: %v\n", err)
		} else {
			fmt.Println("Database saved successfully")
		}
		
		// Close the listener
		listener.Close()
		os.Exit(0)
	}()
	
	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if the server was shut down
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		
		fmt.Printf("New connection from %s\n", conn.RemoteAddr())
		go handleConnection(conn, kv)
	}
}
