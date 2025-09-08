package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
)

///// Event /////

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type EventType byte

const (
	_ = iota
	EventDelete
	EventPut
)

//////TransactionLogger/////

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	Run()
	ReadEvents() (<-chan Event, <-chan error)
}

/////FileTransactionLogger/////

type FileTransactionLogger struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewFileTransactionLogger(fileName string) (TransactionLogger, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	///TODO:: проверить дескриптор файла
	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	errors := make(chan error, 1)
	l.events = events
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(l.file, "%d\t%d\t%s\t%s\n", l.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {

			line := scanner.Text()
			_, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("input parse error %q: %w", line, err)
				return
			}

			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("input parse error %q: sequence not increasing", line)
				return
			}
			l.lastSequence = e.Sequence

			outEvent <- e
		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return

		}
	}()
	return outEvent, outError
}

func inizializeTransactionLogger() error {
	var err error
	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventPut:
				err = Put(e.Key, e.Value)
			case EventDelete:
				err = Delete(e.Key)
			default:
				err = fmt.Errorf("unknown event type: %d", e.EventType)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("failed to read events from transaction log: %w", err)
	}

	logger.Run()
	return err
}

///LOGGER/////

var logger TransactionLogger

// //////STORE//////////
var ErrorNoSuchKey = errors.New("no such key")

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

func Put(key, value string) error {
	store.Lock()
	defer store.Unlock()
	store.m[key] = value
	return nil
}

func Get(key string) (string, error) {
	store.RLock()
	defer store.RUnlock()
	value, ok := store.m[key]
	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}
func Delete(key string) error {
	store.Lock()
	defer store.Unlock()
	_, ok := store.m[key]
	if !ok {
		return ErrorNoSuchKey
	}
	delete(store.m, key)
	logger.WriteDelete(key)
	return nil
}

// ///HANDLERS//////////
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if err == ErrorNoSuchKey {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
}
func main() {
	errInit := inizializeTransactionLogger()
	if errInit != nil {
		log.Fatalf("Failed to initialize transaction logger: %v", errInit)
	}
	r := mux.NewRouter()

	r.HandleFunc("/v1/key/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", keyValueGetHandler).Methods("GET")

	log.Fatal(http.ListenAndServe(":8080", r))
}
