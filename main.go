package main

import (
	"errors"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

//////TransactionLogger/////

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
}

/////FileTransactionLogger/////

type FileTransactionLogger struct {
	// file *os.File
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	// fmt.Fprintf(l.file, "DELETE %s\n", key)
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	// fmt.Fprintf(l.file, "PUT %s %s\n", key, value)
}

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
	r := mux.NewRouter()

	r.HandleFunc("/v1/key/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", keyValueGetHandler).Methods("GET")

	log.Fatal(http.ListenAndServe(":8080", r))
}
