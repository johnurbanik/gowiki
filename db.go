package main

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/oleiade/lane"
)

// DB is a db.
type DB struct {
	*bolt.DB
	mu      sync.Mutex
	updates *lane.Stack
}

// Tx represents a transaction.
type Tx struct {
	*bolt.Tx
	db *DB
}

// NewDB creates a db object to wrap the db in.
func NewDB() *DB {
	return &DB{}
}

// Open opens a DB that exists at the path Path
func (db *DB) Open(path string) error {
	var err error
	db.DB, err = bolt.Open(path, 0600, nil)
	db.updates = lane.NewStack()
	if err != nil {
		db.Close()
		return err
	}
	return nil
}

// View executes a function in the context of a read-only transaction.
func (db *DB) View(fn func(*Tx) error) error {
	return db.DB.View(func(tx *bolt.Tx) error {
		return fn(&Tx{tx, db})
	})
}

// Update executes a function in the context of a writable transaction.
func (db *DB) Update(fn func(*Tx) error) error {
	return db.DB.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{tx, db})
	})
}

// Batch executes a function that can be batched.
func (db *DB) Batch(fn func(*Tx) error) error {
	return db.DB.Batch(func(tx *bolt.Tx) error {
		return fn(&Tx{tx, db})
	})
}

// Meta retrieves a meta field by name.
func (tx *Tx) Meta(key string) string {
	return string(tx.Bucket([]byte("meta")).Get([]byte(key)))
}
