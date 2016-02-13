//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package lmdb

import (
	"fmt"
	"os"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Store implements the store.KVStore interface with an LMDB memory mapped file
// backend.
type Store struct {
	path   string
	db     string
	dbi    lmdb.DBI
	env    *lmdb.Env
	noSync bool
	mo     store.MergeOperator
	txpool *sync.Pool
}

// New allocates and returns a new store.KVStore backed by an LMDB database.
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	store := &Store{mo: mo}

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	store.path = path

	dbname, ok := config["db"].(string)
	if !ok {
		dbname = "bleve"
	}
	store.db = dbname

	mapsize, ok := config["mapsize"].(int64)
	if !ok {
		return nil, fmt.Errorf("must specify map size")
	}
	if mapsize == 0 {
		mapsize = 1 << 20
	}

	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	store.env = env
	err = env.SetMapSize(mapsize)
	if err != nil {
		return nil, err
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		return nil, err
	}

	flags := uint(lmdb.NoTLS)
	noSync, _ := config["nosync"].(bool)
	if noSync {
		store.noSync = noSync
		flags |= lmdb.NoSync
	}

	err = os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}
	err = env.Open(path, flags, 0644)
	if err != nil {
		env.Close()
		return nil, err
	}

	store.txpool = &sync.Pool{}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		store.dbi, err = txn.CreateDBI(dbname)
		return err
	})
	if err != nil {
		env.Close()
		return nil, err
	}

	return store, nil
}

// Close closes the unmaps the store and closes the filehandle.
func (store *Store) Close() error {
	return store.env.Close()
}

// Reader returns a new store.KVReader for the store.
func (store *Store) Reader() (store.KVReader, error) {
	txn, err := store.openTxnReadonly()
	if err != nil {
		return nil, err
	}
	txn.RawRead = true
	return &Reader{
		store: store,
		txn:   txn,
	}, nil
}

// Writer returns a new store.KVWriter for the store.
func (store *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: store,
	}, nil
}

func (store *Store) openTxnReadonly() (*lmdb.Txn, error) {
	txn, ok := store.txpool.Get().(*lmdb.Txn)
	if !ok {
		return store.env.BeginTxn(nil, lmdb.Readonly)
	}
	err := txn.Renew()
	if err != nil {
		txn.Abort()
		return store.env.BeginTxn(nil, lmdb.Readonly)
	}
	return txn, nil
}

func (store *Store) closeTxnReadonly(txn *lmdb.Txn) {
	txn.Reset()
	store.txpool.Put(txn)
}
