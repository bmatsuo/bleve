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
	"github.com/blevesearch/bleve/index/store"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Reader implements store.KVReader.
type Reader struct {
	store *Store
	txn   *lmdb.Txn
}

// Get implements store.KVReader.
func (r *Reader) Get(key []byte) ([]byte, error) {
	v, err := r.txn.Get(r.store.dbi, key)
	if lmdb.IsNotFound(err) || err != nil {
		return nil, nil
	}
	return cp(v), nil
}

// MultiGet implements store.KVReader.
func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, keys)
}

// PrefixIterator implemnts store.KVReader.
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	it := newPrefixIterator(r.txn, r.store.dbi, prefix)
	return it
}

// RangeIterator implements store.KVReader.
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	it := newRangeIterator(r.txn, r.store.dbi, start, end)
	return it
}

// Close terminates the reader transaction.
func (r *Reader) Close() error {
	//r.store.closeTxnReadonly(r.txn)
	r.txn.Abort()
	return nil
}

func cp(b []byte) []byte {
	_b := make([]byte, len(b))
	copy(_b, b)
	return _b
}
