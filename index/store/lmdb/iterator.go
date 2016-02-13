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
	"bytes"
	"fmt"
	"log"

	"github.com/blevesearch/bleve/index/store"
	"github.com/bmatsuo/lmdb-go/exp/lmdbscan"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

type req struct {
	key  []byte
	item chan<- *item
}

type item struct {
	key []byte
	val []byte
	ok  bool
}

type iterator struct {
	Cont func(k, v []byte) bool
	cls  chan struct{}
	term chan struct{}
	s    *lmdbscan.Scanner
	req  chan *req
	io   <-chan *item
	item chan<- *item
	curr *item
}

func newIterator(txn *lmdb.Txn, dbi lmdb.DBI) *iterator {
	it := &iterator{
		cls:  make(chan struct{}),
		term: make(chan struct{}),
		s:    lmdbscan.New(txn, dbi),
	}
	if it.s.Err() != nil {
		log.Print(it.s.Err())
	}
	return it
}

func (it *iterator) start(k []byte) {
	go it.loop(k)
}

func (it *iterator) send(k, v []byte) ([]byte, bool) {
	var i *item
	if len(k) == 0 {
		i = &item{}
	} else {
		i = &item{k, v, true}
	}

	select {
	case <-it.cls:
		close(it.term)
		return nil, false
	case req := <-it.req:
		if req.key != nil {
			log.Print("SEEK")
			it.item = req.item
			return req.key, false
		}
	}

	select {
	case <-it.cls:
		log.Print("closed")
		close(it.term)
		return nil, false
	case it.item <- i:
		log.Print("sent")
		return nil, true
	}
}

func (it *iterator) loop(k []byte) {
	defer it.s.Close()
	defer it.send(nil, nil)
	it.seek(k)
	for it.s.Scan() {
		if it.Cont != nil && !it.Cont(it.s.Key(), it.s.Val()) {
			log.Printf("stop")
			return
		}
		k, ok := it.send(it.s.Key(), it.s.Val())
		if !ok {
			if k == nil {
				return
			}
			it.seek(k)
		}
	}
	if it.s.Err() != nil {
		log.Print(it.s.Err())
	}
	log.Printf("END")
}

// Seek implements store.KVIterator
func (it *iterator) Seek(k []byte) {
	if k == nil {
		k = []byte{}
	}
	c := make(chan *item)
	req := &req{key: k, item: c}
	select {
	case <-it.term:
		return
	case it.req <- req:
		it.io = c
	}
	it.Next()
}

func (it *iterator) seek(k []byte) {
	log.Print("seek")
	flag := uint(lmdb.SetRange)
	if len(k) == 0 {
		flag = lmdb.First
	}
	it.s.Set(k, nil, flag)
}

// Next implemnts store.KVIterator
func (it *iterator) Next() {
	it.req <- &req{}
	var ok bool
	select {
	case <-it.term:
		it.curr = nil
	case it.curr, ok = <-it.io:
		if !ok {
			panic("concurrent method calls detected")
		}
	}
}

// Current implements store.KVIterator
func (it *iterator) Current() ([]byte, []byte, bool) {
	if it.curr == nil {
		return nil, nil, false
	}
	return cp(it.curr.key), cp(it.curr.val), it.curr.ok
}

// Key implements store.KVIterator
func (it *iterator) Key() []byte {
	if it.curr == nil {
		return nil
	}
	return cp(it.curr.key)
}

// Value implements store.KVIterator
func (it *iterator) Value() []byte {
	if it.curr == nil {
		return nil
	}
	return cp(it.curr.val)
}

// Valid implements store.KVIterator
func (it *iterator) Valid() bool {
	if it.curr == nil {
		return false
	}
	return it.curr.ok
}

// Close implements store.KVIterator
func (it *iterator) Close() (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("already closed")
		}
	}()
	close(it.cls)
	<-it.term
	return nil
}

func newPrefixIterator(txn *lmdb.Txn, dbi lmdb.DBI, prefix []byte) store.KVIterator {
	it := newIterator(txn, dbi)
	it.Cont = func(k, v []byte) bool {
		return bytes.HasPrefix(k, prefix)
	}
	it.start(prefix)
	return &prefixIterator{
		iterator: it,
		prefix:   prefix,
	}

}

// prefixIterator implements store.KVIterator
type prefixIterator struct {
	*iterator
	prefix []byte
}

// Seek implements store.KVIterator
func (i *prefixIterator) Seek(k []byte) {
	if !bytes.HasPrefix(k, i.prefix) {
		if bytes.Compare(k, i.prefix) < 0 {
			k = i.prefix
		} else {
			i.curr = nil
			return
		}
	}

	i.iterator.Seek(k)
}

func newRangeIterator(txn *lmdb.Txn, dbi lmdb.DBI, start, end []byte) store.KVIterator {
	it := newIterator(txn, dbi)
	it.Cont = func(k, v []byte) bool {
		return end == nil || bytes.Compare(k, end) < 0
	}
	it.start(start)
	return &rangeIterator{
		iterator: it,
		start:    start,
		end:      end,
	}
}

// rangeIterator implements store.KVIterator
type rangeIterator struct {
	*iterator
	start []byte
	end   []byte
}

// Seek implements store.KVIterator
func (i *rangeIterator) Seek(k []byte) {
	if i.start != nil && bytes.Compare(k, i.start) < 0 {
		k = i.start
	}
	i.iterator.Seek(k)
}
