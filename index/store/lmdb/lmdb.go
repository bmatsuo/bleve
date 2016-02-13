// Package lmdb implements a store.KVStore on top of BoltDB. It supports the
// following options:
//
// "mapsize" (int64): the size of the LMDB memory map (max index size).
//
// "db" (string): the name of LMDB database to use, defaults to "bleve".
//
// "nosync" (bool): if true, set boltdb.DB.NoSync to true. It speeds up index
// operations in exchange of losing integrity guarantees if indexation aborts
// without closing the index. Use it when rebuilding indexes from zero.
package lmdb

import "github.com/blevesearch/bleve/registry"

// Name is the name of the store implementation
const Name = "lmdb"

func init() {
	registry.RegisterKVStore(Name, New)
}
