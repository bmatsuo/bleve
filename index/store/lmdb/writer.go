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

	"github.com/blevesearch/bleve/index/store"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Writer implements store.KVWriter.
type Writer struct {
	store *Store
}

// NewBatch implements store.KVWriter.
func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.store.mo)
}

// NewBatchEx implements store.KVWriter.
func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

// ExecuteBatch implements store.KVWriter.
func (w *Writer) ExecuteBatch(batch store.KVBatch) error {

	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	return w.store.env.Update(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true
		cursor, err := txn.OpenCursor(w.store.dbi)
		if err != nil {
			return err
		}
		defer cursor.Close()

		for k, mergeOps := range emulatedBatch.Merger.Merges {
			kb := []byte(k)
			_, existingVal, err := cursor.Get(kb, nil, 0)
			if err != nil && !lmdb.IsNotFound(err) {
				return err
			}
			mergedVal, fullMergeOk := w.store.mo.FullMerge(kb, existingVal, mergeOps)
			if !fullMergeOk {
				return fmt.Errorf("merge operator returned failure")
			}
			err = cursor.Put(kb, mergedVal, 0)
			if err != nil {
				return err
			}
		}

		for _, op := range emulatedBatch.Ops {
			if op.V != nil {
				err := cursor.Put(op.K, op.V, 0)
				if err != nil {
					return err
				}
			} else {
				err := txn.Del(w.store.dbi, op.K, nil)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// Close implements store.KVWriter.
func (w *Writer) Close() error {
	return nil
}
