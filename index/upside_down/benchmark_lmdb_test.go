//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

import (
	"testing"

	"github.com/blevesearch/bleve/index/store/lmdb"
)

var lmdbTestConfig = map[string]interface{}{
	"path":    "test",
	"mapsize": int64(10 << 20),
	//"nosync":  true,
}

func BenchmarkLMDBIndexing1Workers(b *testing.B) {
	CommonBenchmarkIndex(b, lmdb.Name, lmdbTestConfig, DestroyTest, 1)
}

func BenchmarkLMDBIndexing2Workers(b *testing.B) {
	CommonBenchmarkIndex(b, lmdb.Name, lmdbTestConfig, DestroyTest, 2)
}

func BenchmarkLMDBIndexing4Workers(b *testing.B) {
	CommonBenchmarkIndex(b, lmdb.Name, lmdbTestConfig, DestroyTest, 4)
}

// batches

func BenchmarkLMDBIndexing1Workers10Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 1, 10)
}

func BenchmarkLMDBIndexing2Workers10Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 2, 10)
}

func BenchmarkLMDBIndexing4Workers10Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 4, 10)
}

func BenchmarkLMDBIndexing1Workers100Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 1, 100)
}

func BenchmarkLMDBIndexing2Workers100Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 2, 100)
}

func BenchmarkLMDBIndexing4Workers100Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 4, 100)
}

func BenchmarkLMDBIndexing1Workers1000Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 1, 1000)
}

func BenchmarkLMDBIndexing2Workers1000Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 2, 1000)
}

func BenchmarkLMDBIndexing4Workers1000Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, lmdb.Name, lmdbTestConfig, DestroyTest, 4, 1000)
}
