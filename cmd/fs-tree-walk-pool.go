/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"errors"
	"sync"
	"time"
)

// Global lookup timeout.
const (
	globalLookupTimeout    = time.Minute * 30 // 30minutes.
	treeWalkEntryLimit     = 50
	treeWalkSameEntryLimit = 2
)

// listParams - list object params used for list object map
type listParams struct {
	bucket    string
	recursive bool
	marker    string
	prefix    string
}

// errWalkAbort - returned by doTreeWalk() if it returns prematurely.
// doTreeWalk() can return prematurely if
// 1) treeWalk is timed out by the timer go-routine.
// 2) there is an error during tree walk.
var errWalkAbort = errors.New("treeWalk abort")

// treeWalk - represents the go routine that does the file tree walk.
type treeWalk struct {
	added     time.Time
	resultCh  chan TreeWalkResult
	endWalkCh chan struct{} // To signal when treeWalk go-routine should end.
}

// TreeWalkPool - pool of treeWalk go routines.
// A treeWalk is added to the pool by Set() and removed either by
// doing a Release() or if the concerned timer goes off.
// treeWalkPool's purpose is to maintain active treeWalk go-routines in a map so that
// it can be looked up across related list calls.
type TreeWalkPool struct {
	mu      sync.Mutex
	pool    map[listParams][]treeWalk
	timeOut time.Duration
}

// NewTreeWalkPool - initialize new tree walk pool.
func NewTreeWalkPool(timeout time.Duration) *TreeWalkPool {
	tPool := &TreeWalkPool{
		pool:    make(map[listParams][]treeWalk),
		timeOut: timeout,
	}
	go tPool.cleanup()
	return tPool
}

// Release - selects a treeWalk from the pool based on the input
// listParams, removes it from the pool, and returns the TreeWalkResult
// channel.
// Returns nil if listParams does not have an associated treeWalk.
func (t *TreeWalkPool) Release(params listParams) (resultCh chan TreeWalkResult, endWalkCh chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	walks, ok := t.pool[params] // Pick the valid walks.
	if !ok || len(walks) == 0 {
		// Release return nil if params not found.
		return nil, nil
	}

	// Pop out the first valid walk entry.
	walk := walks[0]
	walks = walks[1:]
	if len(walks) > 0 {
		t.pool[params] = walks
	} else {
		delete(t.pool, params)
	}
	return walk.resultCh, walk.endWalkCh
}

// Set - adds a treeWalk to the treeWalkPool.
func (t *TreeWalkPool) Set(params listParams, resultCh chan TreeWalkResult, endWalkCh chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// If we are above the limit delete at least one entry from the pool.
	if len(t.pool) > treeWalkEntryLimit {
		age := time.Now()
		var oldest listParams
		for k, v := range t.pool {
			if len(v) > 0 && v[0].added.Before(age) {
				oldest = k
				age = v[0].added
			}
		}
		// Invalidate and delete oldest.
		if walks, ok := t.pool[oldest]; ok && len(walks) > 0 {
			close(walks[0].endWalkCh)
			if len(walks) > 1 {
				t.pool[oldest] = walks[1:]
			} else {
				delete(t.pool, oldest)
			}
		}
	}

	walkInfo := treeWalk{
		added:     UTCNow(),
		resultCh:  resultCh,
		endWalkCh: endWalkCh,
	}

	// Append new walk info.
	walks := t.pool[params]
	if len(walks) >= treeWalkSameEntryLimit {
		close(walks[0].endWalkCh)
		walks = walks[1:]
	}
	t.pool[params] = append(walks, walkInfo)
}

func (t *TreeWalkPool) cleanup() {
	for {
		t.mu.Lock()
		for k, v := range t.pool {
			if len(v) > 0 && v[0].added.Add(t.timeOut).Before(UTCNow()) {
				close(v[0].endWalkCh)
				if len(v) > 1 {
					t.pool[k] = v[1:]
				} else {
					delete(t.pool, k)
				}
			}
		}
		t.mu.Unlock()
		time.Sleep(t.timeOut / 10)
	}
}
