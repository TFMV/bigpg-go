// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

package memory

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

var memPool sync.Pool

func init() {
	memPool = sync.Pool{
		New: func() interface{} {
			// This creates a new GoAllocator when the pool is empty
			return memory.NewGoAllocator()
		},
	}
}

// getAllocator retrieves an allocator from the pool
func getAllocator() memory.Allocator {
	// Get an allocator from the pool, or create a new one if the pool is empty
	return memPool.Get().(memory.Allocator)
}

// putAllocator returns an allocator back to the pool
func putAllocator(alloc memory.Allocator) {
	// Reset or clean up the allocator if necessary before putting it back
	memPool.Put(alloc)
}

// GetAllocator is a public function to retrieve an allocator from the pool
func GetAllocator() memory.Allocator {
	return getAllocator()
}

// PutAllocator is a public function to return an allocator back to the pool
func PutAllocator(alloc memory.Allocator) {
	putAllocator(alloc)
}

// NewGoAllocator creates a new Go allocator without using the pool
func NewGoAllocator() memory.Allocator {
	return memory.NewGoAllocator()
}

// NewAllocator returns the default allocator, which in this case is also a GoAllocator
func NewAllocator() memory.Allocator {
	return memory.DefaultAllocator
}

func Reset() {
	memPool = sync.Pool{
		New: func() interface{} {
			// This creates a new GoAllocator when the pool is empty
			return memory.NewGoAllocator()
		},
	}
}
