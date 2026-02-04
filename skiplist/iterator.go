// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package skiplist

import "sync/atomic"
import "unsafe"

// Iterator is used for lookup and range operations on skiplist
type Iterator struct {
	cmp           CompareFn
	s             *Skiplist
	prev, curr    *Node
	valid         bool
	buf           *ActionBuffer
	deleted       bool
	isReverseIter bool

	bs          *BarrierSession
	count       uint
	smrInterval uint
}

// NewIterator creates an iterator for skiplist
func (s *Skiplist) NewIterator(cmp CompareFn,
	buf *ActionBuffer) *Iterator {
	it := s.NewIterator2(cmp, buf)
	it.bs = s.barrier.Acquire()
	return it
}

func (s *Skiplist) NewIterator2(cmp CompareFn,
	buf *ActionBuffer) *Iterator {
	return &Iterator{
		cmp:         cmp,
		s:           s,
		buf:         buf,
		smrInterval: ^uint(0),
	}
}

// SeekFirst moves cursor to the start
func (it *Iterator) SeekFirst() {
	it.prev = it.s.head
	it.curr, _ = it.s.head.getNext(0)
	it.valid = true
}

// SeekWithCmp moves iterator to a provided item by using custom comparator
func (it *Iterator) SeekWithCmp(itm unsafe.Pointer, cmp CompareFn, eqCmp CompareFn) bool {
	var found bool
	if found = it.s.findPath(itm, cmp, it.buf, &it.s.Stats) != nil; found {
		it.prev = it.buf.preds[0]
		it.curr = it.buf.succs[0]
	} else {
		if found = eqCmp != nil && compare(eqCmp, itm, it.buf.preds[0].Item()) == 0; found {
			it.prev = nil
			it.curr = it.buf.preds[0]
		}
	}
	return found
}

// Seek moves iterator to a provided item
func (it *Iterator) Seek(itm unsafe.Pointer) bool {
	it.valid = true
	found := it.s.findPath(itm, it.cmp, it.buf, &it.s.Stats) != nil
	it.prev = it.buf.preds[0]
	it.curr = it.buf.succs[0]
	return found
}

// Valid returns true when iterator reaches the end
func (it *Iterator) Valid() bool {
	if it.isReverseIter {
		if it.valid && it.curr == it.s.head {
			it.valid = false
		}
	} else {
		if it.valid && it.curr == it.s.tail {
			it.valid = false
		}
	}
	return it.valid
}

// Get returns the current item
func (it *Iterator) Get() unsafe.Pointer {
	return it.curr.Item()
}

// GetNode returns node which holds the current item
func (it *Iterator) GetNode() *Node {
	return it.curr
}

// Next moves iterator to the next item
func (it *Iterator) Next() {
	if it.deleted {
		it.deleted = false
		return
	}
	if it.isReverseIter { // Do we just want to call Prev() here?
		return
	}

retry:
	it.valid = true
	next, deleted := it.curr.getNext(0)
	if deleted {
		// Current node is deleted. Unlink current node from the level
		// and make next node as current node.
		// If it fails, refresh the path buffer and obtain new current node.
		if it.s.helpDelete(0, it.prev, it.curr, next, &it.s.Stats) {
			it.curr = next
		} else {
			atomic.AddUint64(&it.s.Stats.readConflicts, 1)
			found := it.s.findPath(it.curr.Item(), it.cmp, it.buf, &it.s.Stats) != nil
			last := it.curr
			it.prev = it.buf.preds[0]
			it.curr = it.buf.succs[0]
			if found && last == it.curr {
				goto retry
			}
		}
	} else {
		it.prev = it.curr
		it.curr = next
	}

	it.count++
	if it.count%it.smrInterval == 0 {
		it.Refresh()
	}
}

func (s *Skiplist) NewPrevIterator(startLim, endLim unsafe.Pointer, cmp CompareFn, buf *ActionBuffer) *Iterator {
	bs := s.barrier.Acquire()
	itr := &Iterator{
		cmp:           cmp,
		s:             s,
		buf:           buf,
		smrInterval:   ^uint(0),
		isReverseIter: true,
		bs:            bs,
	}
	itr.s.findPath(endLim, cmp, buf, &itr.s.Stats)
	/*This makes it so that the value is loaded when we call
	NewPrevIterator(), We can avoid this by adding a sentinel node
	that would point back to the tail node??
	Or we disregard the node and consider it start inclusive, end exclusive??*/
	itr.curr = buf.preds[0]
	itr.valid = true
	return itr
}

func (it *Iterator) Prev() {
	if !it.isReverseIter { // Do we just want to call Next() here?
		return
	}
	it.valid = true
	anchor := int(atomic.LoadInt32(&it.s.level) - 1)
	if it.buf.preds[anchor] == it.curr {
		tempBuf := it.s.MakeBuf()
		it.s.findPath(it.buf.preds[anchor].Item(), it.cmp, tempBuf, &it.s.Stats)
		it.buf.preds[anchor] = tempBuf.preds[anchor]
		it.s.FreeBuf(tempBuf)
	} else {
		for i := 1; i < anchor; i++ {
			if it.buf.preds[i] != it.curr {
				anchor = i
				break
			}
		}
	}

	for i := anchor; i > 0; i-- {
		curr := it.buf.preds[i]
	levelsearch:
		for {
			nextNode, deleted := curr.getNext(i - 1)
			for deleted {
				/*Since we only have the effective Curr and not the prev,
				we may just want to call FindPath to this item with a dummy buffer
				and let it clean it up*/
				tempBuf := it.s.MakeBuf()
				it.s.findPath(curr.Item(), it.cmp, tempBuf, &it.s.Stats) // This would make it O(logn^2)?? (I think)
				// Keep predecessor at level i (must have tower >= i for getNext(i-1))
				it.buf.preds[i] = tempBuf.preds[i]
				it.s.FreeBuf(tempBuf)

				curr = it.buf.preds[i]
				nextNode, deleted = curr.getNext(i - 1)
			}
			if nextNode == it.curr {
				it.buf.preds[i-1] = curr
				break levelsearch
			}
			curr = nextNode
		}

		it.prev = it.curr
		it.curr = it.buf.preds[0]
	}
	it.count++
	if it.count%it.smrInterval == 0 {
		it.Refresh()
	}
}

// Close is a destructor
func (it *Iterator) Close() {
	if it.bs != nil {
		it.s.barrier.Release(it.bs)
	}
}

func (it *Iterator) SetRefreshInterval(interval int) {
	it.smrInterval = uint(interval)
}

func (it *Iterator) Refresh() {
	if it.Valid() {
		currBs := it.bs
		itm := it.Get()
		it.bs = it.s.barrier.Acquire()
		it.Seek(itm)
		it.s.barrier.Release(currBs)
	}
}

func (it *Iterator) Pause() {
	if it.bs != nil {
		it.s.barrier.Release(it.bs)
		it.bs = nil
	}
}

func (it *Iterator) Resume() {
	it.bs = it.s.barrier.Acquire()
}
