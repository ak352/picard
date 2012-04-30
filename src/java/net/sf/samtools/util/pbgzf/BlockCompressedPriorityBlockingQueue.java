/*
 * The MIT License
 *
 * Copyright (c) 2009 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.sf.samtools.util.pbgzf;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.util.Collection;

// TODO: enforce size...

// NB: an ordered queue will not set the id of a block, and unordered queue will set the id of a block
public class BlockCompressedPriorityBlockingQueue {
    private PriorityBlockingQueue<BlockCompressed> queue = null;
    private long id;
    private boolean ordered = false;
    final private Lock lock = new ReentrantLock();
    final private Condition newElement = lock.newCondition();

    public BlockCompressedPriorityBlockingQueue() {
        this(false);
    }

    public BlockCompressedPriorityBlockingQueue(boolean ordered)
    {
        this.queue = new PriorityBlockingQueue<BlockCompressed>();
        this.id = 0;
        this.ordered = ordered;
    }

    // non-blocking
    public boolean add(BlockCompressed o) {
        boolean b = false;
        synchronized (this) {
            if(!this.ordered && -1 == o.id) {
                o.id = this.id;
                this.id++;
            }
            b = this.queue.add(o);
            this.newElement.signalAll(); // signal all
        }
        return b;
    }

    // non-blocking
    public boolean offer(BlockCompressed o) {
        boolean b = false;
        this.lock.lock();
        if(!this.ordered && -1 == o.id) {
            o.id = this.id;
            this.id++;
        }
        b = this.queue.offer(o);
        this.newElement.signalAll(); // signal all
        this.lock.unlock();
        return b;
    }

    // non-blocking
    public boolean offer(BlockCompressed o, long timeout, TimeUnit unit) {
        boolean b = false;
        this.lock.lock();
        if(!this.ordered && -1 == o.id) {
            o.id = this.id;
            this.id++;
        }
        b = this.queue.offer(o, timeout, unit);
        this.newElement.signalAll(); // signal all
        this.lock.unlock();
        return b;
    }

    // non-blocking
    public BlockCompressed poll() {
        BlockCompressed block = null;
        if(!this.ordered) { // unordered
            return this.queue.poll();
        }
        // ordered queue
        this.lock.lock();
        block = this.queue.peek();
        if(null != block && this.id == block.id) { // enforce ordering
            block = this.queue.poll();
            this.id++; // move to the next id we expect
        }
        else {
            block = null;
        }
        this.lock.unlock();
        return block;
    }

    // blocking
    public BlockCompressed take() 
        // TODO
        throws InterruptedException
    {
        BlockCompressed block = null;
        while(true) {
            // poll without a lock
            block = this.poll();
            if(null != block) break;
            // wait for a new element to be added
            this.lock.lock();
            this.newElement.await(); // wait for a new object to be added
            block = this.poll();
            this.lock.unlock();
            if(null != block) break;
        }
        return block;
    }

    // non-blocking
    public void clear() {
        this.queue.clear();
    }

    // non-blocking
    public int size() {
        return this.queue.size();
    }
}
