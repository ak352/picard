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

/**
 * A blocking queue prioritized by block ID.  This is used by the multi-stream block queue
 * for a single stream.
 *
 * If ordered, the queue preserves the order of the input blocks when returned.
 */
public class BlockCompressedPriorityBlockingQueue {
    /**
     * The underlying priority queue, prioritized by increasing block IDs.
     */
    private PriorityBlockingQueue<BlockCompressed> queue = null;

    /**
     * The next id to return or set if this queue is ordered.
     */
    private long id;

    /**
     * True if this queue is to maintain order, false otherwise.
     */
    private boolean ordered = false;

    /**
     * A lock on the queue.
     */
    final private Lock lock = new ReentrantLock();

    /**
     * A condition to signal a new element has been added to the queue.
     */
    final private Condition newElement = lock.newCondition();

    /**
     * Creates an unordered queue.
     */
    public BlockCompressedPriorityBlockingQueue() {
        this(false);
    }

    /**
     * Creates a queue based on the given ordering.
     * @param ordered true if the queue is to be ordered, false otherwise.
     */
    public BlockCompressedPriorityBlockingQueue(boolean ordered)
    {
        this.queue = new PriorityBlockingQueue<BlockCompressed>();
        this.id = 0;
        this.ordered = ordered;
    }

    /**
     * Adds a block to the queue, waiting if necessary.  If the queue is ordered,
     * the id of the block is set.
     * @param o the block to add.
     * @return true if successful, false otherwise.
     */
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

    /**
     * Adds a block to the queue, waiting if necessary.  If the queue is ordered,
     * the id of the block is set.
     * @param o the block to add.
     * @return true if successful, false otherwise.
     */
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

    /**
     * Adds a block to the queue, waiting if necessary.  If the queue is ordered,
     * the id of the block is set.
     * @param o the block to add.
     * @param timeout the length of time to wait.
     * @param unit the unit of time to wait.
     * @return true if successful, false otherwise.
     */
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

    /**
     * Retrieves and removes the head of this queue, or null if this queue is empty. This 
     * will block to preserve ordering.
     * @return the head of the queue.
     */
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

    /**
     * Retrieves and removes the head of this queue, waiting if no elements are present on this queue.
     * @return the head of the queue.
     */
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

    /**
     * Gets a collection of blocks from the queue, waiting if specified. This is only
     * implemented for unordered queues.
     * @param c the collection in which to add the blocks.
     * @param maxElements the maximum number of elements to add.
     * @return the number of blocks added.
     */
    public int drainTo(Collection<BlockCompressed> c, int maxElements) 
        throws Exception
    {
        BlockCompressed block = null;
        if(!this.ordered) { // unordered
            return this.queue.drainTo(c, maxElements);
        }
        else {
            throw new Exception("Not implemented");
        }
    }

    /**
     * Clears this queue.
     */
    public void clear() {
        this.queue.clear();
    }

    /**
     * Returns the number of elements in this collection.
     * @return the size.
     */
    public int size() {
        return this.queue.size();
    }
}
