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
import java.util.Collection;
import java.lang.Thread;
import java.lang.InterruptedException;

import net.sf.samtools.Defaults;

/**
 * TODO
 */
public class BlockCompressedQueue { 

    /**
     * The input queue to the consumers.
     */
    private List<BlockCompressed> input = null;

    /**
     * The output queues from the consumers, one per registered stream.
     */
    private List<BlockCompressed> output = null;

    /**
     * The maximum size of the queue.
     */
    private int maximumSize = 1000;

    /**
     * The lock on this queue.
     */
    private final Lock lock = new ReentrantLock();

    /**
     * The condition that the input queue is not full.
     */
    private final Condition notInputFull = lock.newCondition();

    /**
     * The condition that the output queue is not full.
     */
    private final Condition notOutputFull = lock.newCondition();

    /**
     * The condition that the queue is not empty.
     */
    private final Condition notEmpty = lock.newCondition();

    /**
     * True if the queue is closed, false otherwise.
     */
    private boolean closed = false;

    /**
     * Initializes a new queue.
     */
    public BlockCompressedQueue(int maximumSize) {
        this();
        this.maximumSize = maximumSize;
    }

    /**
     * Initializes a new queue.
     */
    public BlockCompressedQueue() {
        this.input = new LinkedList<BlockCompressed>();
        this.output = new LinkedList<BlockCompressed>();
    }

    /**
     * Adds the block to the queue for consuming.
     */
    public boolean add(BlockCompressed block) 
        throws InterruptedException
    {
        // NB: should we reset the block's countdown latch?

        this.lock.lock();
        try {
            // check that we do not add more than we can handle...
            while(!this.closed) {
                if(this.maximumSize <= this.input.size()) {
                    this.notInputFull.await();
                } else if(this.maximumSize <= this.output.size()) {
                    this.notOutputFull.await();
                }
                else {
                    break;
                }
            }
            if(!this.closed) {
                this.input.add(block);
                this.output.add(block);
                this.notEmpty.signal();
            }
        } finally {
            this.lock.unlock();
        }
        return true;
    }

    /**
     * Returns a block element from the input queue.
     * @param wait true if we are to wait for an available block, false otherwise.
     * @return a block, or null if none was available.
     */
    public BlockCompressed getInput(boolean wait) 
        throws InterruptedException
    {
        BlockCompressed b = null;
        this.lock.lock();
        try {
            while(!this.closed && 0 == this.input.size()) {
                if(!wait) {
                    return null;
                }
                this.notEmpty.await();
            }
            if(!this.closed) {
                // get the first element of the list
                b = this.input.remove(0);
                this.notInputFull.signal();
            }
        } finally {
            this.lock.unlock();
        }
        return b;
    }
    
    /**
     * Returns a block element from the output queue and waits for 
     * the block to be consumed if specified.
     * @param waitForConsumed wait for the block to be consumed.
     * @return the block from the output queue.
     */
    public BlockCompressed getOutput(boolean waitForConsumed) 
        throws InterruptedException
    {
        BlockCompressed b = null;
        this.lock.lock();
        try {
            while(!this.closed && 0 == this.output.size()) {
                this.notEmpty.await();
            }
            if(!this.closed) {
                // get the first element of the list
                b = this.output.remove(0);
                this.notOutputFull.signal();
            }
        } finally {
            this.lock.unlock();
        }
        // wait for the block to be consumed.
        if(null != b && waitForConsumed) {
            b.getConsumed();
        }
        return b;
    }

    /**
     * Returns a block element from the output queue and waits for 
     * the block to be consumed.
     * @return the block from the output queue.
     */
    public BlockCompressed getOutput() 
        throws InterruptedException
    {
        return this.getOutput(true);
    }

    /**
     * Closes the queue. All subsequent methods will be ignored.
     */
    public void close()
        throws InterruptedException
    {
        this.lock.lock();
        try {
            this.closed = true;
            this.notEmpty.signalAll();
            this.notOutputFull.signalAll();
            this.notInputFull.signalAll();
        } finally {
            this.lock.unlock();
        }
    }
    
    /**
     * Waits until the queue for this stream is empty.
     */
    public void waitUntilEmpty() 
        throws InterruptedException
    {
        this.lock.lock();
        try {
            while(!this.closed) {
                if(0 < this.input.size()) {
                    this.notInputFull.await();
                } else if(0 < this.output.size()) {
                    this.notOutputFull.await();
                }
                else {
                    break;
                }
            }
        } finally {
            this.lock.unlock();
        }
    }

}
