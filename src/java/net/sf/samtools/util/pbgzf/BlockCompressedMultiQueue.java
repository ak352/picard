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
 * The multi-stream block queue, to support a fixed number of consumer threads and an arbitrary number of input 
 * and output streams.  The main idea is to use the Singleton pattern to have only one instance of this queue
 * globally.  A given IO stream registers with this queue, and has underlying input and outpu queues from which
 * blocks are added and retrieved.  The consumer threads process blocks in these underlying queues. When an IO
 * stream is finished, it deregisters from the queue. 
 *
 * This queue guarantees that the order in which blocks are added is the same order in which blocks are
 * returned.  The queue also blocks when specified.
 */
public class BlockCompressedMultiQueue implements BlockCompressedBlockingQueue, BlockCompressedConsumerQueue {
    /**
     * The default queue size for each stream's queue.
     */
    private int QUEUE_SIZE = 100;

    /**
     * The default number of milliseconds to sleep.
     */
    private static final long THREAD_SLEEP = 100;

    /**
     * The instance of this class.
     */
    private static BlockCompressedMultiQueue instance = null;

    /**
     * The input queues to the consumers, one per registered stream.
     */
    private List<BlockCompressedPriorityBlockingQueue> input = null;
    
    /**
     * The output queues from the consumers, one per registered stream.
     */
    private List<BlockCompressedPriorityBlockingQueue> output = null;

    /**
     * The number of out-standing blocks possessed by the consumers.
     */
    private List<Integer> numOutstanding = null;

    /**
     * The consumer threads for compressing and decompressing blocks.
     */
    private List<BlockCompressedConsumer> consumers = null;

    /**
     * The lock on this queue.
     */
    final private Lock lock = new ReentrantLock();

    /**
     * The number of consumers.
     */
    private int numConsumers = 1;

    /**
     * The index of the last registered stream to yield blocks to a consumer.
     */
    private int lastI = 0;
    
    /**
     * Sleep.
     */
    private void sleep()
    {
        this.sleep(THREAD_SLEEP);
    }
    
    /**
     * Sleep for the given length of time.
     * @param l the length of time.
     */
    private void sleep(long l)
    {
        try {
            Thread.currentThread().sleep(l);
        } catch(InterruptedException ie) {
            // do nothing
        }
    }

    /**
     * Constructor for the Singleton pattern.
     */
    protected BlockCompressedMultiQueue() {
        int i;
        this.input = new ArrayList<BlockCompressedPriorityBlockingQueue>();
        this.output = new ArrayList<BlockCompressedPriorityBlockingQueue>();
        this.numOutstanding = new ArrayList<Integer>();

        this.numConsumers = Defaults.NUM_PBGZF_THREADS;
        this.QUEUE_SIZE = Defaults.PBGZF_QUEUE_SIZE;
        this.consumers = new ArrayList<BlockCompressedConsumer>();
        for(i=0;i<this.numConsumers;i++) {
            this.consumers.add(new BlockCompressedConsumer(this, i));
        }
        // start
        for(i=0;i<this.numConsumers;i++) {
            this.consumers.get(i).start();
        }
    }

    /**
     * Gets the instance of this queue.
     */
    public static synchronized BlockCompressedMultiQueue getInstance() {
        if(null == instance) {
            instance = new BlockCompressedMultiQueue();
        }
        return instance;
    }

	/**
	 * Gets the ID of the stream relative to the queue.
	 * @return the id of this stream in the queue.
	 */
    public int register() { // register with this queue
        int n = -1;
        this.lock.lock();
        // add a new slot
        n = this.input.size();
        this.input.add(new BlockCompressedPriorityBlockingQueue(false));
        this.output.add(new BlockCompressedPriorityBlockingQueue(true));
        this.numOutstanding.add(new Integer(0));
        this.lock.unlock();
        return n;
    }

	/**
	 * Disassociates this stream with the queue.
	 * @param i the id of this stream returned by register.
	 */
    public void deregister(int i) {
        // get the associated queues
        this.lock.lock();
        BlockCompressedPriorityBlockingQueue in = this.input.get(i);
        BlockCompressedPriorityBlockingQueue out = this.output.get(i);
        if(null != in && null != out) {
            // clear the queues
            in.clear(); 
            out.clear();
            // wait for all outstanding blocks to be returned
            while(0 < this.numOutstanding.get(i)) {
                // TODO: use wait/notifyAll
                this.lock.unlock();
                this.sleep();
                this.lock.lock();
            }
            // clear the queues
            in.clear(); 
            out.clear();
            // nullify
            this.input.set(i, null);
            this.output.set(i, null);
            this.numOutstanding.set(i, -1);
        }
        this.lock.unlock();
    }

    /**
     * Gets a block from the queue, waiting if specified.
     * @param wait true to wait until a block is available, false otherwise.
     * @return null if unsuccesful, otherwise a block.
     */
    public BlockCompressed get(boolean wait) {
        int i, start;
        BlockCompressed block = null;
        start = i = this.lastI;
        while(true) { // while we have not received any
            if(0 < this.input.size()) { // do queues exist
                BlockCompressedPriorityBlockingQueue in = null;
                // get the input queue
                try {
                    in = this.input.get(i);
                } catch(IndexOutOfBoundsException e) {
                    // ignore
                }
                if(null != in) {
                    // get an item
                    block = in.poll(); 
                    // check success
                    if(null != block) {
                        break; // success!
                    }
                }
                // move to the next queue
                i++;
                if(this.input.size() <= i) i = 0;
            }
            if(start == i) { // we gone through all of the queues
                if(!wait) break; // do not wait
                // give a chance for others to add
                // TODO: use wait/notifyAll
                this.sleep();
            }
        }
        this.lastI = i;
        return block;
    }

    /**
     * Gets a collection of blocks from the queue, waiting if specified.
     * @param c the collection in which to add the blocks.
     * @param maxElements the maximum number of elements to add.
     * @param wait true to wait until a block is available, false otherwise.
     * @return the number of blocks added.
     */
    public int drainTo(Collection<BlockCompressed> c, int maxElements, boolean wait)
    {
        int i, start, num = 0;
        start = i = this.lastI;
        while(true) { // while we have not received any
            if(0 < this.input.size()) { // do queues exist
                BlockCompressedPriorityBlockingQueue in = null;
                // get the input queue
                try {
                    in = this.input.get(i);
                } catch(IndexOutOfBoundsException e) {
                    // ignore
                }
                if(null != in) {
                    // get items
                    num = in.drainTo(c, maxElements);
                    // check success
                    if(0 < num) {
                        break; // success!
                    }
                }
                // move to the next queue
                i++;
                if(this.input.size() <= i) i = 0;
            }
            if(start == i) { // we gone through all of the queues
                if(!wait) break; // do not wait
                // give a chance for others to add
                // TODO: use wait/notifyAll
                this.sleep();
            }
        }
        this.lastI = i;
        return num;
    }

    /**
     * Adds a block to the queue, waiting if specified.
     * @param wait true to wait until the block can be added, false otherwise.
     * @return true if successful, false otherwise.
     */
    public boolean add(BlockCompressed block, boolean wait) {
        boolean r = false;
        BlockCompressedPriorityBlockingQueue out = null;
        try {
            out = this.output.get(block.origin);
        } catch(IndexOutOfBoundsException e) {
            out = null;
        }
        if(null != out) {
            if(wait) r = out.offer(block); // this will always return true
            else r = out.offer(block, 0, TimeUnit.MILLISECONDS); // do not wait
            if(r) { // update the number outstanding
                this.lock.lock();
                this.numOutstanding.set(block.origin, this.numOutstanding.get(block.origin)-1);
                this.lock.unlock();
            }
        }
        return r;
    }

	/**
	 * Adds a block of data to the queue for compression/decompression. The
	 * ID of the block will be set here.
	 * @param block the block to add.
	 * @return true if successful, false otherwise.
	 */
    public boolean add(BlockCompressed block) 
        throws InterruptedException
    {
        BlockCompressedPriorityBlockingQueue in = null;
        // lock
        //this.lock.lock();
        // get the queue
        try {
            in = this.input.get(block.origin);
        } catch(IndexOutOfBoundsException e) {
            in = null;
        }
        // unlock
        //this.lock.unlock();
        // add the block
        if(null != in) {
            return in.offer(block);
        }
        return false;
    }

	/**
	 * Returns the next block from the queue, in the same order it was added.
	 * @param i the id of this stream.
	 * @return the block.
	 */
    public BlockCompressed get(int i) 
        throws InterruptedException
    {
        BlockCompressedPriorityBlockingQueue out = null;
        // get the queue
        //this.lock.lock();
        try {
            out = this.output.get(i);
        } catch(IndexOutOfBoundsException e) {
            out = null;
        }
        //this.lock.unlock();
        // get the block
        if(null != out) {
            return out.take();
        }
        return null;
    }

	/**
	 * Waits until the queue for this stream is empty.
	 * @param i the id of this stream.
	 * @param l the length of time to sleep if the queue is not empty.
	 */
    public void waitUntilEmpty(int i, int l) 
        throws InterruptedException
    {
        BlockCompressedPriorityBlockingQueue in = null;
        BlockCompressedPriorityBlockingQueue out = null;
        int n = 0;
        // get the queues
        this.lock.lock();
        in = this.input.get(i);
        out = this.output.get(i);
        n = this.numOutstanding.get(i);
        this.lock.unlock();
        // wait until both are empty
        // TODO: do these need to be synchronized?
        while(0 < n || 0 != in.size() || 0 != out.size()) {
            // TODO: use wait/notifyAll
            this.sleep(l);
            // get the new # of outstanding
            this.lock.lock();
            n = this.numOutstanding.get(i);
            this.lock.unlock();
        }
    }
    
	/**
	 * Waits until the queue for this stream is empty.
	 * @param i the id of this stream.
	 */
    public void waitUntilEmpty(int i) 
        throws InterruptedException
    {
        waitUntilEmpty(i, 100); //sleep for 100 ms
    }
}
