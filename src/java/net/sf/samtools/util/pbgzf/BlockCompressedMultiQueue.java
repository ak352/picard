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
public class BlockCompressedMultiQueue {
    /**
     * The default queue size for each stream's queue.
     */
    private int default_queue_size = 100;

    /**
     * The default number of milliseconds to sleep.
     */
    private static final long THREAD_SLEEP = 100;

    /**
     * The instance of this class.
     */
    private static BlockCompressedMultiQueue instance = null;

    /**
     * The queues for the consumers, one per registered stream.
     */
    private List<BlockCompressedQueue> queues = null;

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
        this.queues = new LinkedList<BlockCompressedQueue>();
        this.numConsumers = Defaults.NUM_PBGZF_THREADS;
        this.default_queue_size = Defaults.PBGZF_QUEUE_SIZE;
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
     * @return the queue to associate with this stream.
     */
    public BlockCompressedQueue register() { // register with this queue
        BlockCompressedQueue queue = null;

        queue = new BlockCompressedQueue(this.default_queue_size);
        
        // add a new slot
        this.lock.lock();
        this.queues.add(queue);
        this.lock.unlock();

        return queue;
    }

    /**
     * Disassociates this stream with the queue and closes the underlying queue.
     * @param i the id of this stream returned by register.
     * @param true if successful, false otherwise.
     */
    public boolean deregister(BlockCompressedQueue queue) 
    {
        // get the associated queues
        this.lock.lock();
        try {
            // close the queue
            queue.close();

            // remove the queue
            return this.queues.remove(queue);
        } catch (InterruptedException e) {
            return false;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Gets a block from the queue, waiting if specified.
     * @return null if unsuccesful, otherwise a block.
     */
    public BlockCompressed get() {
        int i, start;
        BlockCompressed block = null;
        start = i = this.lastI;
        while(true) { // while we have not received any
            if(0 < this.queues.size()) { // do queues exist?
                BlockCompressedQueue queue = null;
                    
                // NB: how else can we detect if the "queues" were modified?
                // We would need a lock!
                try {
                    queue = this.queues.get(i);

                    // get an item
                    block = queue.getInput(false); // do not wait

                    // check success
                    if(null != block) {
                        break; // success!
                    }
                } catch(IndexOutOfBoundsException e) {
                    // ignore
                } catch(InterruptedException e) {
                    // ignore
                }

                // move to the next queue
                i++;
                if(this.queues.size() <= i) i = 0;
            }
            if(start == i) { // we gone through all of the queues
                // give a chance for others to add
                // TODO: use wait/notifyAll
                this.sleep();
            }
        }
        this.lastI = i;
        return block;
    }
}
