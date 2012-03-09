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

import net.sf.samtools.Defaults;

// TODO: bound the queue sizes...
// TODO: register/deregister collisions if the consumers have a block checked out...
// add - add to the queue (input queue) 
// get - remove an element such that there eixsts a spot in the output queue
// remove - get from the queue (output queue)
// register - register a new reader/writer
public class BlockCompressedMultiQueue implements BlockCompressedBlockingQueue, BlockCompressedConsumerQueue {
    private int QUEUE_SIZE = 100;
    private static final long THREAD_SLEEP = 100;

    private static BlockCompressedMultiQueue instance = null;

    private List<BlockCompressedPriorityBlockingQueue> input = null;
    private List<BlockCompressedPriorityBlockingQueue> output = null;
    private List<Integer> numOutstanding = null;
    private LinkedList<Integer> reuseRegisterList = null;

    private List<BlockCompressedConsumer> consumers = null;

    final private Lock lock = new ReentrantLock();

    private int numConsumers = 1;
    private int lastI = 0;
    
    private void sleep()
    {
        this.sleep(THREAD_SLEEP);
    }
    
    private void sleep(long l)
    {
        try {
            Thread.currentThread().sleep(l);
        } catch(InterruptedException ie) {
            // do nothing
        }
    }

    // NB: Singleton pattern
    protected BlockCompressedMultiQueue() {
        int i;
        this.input = new ArrayList<BlockCompressedPriorityBlockingQueue>();
        this.output = new ArrayList<BlockCompressedPriorityBlockingQueue>();
        this.numOutstanding = new ArrayList<Integer>();
        this.reuseRegisterList = new LinkedList<Integer>();

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

    public static synchronized BlockCompressedMultiQueue getInstance() {
        if(null == instance) {
            instance = new BlockCompressedMultiQueue();
        }
        return instance;
    }

    public int register() { // register with this queue
        int n = -1;
        this.lock.lock();
        if(0 < this.reuseRegisterList.size()) { // re-use a slot
            n = this.reuseRegisterList.poll(); // get the first element 
            this.input.set(n, new BlockCompressedPriorityBlockingQueue(false));
            this.output.set(n, new BlockCompressedPriorityBlockingQueue(true));
            this.numOutstanding.set(n, new Integer(0));
        }
        else { // add a new slot
            n = this.input.size();
            this.input.add(new BlockCompressedPriorityBlockingQueue(false));
            this.output.add(new BlockCompressedPriorityBlockingQueue(true));
            this.numOutstanding.add(new Integer(0));
        }
        this.lock.unlock();
        //System.err.println("Registered id=" + n);
        return n;
    }

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
            this.reuseRegisterList.add(i); // we could re-use later
            this.numOutstanding.set(i, -1);
        }
        this.lock.unlock();
        //System.err.println("Deregistered id=" + i);
    }

    // consumers
    public BlockCompressed get(boolean wait) {
        int i, start;
        BlockCompressed block = null;
        //System.err.println("in conumer get 0 [" + this.lastI + "]");
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
                    //System.err.println("in conumer get 1 [" + i + "] size=" + in.size());
                    block = in.poll(); 
                    //System.err.println("in conumer get 2 [" + i + "] size=" + in.size());
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
        //System.err.println("in conumer get 3 [" + i + "]");
        this.lastI = i;
        return block;
    }

    // consumers
    public boolean add(BlockCompressed block, boolean wait) {
        boolean r = false;
        BlockCompressedPriorityBlockingQueue out = null;
        try {
            out = this.output.get(block.origin);
        } catch(IndexOutOfBoundsException e) {
            out = null;
        }
        if(null != out) {
            //System.err.println("in conumer add 1 [" + block.origin + "] size=" + out.size());
            if(wait) r = out.offer(block); // this will always return true
            else r = out.offer(block, 0, TimeUnit.MILLISECONDS); // do not wait
            //System.err.println("in conumer add 2 [" + block.origin + "] size=" + out.size());
            if(r) { // update the number outstanding
                this.lock.lock();
                this.numOutstanding.set(block.origin, this.numOutstanding.get(block.origin)-1);
                this.lock.unlock();
            }
        }
        return r;
    }

    // reader/writers
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

    // reader/writers
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
            //System.err.println("Waiting... [" + in.size() + "] [" + out.size() + "] [" + n + "]");
            this.sleep(l);
            // get the new # of outstanding
            this.lock.lock();
            n = this.numOutstanding.get(i);
            this.lock.unlock();
        }
    }
    
    public void waitUntilEmpty(int i) 
        throws InterruptedException
    {
        waitUntilEmpty(i, 100); //sleep for 100 ms
    }
}
