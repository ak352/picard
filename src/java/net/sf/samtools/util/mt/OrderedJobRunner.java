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
package net.sf.samtools.util.mt;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.lang.InterruptedException;

/**
    A JobRunner that returns finished jobs in the same order they came
    in as.  It is expected that one thread will be adding Jobs to the
    input queue, and another thread will be removing finished Jobs from
    the output queue.  Any other usage may result in deadlock or race
    conditions.
 */
public class OrderedJobRunner implements JobRunner {

    /**
     * The shared input queue for the Worker threads.
     */
    private final BlockingQueue<Job> input;

    /**
     * The output queue for enforcing the job ordering.
     */
    private final BlockingQueue<Job> output;

    /**
     * True if the queue is closed, false otherwise.
     */
    private transient boolean closed = false;

    /**
     * Returns true if the output queue is empty.
     */
    public boolean isEmpty() {
        return output.isEmpty();
    }

    /**
     * Constructs an OrderedJobRunner using the specified shared input queue,
     * and creates a separate output queue to enforce the Job ordering.  The
     * output queue is only used by this OrderedJobRunner, but the input queue
     * can be shared by many JobRunners.
     */
    public OrderedJobRunner(BlockingQueue<Job> input, int outputQueueSize) {
        this.input  = input;
        this.output = new ArrayBlockingQueue<Job>(outputQueueSize);
    }

    /**
     * Adds the Job to input and output queues, unless this OrderedJobRunner is closed.
     */
    public void add(Job job) throws InterruptedException {
        if (this.closed) return;
        // we expect output queue to be smaller, so add to it first
        output.put(job);
        input.put(job);
    }

    /**
       Retrieves the next non-cancelled Job from the output queue and waits for 
       the Job to be finished.  Returns null if this OrderedJobRunner is closed.

       @return the next finished Job from the output queue.
     */
    public Job getFinishedJob() throws InterruptedException {
        if (this.closed) return null;
        while (true) {
            // remove the head of the queue
            Job b = this.output.take();
            if (b.isCancelled()) continue;
            // wait for the block's data to be ready
            b.waitTillFinished();
            return b;
        }
    }

    /**
       Checks to see if the Job at the head of the output queue is finished
       or cancelled, and if so, removes it from the queue.  If the Job was
       cancelled, it is discarded and another another Job is taken from
       the queue. If no finished Job can be found, null is returned.

       <p> If this OrderedJobRunner is closed, null is returned without
       checking the queue.

       <p> Note that this method is not synchronized and race conditions
       will occur if it is called by multiple threads at the same time.
       If a race condition is detected, an Error will be thrown.

       @return the next finished Job from the output queue, or null.
     */
    public Job pollFinishedJob() throws InterruptedException {
        if (this.closed) return null;
        while (true) {
            Job b = this.output.peek();
            if (b == null || (!b.isCancelled() && !b.isFinished())) return null;
            // now we know what the Job is cancelled or finished, so
            // remove it from the head of the queue
            if (this.output.poll() != b)
                throw new Error("coding error: race condition detected");
            if (b.isCancelled()) continue; // try again
            // the job is finished, so return it
            return b;
        }
    }

    /**
     * Closes this OrderedJobRunner.  Jobs already queued will be cancelled.
     * When a cancelled Job is removed from the input queue by a Worker
     * thread, it is discarded and not executed.  Simmilarly, a
     * cancelled Job is discarded when it is removed from the output queue
     * by {@link getFinishedJob()} or {@link pollFinishedJob()}.
     */
    public void close() {
        closed = true;
    }

    /**
        Is this OrderedJobRunner closed?
    */
    public boolean isClosed() {
        return closed;
    }
}
