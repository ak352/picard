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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
   A Job is a user-defined task that can be executed by a Worker thread.
   This base class is meant to be extended, because it only handles the
   administration of the job but does no real work.  Subclasses should contain data
   and/or methods that a Worker thread can use to perform the required task.
   Currently the Worker class only knows how to execute these subclasses of
   Job: {@link CompressedBlock}, {@link GroupJob}, and {@link RunnableJob}.
   Subclasses of Job that are unknown to the Worker class will cause an Error
   to be thrown.
 */
public class Job {

    // used to pass exceptions up to the parent thread
    private final AtomicReference<Throwable> ex;

    // used to check for cancellation
    private final JobRunner jobRunner;

    // latch used to wait for the job to be finished
    private CountDownLatch finished = new CountDownLatch(1); // 1 means not finished

    public boolean isCancelled() {
        return jobRunner.isClosed();
    }

    public void setThrowable(Throwable throwable) {
        ex.compareAndSet(null, throwable);
    }

    public Job(JobRunner r, AtomicReference<Throwable> ex) {
        this.ex = ex;
        this.jobRunner = r;
    }

    /**
     * Is the Job finished?
     */
    public boolean isFinished() throws InterruptedException {
        return (finished.getCount() == 0);
    }

    /**
     * Waits until the Job has finished.
     */
    public void waitTillFinished() throws InterruptedException {
        finished.await();
    }

    /**
     * Sets the jobs's finished state
     */
    public void setFinished(boolean yes) {
        if (yes) {
            finished.countDown();
        }
        else if (finished.getCount() == 0) {
            finished = new CountDownLatch(1);
        }
    }
}
