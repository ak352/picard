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

/**
 * Interface into the underlying multi-stream blocking queue for the
 * ParallelBlockCompressedInputStream and ParallelBlockCompressedOutputStream
 * classes.
 */
public interface BlockCompressedBlockingQueue {

	/**
	 * Gets the ID of the stream relative to the queue.
	 * @return the id of this stream in the queue.
	 */
    public int register();

	/**
	 * Disassociates this stream with the queue.
	 * @param i the id of this stream returned by register.
	 */
    public void deregister(int i);

	/**
	 * Adds a block of data to the queue for compression/decompression. The
	 * ID of the block will be set here.
	 * @param block the block to add.
	 * @return true if successful, false otherwise.
	 */
    public boolean add(BlockCompressed block) 
        throws InterruptedException;

	/**
	 * Returns the next block from the queue, in the same order it was added.
	 * @param i the id of this stream.
	 * @return the block.
	 */
    public BlockCompressed get(int i) 
        throws InterruptedException;

	/**
	 * Waits until the queue for this stream is empty.
	 * @param i the id of this stream.
	 * @param l the length of time to sleep if the queue is not empty.
	 */
    public void waitUntilEmpty(int i, int l) 
        throws InterruptedException;
    
	/**
	 * Waits until the queue for this stream is empty.
	 * @param i the id of this stream.
	 */
    public void waitUntilEmpty(int i) 
        throws InterruptedException;
}
