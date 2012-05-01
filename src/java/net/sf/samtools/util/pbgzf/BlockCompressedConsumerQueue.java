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

/**
 * The interface into the multi-stream block queue for the consumer.
 */
public interface BlockCompressedConsumerQueue {

    /**
     * Gets a block from the queue, waiting if specified.
     * @param wait true to wait until a block is available, false otherwise.
     * @return null if unsuccesful, otherwise a block.
     */
    public BlockCompressed get(boolean wait);

    /**
     * Adds a block to the queue, waiting if specified.
     * @param wait true to wait until the block can be added, false otherwise.
     * @return true if successful, false otherwise.
     */
    public boolean add(BlockCompressed block, boolean wait);

    /**
     * Gets a collection of blocks from the queue, waiting if specified.
     * @param c the collection in which to add the blocks.
     * @param maxElements the maximum number of elements to add.
     * @param wait true to wait until a block is available, false otherwise.
     * @return the number of blocks added.
     */
    public int drainTo(Collection<BlockCompressed> c, int maxElements, boolean wait);
}
