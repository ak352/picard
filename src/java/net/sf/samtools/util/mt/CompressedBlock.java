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
 * CompressedBlock is a Job that compresses or decompresses a block of BGZF data.
 * Actually, a CompressedBlock job just holds the data, but a Worker thread
 * does the actual work.  Why?  Because the Worker threads have a set of persistent Deflaters
 * and Inflaters that they use over and over again, without having to recreate them.
 * This saves a lot of time, because Java's memory allocation and garbage collection
 * are very slow. <p> Each buffer used in a CompressedBlock is obtained from
 * an {@link ObjectCache} and returned to it when the buffer is no longer needed.
 * This is much faster that creating a new buffer each time.
 */
public class CompressedBlock extends Job {
    /**
     * The byte buffer containing the compressed or uncompressed block of data.
     */
    public byte[] buffer;

    /**
     * The number of "live" bytes in the buffer.  This number may be less
     * than the actual size of the buffer.
     */
    public int blockLength = 0;

    /**
     * The current byte offset into the block.
     */
    public int blockOffset = 0;

    /**
     * The prior length of this block before compression or decompression.
     */
    int priorLength = 0;

    /**
     * The CRC for this block.
     */
    long crc;

    boolean checkCrcs = false;

    /**
     * The block address in the input or output file.
     */
    public long blockAddress = 0;

    /**
     * The latch to signal that this block's address is ready.
     */
    private final CountDownLatch blockAddressLatch = new CountDownLatch(1); // is the block address ready?

    /**
     * The compression level.
     */
    int compressLevel;

    /**
     * True if this block is to be compressed, false otherwise.
     */
    boolean compress;

    /**
     * Creates a new block that goes from uncompressed to compressed.
     * @param compressLevel the desired compression level
     */
    public CompressedBlock(JobRunner r, AtomicReference<Throwable> ex, int compressLevel) {
        super(r, ex);
        this.buffer = MultiThreading.getBgzfBuffer();
        this.blockOffset = 0;
        this.blockLength = 0;
        this.blockAddress = 0;
        this.compress = true;
        this.compressLevel = compressLevel;
    }

    /**
     * Creates a new block that goes from compressed to uncompressed.
     */
    public CompressedBlock(JobRunner r, AtomicReference<Throwable> ex) {
        super(r, ex);
        this.buffer = MultiThreading.getBgzfBuffer();
        this.blockOffset = 0;
        this.blockLength = 0;
        this.blockAddress = 0;
        this.compress = false;
        this.compressLevel = -1;
    }

    /**
     * @return the block address, blocking if the address has not been set.
     */
    public long getBlockAddress() {
        try {
            this.blockAddressLatch.await();
        }
        catch (InterruptedException e) {
            throw new RuntimeException("BlockCompressed.getBlockAddress() was interrupted");
        }
        return this.blockAddress;
    }

    /**
     * Sets the block address.
     * @param blockAddress the block address.
     */
    public void setBlockAddress(long blockAddress) {
        this.blockAddress = blockAddress;
        this.blockAddressLatch.countDown();
    }
}
