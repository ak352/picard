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

import java.util.concurrent.CountDownLatch;

/**
 * Stores a compressed or uncompressed block of data.
 */
public class BlockCompressed implements Comparable<BlockCompressed> {
    /**
     * The byte buffer of compressed or uncompressed data.
     */
    public byte[] buffer;

    /**
     * The number of bytes in the block of data.
     */
    public int blockLength = 0;

    /**
     * The current byte offset into the block, used by a consumer/reader of this data.
     */
    public int blockOffset = 0;

    /**
     * The prior length of this block before compression/decompression.
     */
    public int priorLength = 0;

    /**
     * The CRC for this block.
     */
    public long crc;

    /**
     * The ID of this block, to order it from the input or to the output stream.
     */
    public long id = -1;

    /**
     * The block address in the input or output file.
     */
    private long blockAddress = 0;

    /**
     * The latch to signal when the block address is ready upon writing this block by the writer.
     */
    private final CountDownLatch blockAddressLatch = new CountDownLatch(1); // is the block address ready?

    // TODO: set these!
    // Must be set by creater etc.
    /**
     * The origin id of this block, usually the index into the input/output queues.
     */
    public int origin = -1; 

    /**
     * The compression level.
     */
    public int compressLevel;

    /**
     * True if this block is to be compressed, false otherwise.
     */
    public boolean compress;

    /**
     * The maximum block size.
     */
    public final int MAX_BLOCK_SIZE = 64 * 1024;
    
    /**
     * Initializes a block.
     * @param initBlockLength the initial block length.
     */
    private void init(boolean initBlockLength)
    {
        this.buffer = new byte[MAX_BLOCK_SIZE];
        this.blockOffset = 0;
        if(initBlockLength) {
            this.blockLength = buffer.length;
        }
        else {
            this.blockLength = 0;
        }
        this.blockAddress = 0;
        this.id = -1;
    }
    
    /**
     * Creates a new block.
     * @param origin the origin id of this block.
     * @param compressLevel the compression level of this block
     * @param initBlockLength the initial block length.
     */
    public BlockCompressed(int origin, int compressLevel, boolean initBlockLevel)
    {
        init(initBlockLevel);
        this.origin = origin;
        this.compress = true;
        this.compressLevel = compressLevel;
    }
    
    /**
     * Creates a new block.
     * @param origin the origin id of this block.
     * @param compressLevel the compression level of this block
     */
    public BlockCompressed(int origin, int compressLevel)
    {
        this(origin, compressLevel, false);
    }

    /**
     * Creates a new block.
     * @param origin the origin id of this block.
     * @param initBlockLength the initial block length.
     */
    public BlockCompressed(int origin, boolean initBlockLevel)
    {
        init(initBlockLevel);
        this.origin = origin;
        this.compress = false;
        this.compressLevel = -1;
    }

    /**
     * Creates a new block.
     * @param origin the origin id of this block.
     */
    public BlockCompressed(int origin)
    {
        this(origin, false); 
    }

    /**
     * Compares the ID of the given block to this block.
     * @param o the block to compare.
     * @return -1, if this.id < o.id, 0 if (this.id == o.id), 1 otherwise.
     */
    public int compareTo(BlockCompressed o) {
        if(this.id < o.id) return -1;
        else if(this.id == o.id) return 0;
        else return 1;
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

    /**
     * Compares the block IDs for equality.
     * @param o the block to compare.
     * @return true if the IDs are equal, false otherwise.
     */
    public boolean equals(BlockCompressed o) {
        return (0 == this.compareTo(o));
    }
}
