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

public class BlockCompressed implements Comparable<BlockCompressed> {
    public byte[] buffer;
    public int blockLength = 0;
    public int blockOffset = 0;
    public int priorLength = 0;
    public long crc;
    public long id = -1;

    private long blockAddress = 0;
    private final CountDownLatch blockAddressLatch = new CountDownLatch(1); // is the block address ready?

    // TODO: set these!
    // Must be set by creater etc.
    public int origin = -1; 
    public int compressLevel;
    public boolean compress;

    public final int MAX_BLOCK_SIZE = 64 * 1024;
    
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
    
    public BlockCompressed(int origin, int compressLevel, boolean initBlockLevel)
    {
        init(initBlockLevel);
        this.origin = origin;
        this.compress = true;
        this.compressLevel = compressLevel;
    }
    
    public BlockCompressed(int origin, int compressLevel)
    {
        this(origin, compressLevel, false);
    }

    public BlockCompressed(int origin, boolean initBlockLevel)
    {
        init(initBlockLevel);
        this.origin = origin;
        this.compress = false;
        this.compressLevel = -1;
    }

    public BlockCompressed(int origin)
    {
        this(origin, false); 
    }

    public int compareTo(BlockCompressed o) {
        if(this.id < o.id) return -1;
        else if(this.id == o.id) return 0;
        else return 1;
    }

    public long getBlockAddress() {
        try {
            this.blockAddressLatch.await();
        }
        catch (InterruptedException e) {
            throw new RuntimeException("BlockCompressed.getBlockAddress() was interrupted");
        }
        return this.blockAddress;
    }

    public void setBlockAddress(long blockAddress) {
        this.blockAddress = blockAddress;
        this.blockAddressLatch.countDown();
    }
}
