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
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABlE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.sf.samtools.util.pbgzf;

import java.util.List;
import java.util.ArrayList;

public class BlockCompressedPool {
    public int head = 0;
    public int n = 0;
    public int m = 0;
    List<BlockCompressed> blocks = null;

    public final int MAX_BlOCK_SIZE = 64 * 102;
    public static final int DEFAULT_SIZE = 100;
    
    public BlockCompressedPool(boolean fill)
    {
        this(DEFAULT_SIZE, fill);
    }

    public BlockCompressedPool(int m, boolean fill)
    {
        int i;
        this.m = m;
        this.blocks = new ArrayList<BlockCompressed>();
        if(fill) {
            for(i=0;i<this.m;i++) {
                this.blocks.add(new BlockCompressed(-1));
            }
        }
        else {
            for(i=0;i<this.m;i++) {
                this.blocks.add(null);
            }
        }
    }

    private void shift()
    {
        int i;
        if(0 == this.head) return;
        for(i=0;i<this.n;i++) {
            this.blocks.set(i, blocks.get(i + this.head));
            this.blocks.set(i + this.head, null);
        }
        this.head = 0;
    }

    public synchronized boolean add(BlockCompressed block)
    {
        if(null == block) return false;
        if(this.n < this.m) {
            if(this.head + this.n == this.m) {
                this.shift();
            }
            this.blocks.set(this.head + this.n, block);
            this.n++;
        }
        else { // ignore
            // the block is destroyed here
        }
        return true;
    }

    public synchronized BlockCompressed get()
    {
        BlockCompressed b = null;
        if(0 < this.n) {
            b = this.blocks.get(this.head);
            this.blocks.set(this.head, null);
            this.n--;
            this.head++;
            if(this.head == this.m) this.head = 0; // NB: this.n == 0
        }
        return b;
    }

    public synchronized BlockCompressed peek()
    {
        BlockCompressed b = null;
        if(0 < this.n) {
            b = this.blocks.get(this.head);
        }
        return b;
    }
}
