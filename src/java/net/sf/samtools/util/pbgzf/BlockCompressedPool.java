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
import java.util.Collection;
import java.util.Iterator;

/**
 * A simple collection to store a pool of blocks for a consumer to buffer.
 */
public class BlockCompressedPool implements Collection<BlockCompressed> {
    /**
     * The head of the queue.
     */
    public int head = 0;

    /**
     * The number of blocks in the queue.
     */
    public int n = 0;

    /**
     * The maximum size of the queue.
     */
    public int m = 0;

    /**
     * The queue.
     */
    public List<BlockCompressed> blocks = null;

    /**
     * The default size of the queue.
     */
    public static final int DEFAULT_SIZE = 100;
    
    /**
     * Creates a block pool.
     * @param fill true if we are to initialize blocks in the queue, false otherwise.
     */
    public BlockCompressedPool(boolean fill)
    {
        this(DEFAULT_SIZE, fill);
    }

    /**
     * Creates a block pool.
     * @param m the initial size of the queue.
     * @param fill true if we are to initialize blocks in the queue, false otherwise.
     */
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

    /**
     * Shifts down the blocks.
     */
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

    /**
     * Adds the given block to the pool.
     */
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
    
    /**
     * Adds the collection of blocks to the pool.
     * @param c the collection of blocks.
     */
    public synchronized boolean addAll(Collection<? extends BlockCompressed> c)
    {
        for(BlockCompressed b : c) {
            if(!this.add(b)) {
                this.blocks.add(null);
                this.m++;
                this.add(b);
            }
        }
        c.clear();
        return true;
    }

    /**
     * Clears the pool.
     */
    public synchronized void clear()
    {
        this.n = 0;
        this.m = 0;
        this.head = 0;
        for(BlockCompressed b : this.blocks) {
            if(null != b) {
                b.id = -1;
            }
        }
    }
    
    public synchronized boolean contains(Object o) 
    {
        return this.blocks.contains(o);
    }

    public synchronized boolean contains(BlockCompressed b)
    {
        return this.blocks.contains(b);
    }

    public synchronized boolean containsAll(Collection<?> c)
    {
        return this.blocks.containsAll(c);
    }

    public synchronized boolean equals(BlockCompressedPool pool)
    {
        return this.blocks.equals(pool.blocks);
    }
    
    /**
     * Removes the head of the queue.
     * @return the head of the queue.
     */
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

    public synchronized int hashCode()
    {
        return this.blocks.hashCode();
    }

    public synchronized boolean isEmpty()
    {
        return (0 == this.n);
    }

    public synchronized Iterator<BlockCompressed> iterator()
    {
        return this.blocks.iterator();
    }

    public synchronized boolean remove(Object o)
    {
        return this.blocks.remove(o);
    }
    
    public synchronized boolean removeAll(Collection<?> c)
    {
        return this.blocks.removeAll(c);
    }
    
    public synchronized boolean retainAll(Collection<?> c)
    {
        return this.blocks.retainAll(c);
    }

    public synchronized int size()
    {
        return this.n;
    }

    public synchronized Object[] toArray()
    {
        return this.blocks.toArray();
    }

    public synchronized <BlockCompressed> BlockCompressed[] toArray(BlockCompressed[] a)
    {
        return this.blocks.toArray(a);
    }

    /**
     * Peeks at the head of the queue.
     * @return the head of the queue.
     */
    public synchronized BlockCompressed peek()
    {
        BlockCompressed b = null;
        if(0 < this.n) {
            b = this.blocks.get(this.head);
        }
        return b;
    }

}
