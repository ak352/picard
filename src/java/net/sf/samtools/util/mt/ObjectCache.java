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
import java.util.concurrent.LinkedBlockingQueue;

/**
 *  Keeps a cache of objects that can be reused.  Why?  Because using "new"
 *  to create large objects is very slow.  Therefore, once they are created,
 *  we reuse them.
*/
public class ObjectCache<E> {

    private final Creator<E> creator;
    private final BlockingQueue<E> queue;

    /**
        Creates an object cache of fixed or unlimited size.
    */
    public ObjectCache(Creator<E> c, int size) {
        if (c == null) throw new RuntimeException("Creator cannot be null");
        creator = c;
        if (size <= 0) { // unlimited size
            queue = new LinkedBlockingQueue<E>();
        }
        else {
            queue = new ArrayBlockingQueue<E>(size);
        }
    }

    /**
        Creates an object cache of unlimited size.
    */
    public ObjectCache(Creator<E> c) {
        this(c, 0);
    }

    /**
        Tries to retrieve an object from the cache.  If the cache is empty,
        then a new object is created and returned.
    */
    public E get() {
        E object = queue.poll();
        if (object == null) object = creator.create();
        return object;
    }

    /**
        Returns an object to the cache, unless the cache is full.
    */
    public void recycle(E object) {
        queue.offer(object);
    }

    /**
        Returns the current size of the recycle queue.
    */
    public int size() {
        return queue.size();
    }

    /**
        Object constructor interface.
    */
    public interface Creator<E> {
        /** Creates the object. */
        public E create();
    }
}
