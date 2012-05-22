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

import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.samtools.Defaults;
import static net.sf.samtools.util.BlockCompressedStreamConstants.*;

/**
   Shared objects and methods used for multi-threading.

   We create a fixed number of Worker threads that do most of the
   CPU-intensive work, and a shared input queue that feeds Jobs
   to the Workers. (This is the classic "single queue, multiple server"
   model.)

   Java's memory allocation is very slow, so we also create two byte array
   caches to hold the buffers that can be reused repeated.  After each
   buffer has been used, it is recycled back into the cache.
*/
public class MultiThreading {
    private static int numWorkerThreads = Defaults.MT_WORKER_THREADS;
    private static int decompressBatchSize = Defaults.MT_DECOMPRESS_BATCH_SIZE;
    private static int decompressQueueSize = Defaults.MT_DECOMPRESS_QUEUE_SIZE;
    private static int compressQueueSize = Defaults.MT_COMPRESS_QUEUE_SIZE;
    private static int bamDecodeQueueSize = Defaults.MT_BAMDECODE_QUEUE_SIZE;

    // Cache of 64K byte arrays for CompressedBlock.
    // 64K is large enough to hold any BGZF block, whether
    // compressed or uncompressed.
    private static final int BGZF_BUFFER_SIZE = 64 * 1024; // NEVER CHANGE THIS!
    private static final int BGZF_BUFFER_CACHE_SIZE = 2000;
    private static ObjectCache<byte[]> bgzfBufferCache = null;

    // Cache of byte arrays to hold undecoded BAM records.
    private static final int BAMREC_BUFFER_SIZE = 64 * 1024;
    private static final int BAMREC_BUFFER_CACHE_SIZE = 2000;
    private static ObjectCache<byte[]> bamRecBufferCache = null;

    public static int numWorkerThreads() {
        return numWorkerThreads;
    }

    public static boolean isParallelCompressionEnabled() {
        return compressQueueSize > 0 && numWorkerThreads > 0;
    }

    public static boolean isParallelDecompressionEnabled() {
        return decompressQueueSize > 0 && numWorkerThreads > 0;
    }

    public static boolean isParallelBamReaderEnabled() {
        return bamDecodeQueueSize > 0 && numWorkerThreads > 0;
    }

    public static int decompressBatchSize() {
        return decompressBatchSize;
    }

    public static int decompressQueueSize() {
        return decompressQueueSize;
    }

    public static int compressQueueSize() {
        return compressQueueSize;
    }

    public static int bamDecodeQueueSize() {
        return bamDecodeQueueSize;
    }

    public static int bamDecodeBufferSize() {
        return BAMREC_BUFFER_SIZE;
    }

    // output debugging information
    public static void debug(String s)                              { debug(1, s); }
    public static void debug(PrintStream ps, String s)              { debug(1, ps, s); }
    public static void debug(String s, Object... o)                 { debug(1, s, o); }
    public static void debug(PrintStream ps, String s, Object... o) { debug(1, ps, s, o); }
    public static void debug(int level, String s) {
        if (Defaults.MT_DEBUG_LEVEL >= level) System.err.print(s);
    }
    public static void debug(int level, PrintStream ps, String s) {
        if (Defaults.MT_DEBUG_LEVEL >= level) ps.print(s);
    }
    public static void debug(int level, String s, Object... o) {
        if (Defaults.MT_DEBUG_LEVEL >= level) System.err.printf(s, o);
    }
    public static void debug(int level, PrintStream ps, String s, Object... o) {
        if (Defaults.MT_DEBUG_LEVEL >= level) ps.printf(s, o);
    }

    private static ObjectCache<byte[]> bgzfBufferCache() {
        if (bgzfBufferCache == null) {
            ByteArrayCreator creator = new ByteArrayCreator(BGZF_BUFFER_SIZE);
            bgzfBufferCache = new ObjectCache<byte[]>(creator, BGZF_BUFFER_CACHE_SIZE);
        }
        return bgzfBufferCache;
    }

    private static ObjectCache<byte[]> bamRecBufferCache() {
        if (bamRecBufferCache == null) {
            ByteArrayCreator creator = new ByteArrayCreator(BAMREC_BUFFER_SIZE);
            bamRecBufferCache = new ObjectCache<byte[]>(creator, BAMREC_BUFFER_CACHE_SIZE);
        }
        return bamRecBufferCache;
    }

    public static byte[] getBgzfBuffer() {
        return bgzfBufferCache().get();
    }

    public static byte[] getBamRecBuffer() {
        return bamRecBufferCache().get();
    }

    public static void recycleBamRecBuffer(byte[] buffer) {
        if (buffer.length != BAMREC_BUFFER_SIZE) return;
        bamRecBufferCache().recycle(buffer);
    }

    public static void recycleBgzfBuffer(byte[] buffer) {
        if (buffer.length != BGZF_BUFFER_SIZE) return;
        bgzfBufferCache().recycle(buffer);
    }

    static void recycleBlock(CompressedBlock block) {
        if (block == null || block.buffer == null) return;
        if (block.buffer.length != BGZF_BUFFER_SIZE) return;
        bgzfBufferCache().recycle(block.buffer);
        block.buffer = null;
    }

    private static class ByteArrayCreator implements ObjectCache.Creator<byte[]> {
        final int size;
        public ByteArrayCreator(int size) {
            this.size = size;
        }
        public byte[] create() {
            return new byte[size];
        }
    }

    /**
     * The shared input queue for the workers.
     */
    private static BlockingQueue<Job> input_queue = null;

    /**
     * The default queue size, used for both the Worker {@code input_queue} and the
     * OrderedJobRunner {@code output} queues.
     */
    static final int queue_size = Defaults.MT_WORKER_QUEUE_SIZE;

    /**
     * The consumer threads for compressing and decompressing blocks.
     */
    private static List<Worker> workers = null;
 
    /**
     * Start the Worker threads.
     */
    static void startWorkers() {
        if (workers != null) return;
        input_queue = new ArrayBlockingQueue<Job>(queue_size);
        workers = new ArrayList<Worker>();
        for (int i = 0; i < numWorkerThreads; i++) {
            Worker w = new Worker(input_queue);
            w.setDaemon(true); // don't prohibit Java from exiting
            workers.add(w);
            w.start();
        }
    }

    /**
     * Returns a new OrderedJobRunner, and starts the Worker threads if necessary.
     * @return the OrderedJobRunner
     */
    public static OrderedJobRunner newOrderedJobRunner(int queueSize) {
        startWorkers();
        return new OrderedJobRunner(input_queue, queueSize);
    }

    /**
     * Checks to see if an exception has been raised in the reader thread
     * and if so rethrows it as an Error or RuntimeException as appropriate.
     */
    public static final void checkAndRethrow(AtomicReference<Throwable> ex) {
        final Throwable t = ex.get();
        if (t != null) {
            if (t instanceof Error) throw (Error) t;
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            else throw new RuntimeException(t);
        }
    }

    /**
        If size > 0 then return a new ArrayBlockingQueue of fixed size,
        otherwise return a new LinkedBlockingQueue of unlimited size.
    */
    <E> BlockingQueue<E> newQueue(int size, E value) {
        if (size <= 0) { // unlimited size
            return new LinkedBlockingQueue<E>();
        }
        else {
            return new ArrayBlockingQueue<E>(size);
        }
    }
}
