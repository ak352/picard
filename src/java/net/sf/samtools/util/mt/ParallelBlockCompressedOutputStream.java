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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;

import net.sf.samtools.util.*;
import static net.sf.samtools.util.BlockCompressedOutputStream.getDefaultCompressionLevel;
import static net.sf.samtools.util.mt.MultiThreading.debug;

/**
   <p> This is a parallel (multi-threaded) version of {@link
   BlockCompressedOutputStream}.

   <p> Writes are performed to a 64K buffer, which represents an
   uncompressed BGZF block.  When the block is full, it is added to a
   queue of size {@link net.sf.samtools.Defaults#MT_COMPRESS_QUEUE_SIZE}
   to be compressed by the Worker threads. Compressed blocks are removed
   from this queue by the Writer thread, and written to the underlying OutputStream.

   <p> If the number of Worker threads
   ({@link net.sf.samtools.Defaults#MT_WORKER_THREADS}) is 0,
   then multi-threading is turned off.

   <p> The relevant constants are described more fully in {@link net.sf.samtools.Defaults
   Defaults}.

 */
public class ParallelBlockCompressedOutputStream extends OutputStream {
    private final BinaryCodec codec;
    private byte[] uncompressedBuffer = null;
    private int numUncompressedBytes = 0;
    private final byte[] compressedBuffer = null;
    private File file = null;
    private long mBlockAddress = 0;

    // new parallel stuff
    private Writer writer = null;
    private OrderedJobRunner runner = null;
    private CompressedBlock block = null;
    private final AtomicReference<Throwable> ex = new AtomicReference<Throwable>(null);
    private int compressionLevel;
    private transient boolean closed = false; // stream is closed for writing

    /**
     * Uses default compression level, which is 5 unless changed by setDefaultCompressionLevel
     */
    public ParallelBlockCompressedOutputStream(final String filename) {
        this(filename, getDefaultCompressionLevel());
    }

    /**
     * Uses default compression level, which is 5 unless changed by setDefaultCompressionLevel
     */
    public ParallelBlockCompressedOutputStream(final File file) {
        this(file, getDefaultCompressionLevel());
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public ParallelBlockCompressedOutputStream(final String filename, final int compressionLevel) {
        this(new File(filename), compressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public ParallelBlockCompressedOutputStream(final File file, final int compressionLevel) {
        this.file = file;
        codec = new BinaryCodec(file, true);
        init(compressionLevel);
    }

    /**
     * Constructors that take output streams
     * file may be null
     */
    public ParallelBlockCompressedOutputStream(final OutputStream os, File file) {
        this(os, file, getDefaultCompressionLevel());
    }

    public ParallelBlockCompressedOutputStream(final OutputStream os, final File file, final int compressionLevel) {
        this.file = file;
        codec = new BinaryCodec(os);
        if (file != null) {
            codec.setOutputFileName(file.getAbsolutePath());
        }
        init(compressionLevel);
    }

    /**
     * Writes b.length bytes from the specified byte array to this output stream. The general contract for write(b)
     * is that it should have exactly the same effect as the call write(b, 0, b.length).
     * @param bytes the data
     */
    @Override
    public void write(final byte[] bytes) throws IOException {
        checkAndRethrow();
        write(bytes, 0, bytes.length);
    }

    /**
     * Writes the specified byte to this output stream. The general contract for write is that one byte is written
     * to the output stream. The byte to be written is the eight low-order bits of the argument b.
     * The 24 high-order bits of b are ignored.
     * @param bite
     * @throws IOException
     */
    // Really a local variable, but allocate once to reduce GC burden.
    private final byte[] singleByteArray = new byte[1];
    @Override
    public void write(final int bite) throws IOException {
        checkAndRethrow();
        singleByteArray[0] = (byte)bite;
        write(singleByteArray);
    }

    /**
     * Writes len bytes from the specified byte array starting at offset off to this output stream. The general
     * contract for write(b, off, len) is that some of the bytes in the array b are written to the output stream in order;
     * element b[off] is the first byte written and b[off+len-1] is the last byte written by this operation.
     *
     * @param bytes the data
     * @param startIndex the start offset in the data
     * @param numBytes the number of bytes to write
     */
    @Override
    public void write(final byte[] bytes, int startIndex, int numBytes) throws IOException {
        checkAndRethrow();
        if (this.closed) throw new IOException("stream is closed!");
        int maxLength = Math.min(uncompressedBuffer.length, BlockCompressedStreamConstants.DEFAULT_UNCOMPRESSED_BLOCK_SIZE);
        assert(numUncompressedBytes < maxLength);
        while (numBytes > 0) {
            final int bytesToWrite = Math.min(maxLength - numUncompressedBytes, numBytes);
            System.arraycopy(bytes, startIndex, uncompressedBuffer, numUncompressedBytes, bytesToWrite);
            numUncompressedBytes += bytesToWrite;
            startIndex += bytesToWrite;
            numBytes -= bytesToWrite;
            assert(numBytes >= 0);
            if (numUncompressedBytes == maxLength) {
                deflateBlock();
            }
        }
    }

    /**
     * WARNING: flush() affects the output format, because it causes the current contents of uncompressedBuffer
     * to be compressed and written, even if it isn't full.  Unless you know what you're doing, don't call flush().
     * Instead, call close(), which will flush any unwritten data before closing the underlying stream.
     */
    @Override
    public void flush() throws IOException {
        checkAndRethrow();
        try {
            // add the current block to the queue
            while (numUncompressedBytes > 0) {
                deflateBlock();
                checkAndRethrow();
            }
            // flush the writer
            writer.flush();

        } catch (RuntimeException e) {
            throw new IOException(e);
        }
        checkAndRethrow();
    }

    /**
     * close() must be called in order to flush any remaining buffered bytes.
     * An unclosed file will likely be defective.
     */
    @Override
    public void close() throws IOException {
        checkAndRethrow();
        if (closed) return;
        flush(); // flush the data
        // write an empty GZIP block directly to the codec
        codec.writeBytes(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
        writer.close(); // close the Writer
        codec.close(); // close the codec
        closed = true;
        // Can't re-open something that is not a regular file, e.g. a named pipe or an output stream
        if (this.file == null || !this.file.isFile()) return;
        if (BlockCompressedInputStream.checkTermination(this.file) !=
                BlockCompressedInputStream.FileTermination.HAS_TERMINATOR_BLOCK) {
            throw new IOException("Terminator block not found after closing BGZF file " + this.file);
        }
        checkAndRethrow();
    }

    public DelayedFilePointer getDelayedFilePointer() {
        checkAndRethrow();
        return new DelayedFilePointer(this.block, numUncompressedBytes);
    }

    private void init(int compressionLevel) {
        if (MultiThreading.numWorkerThreads() <= 0) {
            throw new RuntimeException("MT_WORKER_THREADS is 0");
        }
        this.compressionLevel = compressionLevel;
        debug("ParallelBlockCompressedOutputStream: queue size %d, compression level %d\n",
            MultiThreading.compressQueueSize(), compressionLevel);
        // get the queue for this stream
        this.runner = MultiThreading.newOrderedJobRunner(MultiThreading.compressQueueSize());
        // create the writer
        writer = new Writer();
        writer.setDaemon(true);
        // start the writer
        writer.start();
        newBlock(); // allocate first block
    }

    private void newBlock() {
        block = new CompressedBlock(runner, ex, compressionLevel);
        // set variables in the superclass
        numUncompressedBytes = 0;
        uncompressedBuffer = block.buffer;
    }

    private void deflateBlock() {
        try {
            block.blockLength = numUncompressedBytes;
            runner.add(block);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        newBlock();
    }

    /**
     * Rethrows any exception raised in the reader thread or one of the worker threads.
     */
    private final void checkAndRethrow() {
        MultiThreading.checkAndRethrow(ex);
    }

    static int writerCount = 0;
    class Writer extends Thread {

        /**
         * The block address of the last block written.
         */
        private long mBlockAddress = 0;

        /**
         * Creates a new writer.
         */
        public Writer() {
            super("BlockWriter-" + writerCount++);
        }

        /**
         * Writes the given GZIP block, adding the header.
         * @param block the deflated block to write.
         */
        private int writeGzipBlock(final CompressedBlock block) {
            // Init gzip header 
            codec.writeByte(BlockCompressedStreamConstants.GZIP_ID1);
            codec.writeByte(BlockCompressedStreamConstants.GZIP_ID2);
            codec.writeByte(BlockCompressedStreamConstants.GZIP_CM_DEFLATE);
            codec.writeByte(BlockCompressedStreamConstants.GZIP_FLG);
            codec.writeInt(0); // Modification time
            codec.writeByte(BlockCompressedStreamConstants.GZIP_XFL);
            codec.writeByte(BlockCompressedStreamConstants.GZIP_OS_UNKNOWN);
            codec.writeShort(BlockCompressedStreamConstants.GZIP_XLEN);
            codec.writeByte(BlockCompressedStreamConstants.BGZF_ID1);
            codec.writeByte(BlockCompressedStreamConstants.BGZF_ID2);
            codec.writeShort(BlockCompressedStreamConstants.BGZF_LEN);
            final int totalBlockSize = block.blockLength + BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH +
                    BlockCompressedStreamConstants.BLOCK_FOOTER_LENGTH;
                
            // I don't know why we store block size - 1, but that is what the spec says
            codec.writeShort((short)(totalBlockSize - 1));
            codec.writeBytes(block.buffer, 0, block.blockLength);
            codec.writeInt((int)block.crc);
            codec.writeInt(block.priorLength);
            return totalBlockSize;
        }

        private CountDownLatch writeQueueIsEmpty = new CountDownLatch(1);

        /**
         * Runs the writer.
         */
        public void run() {
            try {
                while (!closed) {
                    if (runner.isEmpty()) writeQueueIsEmpty.countDown();
                    else if (writeQueueIsEmpty.getCount() == 0) {
                        writeQueueIsEmpty = new CountDownLatch(1);
                    }
                    // get a block
                    CompressedBlock b = (CompressedBlock) runner.getFinishedJob();
                    if (b == null) { // OrderedJobRunner was closed
                        break;
                    }
                    // write the block
                    int totalBlockSize = writeGzipBlock(b);
                    b.setBlockAddress(this.mBlockAddress);
                    this.mBlockAddress += totalBlockSize;
                }
            }
            catch (InterruptedException e) {
                // somebody told us to stop
            }
            catch (Throwable t) {
                // tell the parent thread about the error or exception
                ex.compareAndSet(null, t);
            }
        }

        /**
         * Closes the writer.
         */
        public void close() throws IOException {
            this.flush();
            writer.interrupt();
        }

        /**
           Waits for the write queue (OrderedJobRunner's output queue) to
           become empty, then flushes the underlying stream.  To avoid
           race conditions, this method must only be called by the main
           thread that is filling the queue.
         */
        public void flush() throws IOException {
            // wait until the output queue is empty
            try {
                writeQueueIsEmpty.await();
            }
            catch (InterruptedException e) {
                throw new IOException("flush() interrupted");
            }
            codec.getOutputStream().flush();
        }
    }

    /**
     * Encapsulates a virtual file pointer that waits until the associated block is written before 
     * returning its result.
     */
    public static class DelayedFilePointer {
        private CompressedBlock block = null;
        private int offset = 0; // this needs to be set as the block.blockOffset could change...

        public DelayedFilePointer(CompressedBlock block, int offset) {
            this.block = block;
            this.offset = offset;
        }

        /**
         * Convert the file pointer to its <code>long</code> equivalent.
         * Will wait until the associated blocks have been written.
         */
        public long toLong() {
            return BlockCompressedFilePointerUtil.makeFilePointer(this.getBlockAddress(), this.offset);
        }

        /**
         * Return the block address portion of the file pointer.
         * Will wait until the associated blocks have been written.
         * @return File offset of start of BGZF block for this virtual file pointer.
         */
        public long getBlockAddress() {
            return this.block.getBlockAddress();
        }

        /**
         * @return Offset into uncompressed block for this virtual file pointer.
         */
        public int getBlockOffset() {
            return offset;
        }
    }
}
