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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.samtools.FileTruncatedException;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedFilePointerUtil;
import net.sf.samtools.util.SeekableStream;
import net.sf.samtools.util.BlockCompressedStreamConstants;
import static net.sf.samtools.util.mt.MultiThreading.debug;
import static net.sf.samtools.util.BlockCompressedInputStream.FileTermination.*;

/**
   This is the parallel (multi-threaded) version of {@link
   BlockCompressedInputStream}.  It starts one Reader thread, which
   continuously reads compressed BGZF blocks in batches of size
   {@link net.sf.samtools.Defaults#MT_DECOMPRESS_BATCH_SIZE} and adds
   them to a queue of size
   {@link net.sf.samtools.Defaults#MT_DECOMPRESS_QUEUE_SIZE}
   to be decompressed by the Worker threads.

   <p> Therefore, the amount of read-ahead performed is
   (MT_DECOMPRESS_BATCH_SIZE * MT_DECOMPRESS_QUEUE_SIZE) BGZF blocks.

   <p> If the number of Worker threads
   ({@link net.sf.samtools.Defaults#MT_WORKER_THREADS}) is 0,
   then multi-threading is turned off.

   <p> The relevant constants are described more fully in {@link net.sf.samtools.Defaults
   Defaults}.
 */
public class ParallelBlockCompressedInputStream extends BlockCompressedInputStream {
/* inherited from BlockCompressedInputStream:
    protected InputStream mStream;
    protected SeekableStream mFile;
    protected int mCurrentOffset;
    protected long mBlockAddress; // block base address in file
    protected int mLastBlockLength; // compressed block length
    protected int uncompressedBlockLength; // uncompressed block length
*/
    private Reader reader = null; // Reader thread
    private OrderedJobRunner runner = null;
    private CompressedBlock block = null;
    private boolean eof = false;
    private boolean lastBlockEmpty = false;
    private final AtomicReference<Throwable> ex = new AtomicReference<Throwable>(null);
    private boolean checkCRC = false;
    private boolean improperlyTerminated = false;

    /**
     * Note that seek() is not supported if this constructor is used.
     */
    public ParallelBlockCompressedInputStream(final InputStream stream) {
        super(stream);
        init();
    }

    /**
     * Use this constructor if you wish to call seek()
     */
    public ParallelBlockCompressedInputStream(final File file)
        throws IOException {
        super(file);
        init();

    }

    public ParallelBlockCompressedInputStream(final URL url) {
        super(url);
        init();
    }

    /**
     * For providing some arbitrary data source.  No additional buffering is
     * provided, so if the underlying source is not buffered, wrap it in a
     * SeekableBufferedStream before passing to this ctor.
     */
    public ParallelBlockCompressedInputStream(final SeekableStream strm) {
        super(strm);
        init();
    }

    private void init() {
        if (MultiThreading.numWorkerThreads() <= 0) {
            throw new RuntimeException("MT_WORKER_THREADS is 0");
        }
        debug("ParallelBlockCompressedInputStream created, queue size %d, batch size %d\n",
                MultiThreading.decompressQueueSize(), MultiThreading.decompressBatchSize());
        startRunner();
        startReader(0);
    }

    /**
     * Checks to see if an exception has been raised in the reader thread
     * and if so rethrows it as an Error or RuntimeException as appropriate.
     */
    private final void checkAndRethrow() {
        final Throwable t = this.ex.get();
        if (t != null) {
            if (t instanceof Error) throw (Error) t;
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            else throw new RuntimeException(t);
        }
    }

    private void startRunner() {
        runner = MultiThreading.newOrderedJobRunner(MultiThreading.decompressQueueSize()); // create queues
    }

    private void startReader(long address) {
        // create the reader
        InputStream stream = (mStream != null) ? mStream : mFile;
        reader = new Reader(stream, address, this);
        reader.setDaemon(true); // allow Java to exit
        reader.start(); // start the reader
    }

    /**
     * Determines whether or not the inflater will re-calculated the CRC on the decompressed data
     * and check it against the value stored in the GZIP header.  CRC checking is an expensive
     * operation and should be used accordingly.
     */
    public void setCheckCrcs(final boolean check) {
        checkCRC = check;
    }

    /**
       Returns the number of bytes that can be read (or skipped over)
       from this input stream without blocking.

       <p> As per the {@link BlockCompressedInputStream#available
       BlockCompressedInputStream} implementation, this method <b>does</b>
       block until data is available, and returns 0 on EOF.

       @return the number of bytes that can be read (or skipped over) from this input stream without blocking.
    */
    @Override
    public int available() throws IOException {
        return available(true);
    }

    /**
       Non-blocking version of {@link available()}.  Returns the number
       of bytes in the internal buffer that are currently available
       for reading.

       <p> This method <i>does not block</i>, and a return value of 0
       does not necessarily indicate EOF.  Use {@link eof()} to check for EOF.

       <p> This method is currently declared as private because it's
       unlikely that anybody will even need it, but it is here just in case.

       @return the number of bytes that can be read (or skipped over) from this input stream without blocking.
     */
    private int availableNB() throws IOException {
        return available(false);
    }

    /**
       Returns the number of bytes currently available from this input
       stream.  If {@code wait} is true, then this method will wait for
       data to become available, otherwise it will return the number of
       bytes that can be read without blocking.  Note that a return value
       of 0 indicates EOF only if {@code wait} is true, otherwise it only
       indicates that no data is currently buffered and available for reading..

       @return the number of bytes that can be read (or skipped over) from this input stream without blocking.
     */
    private int available(boolean wait) throws IOException {
        checkAndRethrow();
        checkClosed();
        if (eof) return 0;
        if (mCurrentBlock == null || mCurrentOffset == uncompressedBlockLength) {
            readBlock(wait);
        }
        if (mCurrentBlock == null) {
            return 0;
        }
        return uncompressedBlockLength - mCurrentOffset;
    }

    /**
     * Closes the underlying InputStream or RandomAccessFile
     */
    @Override
    public void close() throws IOException {
        // shut down the reader
        if (reader != null) reader.close(); reader = null;
        // close the OrderedJobRunner
        if (runner != null) runner.close(); runner = null;
        // close the InputStream
        super.close();
        mCurrentBlock = null;
        if (block != null) MultiThreading.recycleBlock(block);
        block = null;
        // rethrow any child exceptions
        checkAndRethrow();
    }

    private void checkClosed() throws IOException {
        if (mStream == null && mFile == null) {
            throw new IOException("stream has already been closed");
        }
    }

    /**
     * Reads and returns the next byte of data from the input stream.
     * If no byte is available because the end of the stream has been reached, -1 is returned.
     * @return the next byte of data, or -1 if the end of the stream is reached.
     */
    @Override
    public int read() throws IOException {
        checkAndRethrow();
        checkClosed();
        if (eof) return -1;
        return (available(true) > 0) ? (mCurrentBlock[mCurrentOffset++] & 0xFF) : -1;
    }

    /**
     * Reads some number of bytes from the input stream and stores them into the buffer array b. The number of b
     * actually read is returned as an integer. This method blocks until input data is available, end of file is
     * or an exception is thrown.
     *
     * read(buf) has the same effect as read(buf, 0, buf.length).
     *
     * @param buffer the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 is there is no more data because the end of
     * the stream has been reached.
     */
    @Override
    public int read(final byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    /**
     * Reads a whole line. A line is considered to be terminated by either a line feed ('\n'),
     * carriage return ('\r') or carriage return followed by a line feed ("\r\n").
     *
     * @return  A String containing the contents of the line, excluding the line terminating
     *          character, or null if the end of the stream has been reached
     *
     * @exception  IOException  If an I/O error occurs
     * @
     */
    @Override
    public String readLine() throws IOException {
        checkAndRethrow();
        checkClosed();
        return super.readLine();
    }

    /**
     * Attempts to read {@code length} bytes of data from the input stream into an array of bytes.
     * A smaller number of bytes may be read if end-of-file is reached first.
     * The number of bytes actually read is returned as an integer, or -1 if the stream is at end-of-file.
     *
     * This method blocks until input data is available, end of file is detected, or an exception is thrown.
     *
     * @param buffer buffer into which data is read.
     * @param offset the start offset in {@code buffer} at which the data is written.
     * @param length the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data because the end of
     * the stream has been reached.
     */
    @Override
    public int read(final byte[] buffer, int offset, int length) throws IOException {
        checkAndRethrow();
        checkClosed();
        return super.read(buffer, offset, length);
    }

    /**
     * Seeks to the given virtual position in the file.  The position is composed of a 48-bit
     * offset into the compressed file shifted left by 16 bits, and a 16-bit offset into the
     * the decompressed BGZF block.
     *
     * @param pos virtual file pointer
     */
    @Override
    public void seek(final long pos) throws IOException {
        checkAndRethrow();
        checkClosed();
        if (mFile == null) {
            throw new IOException("Cannot seek on stream based file");
        }
        // Decode virtual file pointer
        // Upper 48 bits is the byte offset into the compressed stream of a block.
        // Lower 16 bits is the byte offset into the uncompressed stream inside the block.
        final long blockAddress = BlockCompressedFilePointerUtil.getBlockAddress(pos);
        final int blockOffset = BlockCompressedFilePointerUtil.getBlockOffset(pos);
        debug(2, "ParallelBlockCompressedInputStream.seek(%d:%d) ", blockAddress, blockOffset);
        debug(2, "current address (%d:%d)\n", mBlockAddress, mCurrentOffset);
        final int available;
        // are we already at the correct block?
        if (mBlockAddress == blockAddress && mCurrentBlock != null) {
            debug(2, "seek: we are already in correct block, no seek required\n");
            available = uncompressedBlockLength;
        }
        else {
            // close the OrderedJobRunner
            debug(2, "closing runner\n");
            if (runner != null) runner.close(); runner = null;
            // tell the reader to shut down, and wait until it does
            if (reader != null) reader.close(); reader = null;

            debug(2, "seeking to block address %d\n", blockAddress);
            // seek and reset variables
            mFile.seek(blockAddress);
            mBlockAddress = blockAddress;
            mLastBlockLength = 0;
            mCurrentBlock = null;
            mCurrentOffset = uncompressedBlockLength = 0;
            eof = false;
            // recycle old block
            if (block != null) MultiThreading.recycleBlock(block);
            block = null;

            // start a new OrderedJobRunner and Reader
            debug(2, "starting new runner\n");
            startRunner();
            startReader(blockAddress);

            available = available(true); // read block and return size
        }
        // is offset greater than block size?
        if (blockOffset >= available) {
            throw new IOException("Invalid file pointer: " + pos);
        }
        mCurrentOffset = blockOffset;
    }

    private void setEOF() {
        // we are at EOF so shut down the reader and runner.
        // seek() will start up new ones
        if (reader != null) reader.close(); reader = null;
        if (runner != null) runner.close(); runner = null;
        eof = true;
        if (!lastBlockEmpty && !isSeekable()) {
            System.err.print("warning: BGZF stream was not properly terminated before EOF!\n");
        }
        mCurrentBlock = null;
        if (block != null) MultiThreading.recycleBlock(block);
        block = null;
    }

    Iterator<Job> currentIterator = null;

    @Override
    protected void readBlock() throws IOException {
        readBlock(true); // read next block, waiting if necessary
    }

    // Read next block from the output queue
    private void readBlock(boolean wait) throws IOException {
        if (block != null) MultiThreading.recycleBlock(block);
        block = null;
        if (eof) return;
        while (true) {
            mCurrentOffset = 0;
            mBlockAddress += mLastBlockLength; // add size of current block
            mLastBlockLength = 0; // set current block to empty
            uncompressedBlockLength = 0;
            mCurrentBlock = null;
            try {
                if (currentIterator == null) {
                    GroupJob job;
                    if (wait) {
                        job = (GroupJob) runner.getFinishedJob();
                        if (job == null) break; // EOF
                    }
                    else {
                        job = (GroupJob) runner.pollFinishedJob();
                        if (job == null) return; // nothing available
                    }
                    debug(3, "readBlock(%b) returns job %s from queue %s\n", wait, job, runner);
                    currentIterator = job.jobs.iterator();
                }
                if (currentIterator.hasNext()) {
                    block = (CompressedBlock) currentIterator.next();
                    debug(3, "readBlock(%b) returns block %s cancelled %b\n", wait, block, block.isCancelled());
                    if (block.isCancelled()) {
                        // force another read from the queue
                        currentIterator = null;
                        continue;
                    }
                    // Reader closed or EOF?
                    if (block == null || block.blockLength < 0) {
                        setEOF();
                        return;
                    }
                    if (mBlockAddress != block.blockAddress) {
                        String err = String.format("coding error: wrong block address! wanted %d but got %d.",
                                mBlockAddress, block.blockAddress);
                        throw new IOException(err);
                    }
                    mLastBlockLength = block.priorLength;
                    uncompressedBlockLength = block.blockLength;
                    mCurrentBlock = block.buffer;
                    debug(3, "read block of size %d\n", block.blockLength);
                    lastBlockEmpty = (block.blockLength == 0);
                    if (lastBlockEmpty) {
                        debug(2, "ignoring empty block at block address %d\n", mBlockAddress);
                        continue;
                    }
                    return;
                }
                else {
                    currentIterator = null;
                    // loop again and read another Job
                    continue;
                }
            }
            catch (InterruptedException e) {
                /*
                   Somebody interrupted us (probably ParallelBAMFileReader),
                   but we can't throw InterruptedException without declaring it,
                   and we can't declare it because available() and read() do not
                   throw it.  So we shut things down and throw ThreadDeath instead,
                   using Thread.currentThread().stop().
                */
                debug(2, "readBlock(%b) interrupted in thread %s\n\n", wait, Thread.currentThread());
                if (runner != null) runner.close(); runner = null;
                if (reader != null) reader.close(); reader = null;
                Thread.currentThread().stop(); // bye bye cruel world
            }
        }
    }

    /** Returns true of the stream is at end-of-file. */
    @Override
    protected boolean eof() throws IOException {
        checkAndRethrow();
        return eof;
    }

    /**
     * Reads BGZF blocks from a stream and adds them to a queue for decompression. This
     * class only reads sequentially. If a seek needs to be performed, a new Reader
     * must be created.
     */
    static class Reader extends Thread {
        // The input stream
        private InputStream inputStream = null;

        // The reader's file position
        private long readerAddress = 0;

        // tell Reader thread to stop
        private boolean stop = false;

        private final ParallelBlockCompressedInputStream parent;

        /**
         * Creates a new reader.
         * @param stream the underlying input stream.
         * @param file the file.
         * @param queue the input queue.
         */
        public Reader(final InputStream stream, long address, ParallelBlockCompressedInputStream parent) {
            super("BlockReader-" + address);
            debug(2, "starting new Reader %s at block address %d [%x]\n", this, address, address);
            this.inputStream = stream;
            this.readerAddress = address;
            this.parent = parent;
        }

        // read fully a certain number of bytes, unless EOF is hit
        private int readBytes(final byte[] buffer, final int offset, final int length) throws Exception {
            int bytesRead = 0;
            while (bytesRead < length) {
                if (stop || inputStream == null) throw new InterruptedException();
                final int count = inputStream.read(buffer, offset + bytesRead, length - bytesRead);
                if (count <= 0) {
                    break;
                }
                bytesRead += count;
            }
            return bytesRead;
        }

        /**
         * Reads a block's worth of data into the block. The block address is set here.
         * @param block the block in which to place the data.
         * @return true if successful, false otherwise.
         */
        private boolean readBlock(CompressedBlock block) throws Exception {
            int count = readBytes(block.buffer, 0, BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH);
            if (count == 0) {
                // Handle EOF
                block.blockLength = -1;
                return false;
            }
            if (count != BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH) {
                throw new IOException("Premature end of file");
            }
            block.blockLength = unpackInt16(block.buffer, BlockCompressedStreamConstants.BLOCK_LENGTH_OFFSET) + 1;
            if (block.blockLength < BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH || block.blockLength > block.buffer.length) {
                throw new IOException("Unexpected compressed block length: " + block.blockLength);
            }
            final int remaining = block.blockLength - BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH;
            count = readBytes(block.buffer, BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH, remaining);
            if (count != remaining) {
                throw new FileTruncatedException("Premature end of file");
            }
            // reset the buffer position
            // set the block's starting address
            block.blockAddress = this.readerAddress;
            // update file pointer
            this.readerAddress += block.blockLength;
            debug(3, "Reader marked block at address %d\n", block.blockAddress);
            return true;
        }

        /**
         * Shut down the reader.
         */
        public void close() {
            debug(2, "closing Reader %s in thread %s\n", this, Thread.currentThread());
            stop = true; // stop if polling
            this.interrupt(); // stop if waiting
            try {
                /*
                   We MUST wait for the Reader thread to stop, because
                   we can't safely seek until the thread stops reading.
                */
                this.join();
            }
            catch (InterruptedException e) {
                throw new RuntimeException("thread interrupted");
            }
        }

        /**
         * Runs the reader.
         */
        public void run() {
            OrderedJobRunner queue = parent.runner;
            GroupJob job = new GroupJob(queue, parent.ex);
            try {
                while (true) {
                    CompressedBlock b = new CompressedBlock(queue, parent.ex);
                    b.checkCrcs = parent.checkCRC;
                    if (queue.isClosed()) break; // quit if closed
                    boolean eof = !readBlock(b);
                    if (queue.isClosed()) break; // quit if closed
                    if (job.jobs.size() >= MultiThreading.decompressBatchSize()) {
                        debug(3, "Reader adds job %s to queue %s\n", job, queue);
                        queue.add(job);
                        job = new GroupJob(queue, parent.ex);
                    }
                    debug(3, "Reader %s adding block %s with address %d to job\n", this, b, b.blockAddress);
                    if (eof) debug("eof at %d next %d in Reader %s\n", b.blockAddress, readerAddress, this);
                    job.add(b);
                    if (eof) {
                        queue.add(job);
                        break; // EOF, so stop
                    }
                }
            }
            catch (InterruptedException e) {
                // somebody interrupted us, so exit
            }
            catch (Throwable t) {
                // tell the parent about the error or exception
                parent.ex.compareAndSet(null, t);
            }
            debug(2, "ParallelBlockCompressedInputStream.Reader %s exiting\n", this);
        }
    }
}
