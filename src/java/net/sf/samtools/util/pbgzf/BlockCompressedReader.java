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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import net.sf.samtools.util.SeekableStream;
import net.sf.samtools.util.SeekableFileStream;
import net.sf.samtools.util.SeekableBufferedStream;
import net.sf.samtools.util.SeekableHTTPStream;
import net.sf.samtools.util.IOUtil;
import net.sf.samtools.util.BlockCompressedStreamConstants;

import net.sf.samtools.FileTruncatedException;

/**
 * Reads blocks of data from a file, and adds it to a queue for consumption. Unlike a 
 * Thread, this class can be started and joined multiple times.
 *
 */
public class BlockCompressedReader {
    /**
     * The input stream.
     */
    private InputStream mStream = null;

    /**
     * The input file.
     */
    private SeekableStream mFile = null;

    /**
     * The underlying reader thread.
     */
    private BlockCompressedReaderThread readerThread = null;

    /**
     * The latest block address read.
     */
    private long mBlockAddress = 0;

    /**
     * The queue in which to place blocks.
     */
    public BlockCompressedQueue queue = null;

    /**
     * True if the reader has read in all blocks, false otherwise.
     */
    private boolean isDone = false;

    /**
     * True if the reader has been closed, false otherwise.
     */
    public boolean isClosed = false;

    /**
     * Creates a new reader.
     * @param stream the underlying input stream.
     * @param file the file.
     * @param queue the input queue.
     */
    public BlockCompressedReader(final InputStream stream, final SeekableStream file, BlockCompressedQueue queue) {
        this.mStream = stream;
        this.mFile = file;
        this.queue = queue;
    }

    private int readBytes(final byte[] buffer, final int offset, final int length)
        throws IOException {
        if (mFile != null) {
            return readBytes(mFile, buffer, offset, length);
        } else if (mStream != null) {
            return readBytes(mStream, buffer, offset, length);
        } else {
            return 0;
        }
    }

    private static int readBytes(final SeekableStream file, final byte[] buffer, final int offset, final int length)
        throws IOException {
        int bytesRead = 0;
        while (bytesRead < length) {
            final int count = file.read(buffer, offset + bytesRead, length - bytesRead);
            if (count <= 0) {
                break;
            }
            bytesRead += count;
        }
        return bytesRead;
    }

    private static int readBytes(final InputStream stream, final byte[] buffer, final int offset, final int length)
        throws IOException {
        int bytesRead = 0;
        while (bytesRead < length) {
            final int count = stream.read(buffer, offset + bytesRead, length - bytesRead);
            if (count <= 0) {
                break;
            }
            bytesRead += count;
        }
        return bytesRead;
    }

    private int unpackInt16(final byte[] buffer, final int offset) {
        return ((buffer[offset] & 0xFF) |
                ((buffer[offset+1] & 0xFF) << 8));
    }

    /**
     * Reads a block's worth of data into the block. The block address is set here.
     * @param block the block in which to place the data.
     * @return true if successful, false otherwise.
     */
    private boolean readBlock(BlockCompressed block)
        throws IOException {

        if (null == block) return false;
        
        if (block.buffer == null) {
            block.buffer = new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE];
        }
        int count = readBytes(block.buffer, 0, BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH);
        if (count == 0) {
            // Handle case where there is no empty gzip block at end.
            block.blockLength = 0; // TODO: this.blockLength = 0?
            return true;
        }
        if (count != BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH) {
            throw new IOException("Premature end of file");
        }
        block.blockLength = unpackInt16(block.buffer, BlockCompressedStreamConstants.BLOCK_LENGTH_OFFSET) + 1;
        if (block.blockLength < BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH || block.blockLength > block.buffer.length) {
            /*
            System.err.println("block.blockLength=" + block.blockLength);
            System.err.println("block.buffer.length=" + block.buffer.length);
            System.err.println("BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH=" + BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH);
            */
            throw new IOException("Unexpected compressed block length: " + block.blockLength);
        }
        final int remaining = block.blockLength - BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH;
        count = readBytes(block.buffer, BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH, remaining);
        if (count != remaining) {
            throw new FileTruncatedException("Premature end of file");
        }
        block.blockOffset = 0;
        
        // set the block address
        block.setBlockAddress(this.mBlockAddress);

        this.mBlockAddress += block.blockLength;
        return true;
    }

    /**
     * Set the reader state to done.
     */
    public void setDone() {
        this.isDone = true;
    }

    /**
     * @return returns true if the reader is done reading.
     */
    public boolean isDone() {
        return this.isDone;
    }

    /**
     * Resets the state of the reader.
     * @param queue the new input queue.
     */
    public void reset(BlockCompressedQueue queue) {
        this.isDone = this.isClosed = false;
        this.queue = queue;
    }

    /**
     * @return returns the underlying file.
     */
    public SeekableStream getFile() {
        return mFile;
    }

    /**
     * Closes this reader.
     */
    public void close()
        throws IOException {
        if (mFile != null) {
            mFile.close();
            mFile = null;
        } else if (mStream != null) {
            mStream.close();
            mStream = null;
        }
        this.queue = null;
    }

    /**
     * Starts the underlying reader thread.
     */
    public void start() {
        this.readerThread = new BlockCompressedReaderThread(this);
        this.readerThread.setDaemon(true);
        this.readerThread.start();
    }

    /**
     * Joins the underlying reader thread.
     */
    public void join() {
        try {
            this.readerThread.join();
            this.readerThread = null;
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * The underlying reader thread.
     */
    protected class BlockCompressedReaderThread extends Thread {
        /**
         * The reader associated with this thread.
         */
        private BlockCompressedReader reader = null;

        /**
         * @param the reader associated with this thread.
         */
        public BlockCompressedReaderThread(BlockCompressedReader reader) {
            this.reader = reader;
        }

        /**
         * Runs the reader.
         */
        public void run()
        {
            BlockCompressed b = null;
            boolean wait = true;
            long n = 0;

            try {
                while(!this.reader.isDone) {
                    // TODO: use the local pool

                    // read a block
                    //System.err.println("Reader: reading");
                    b = new BlockCompressed();
                    if(!this.reader.readBlock(b)) {
                        throw new Exception("BlockCompressedReader: could not read a block");
                    }
                    if(null == b || 0 == b.blockLength) {
                        b = null;
                        break;
                    }

                    // add it to the queue
                    //System.err.println("Reader: adding");
                    if(!this.reader.queue.add(b)) {
                        break;
                    }
                    //System.err.println("Reader: looping");
                    b = null;
                    n++;
                }
                //System.err.println("Reader: done (" + n + ")");
                this.reader.isDone = true;
            }
            catch(Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
