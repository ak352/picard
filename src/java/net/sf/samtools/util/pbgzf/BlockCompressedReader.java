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

public class BlockCompressedReader {
    private InputStream mStream = null;
    private SeekableStream mFile = null;
    private BlockCompressedReaderThread readerThread = null;

    // TODO: are these necessary?
    private int blockOffset = 0;
    private long mBlockAddress = 0;

    public BlockCompressedBlockingQueue input = null;
    private boolean isDone = false;
    public boolean isClosed = false;

    private int id;

    public BlockCompressedReader(final InputStream stream, final SeekableStream file, BlockCompressedBlockingQueue input, int id) {
        this.mStream = stream;
        this.mFile = file;
        this.input = input;
        this.id = id;
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
        //System.err.println("HERE 12 block.blockAddress=" + block.blockAddress);
        //System.err.println("HERE 12 block.blockLength=" + block.blockLength);
        return true;
    }

    public void setDone() {
        this.isDone = true;
    }

    public boolean isDone() {
        return this.isDone;
    }

    public void reset(int id) {
        this.isDone = this.isClosed = false;
        this.id = id;
    }

    public SeekableStream getFile() {
        return mFile;
    }

    public void close()
        throws IOException {
        if (mFile != null) {
            mFile.close();
            mFile = null;
        } else if (mStream != null) {
            mStream.close();
            mStream = null;
        }
        this.input = null;
    }

    public void start() {
        this.readerThread = new BlockCompressedReaderThread(this);
        this.readerThread.setDaemon(true);
        this.readerThread.start();
    }

    public void join() {
        try {
            this.readerThread.join();
            this.readerThread = null;
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected class BlockCompressedReaderThread extends Thread {
        private BlockCompressedReader reader = null;

        public BlockCompressedReaderThread(BlockCompressedReader reader) {
            this.reader = reader;
        }

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
                    b = new BlockCompressed(this.reader.id);
                    if(!this.reader.readBlock(b)) {
                        throw new Exception("BlockCompressedReader: could not read a block");
                    }
                    if(null == b || 0 == b.blockLength) {
                        b = null;
                        break;
                    }

                    // add it to the queue
                    //System.err.println("Reader: adding");
                    if(!this.reader.input.add(b)) {
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
