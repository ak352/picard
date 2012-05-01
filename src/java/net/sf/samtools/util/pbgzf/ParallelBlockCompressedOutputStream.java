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
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.List;
import java.util.ArrayList;

import net.sf.samtools.util.BlockCompressedOutputStream;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedStreamConstants;

/**
 * BlockCompressedWriter for a file that is a series of gzip blocks (BGZF format).  The caller just treats it as an
 * OutputStream, and under the covers a gzip block is written when the amount of uncompressed as-yet-unwritten
 * bytes reaches a threshold.
 *
 * The advantage of BGZF over conventional gzip is that BGZF allows for seeking without having to scan through
 * the entire file up to the position being sought.
 *
 * Note that the flush() method should not be called by client
 * unless you know what you're doing, because it forces a gzip block to be written even if the
 * number of buffered bytes has not reached threshold.  close(), on the other hand, must be called
 * when done writing in order to force the last gzip block to be written.
 *
 * c.f. http://samtools.sourceforge.net/SAM1.pdf for details of BGZF file format.
 */
public class ParallelBlockCompressedOutputStream
        extends BlockCompressedOutputStream
{
    private BlockCompressedWriter writer = null;
    private BlockCompressedMultiQueue multiQueue = null;
    private BlockCompressedQueue blockQueue = null;
    private BlockCompressed block = null;

    private File file = null;

    private int compressionLevel;

    private void init(int compressionLevel)
    {
        int i;
        this.compressionLevel = compressionLevel;
        // get the queue
        this.multiQueue = BlockCompressedMultiQueue.getInstance();
        // get the queue for this stream
        this.blockQueue = this.multiQueue.register();
        // create the writer
        this.writer = new BlockCompressedWriter(codec, this.blockQueue);
        // start the writer
        this.writer.start();
    }

    /**
     * Uses default compression level, which is 5 unless changed by setDefaultCompressionLevel
     */
    public ParallelBlockCompressedOutputStream(final String filename) {
        super(filename);
        this.init(defaultCompressionLevel);
    }

    /**
     * Uses default compression level, which is 5 unless changed by setDefaultCompressionLevel
     */
    public ParallelBlockCompressedOutputStream(final File file) {
        super(file);
        this.init(defaultCompressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public ParallelBlockCompressedOutputStream(final String filename, final int compressionLevel) {
        super(filename, compressionLevel);
        this.init(compressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public ParallelBlockCompressedOutputStream(final File file, final int compressionLevel) {
        super(file, compressionLevel);
        this.init(compressionLevel);
    }

    /**
     * Constructors that take output streams
     * file may be null
     */
    public ParallelBlockCompressedOutputStream(final OutputStream os, File file) {
        this(os, file, defaultCompressionLevel);
    }

    public ParallelBlockCompressedOutputStream(final OutputStream os, final File file, final int compressionLevel) {
        super(os, file);
        this.init(compressionLevel);
    }

    /**
     * Writes b.length bytes from the specified byte array to this output stream. The general contract for write(b)
     * is that it should have exactly the same effect as the call write(b, 0, b.length).
     * @param bytes the data
     */
    @Override
    public void write(final byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
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
            try {
                if(null == this.block) {
                    this.block = new BlockCompressed(this.compressionLevel, true);
                }

                while (numBytes > 0) {
                    final int bytesToWrite = Math.min(this.block.blockLength - this.block.blockOffset, numBytes);
                    System.arraycopy(bytes, startIndex, this.block.buffer, this.block.blockOffset, bytesToWrite);
                    this.block.blockOffset += bytesToWrite;
                    startIndex += bytesToWrite;
                    numBytes -= bytesToWrite;
                    assert(0 <= numBytes);
                    if(this.block.blockLength == this.block.blockOffset) {
                        this.blockQueue.add(this.block);
                        this.block = new BlockCompressed(this.compressionLevel, true);
                    }
                }
            } catch(InterruptedException e) {
                throw new IOException(e);
            }
        }

    private void flush(boolean restart) throws IOException {
        try {
            // add the current block to the queue
            if(null != this.block && 0 < this.block.blockOffset) {
                this.block.blockLength = this.block.blockOffset;
                this.blockQueue.add(this.block);
            }
            this.block = null;

            // wait until the queue is empty
            this.blockQueue.waitUntilEmpty();

            // wait until the write has finished writing is blocking on a wait
            while(!this.writer.isGetting()) {
                try {
                    Thread.currentThread().sleep(100);
                } catch(InterruptedException ie) {
                    // do nothing
                }
            }
            
            // interrupt the writer
            this.writer.interrupt();

            // join the writer
            this.writer.join();
            
            // flush in the writer
            this.writer.flush();

            // close the queue
            this.multiQueue.deregister(this.blockQueue);
            
            // restart 
            if(restart) {
                // re-register
                this.blockQueue = this.multiQueue.register();

                // new block
                this.block = new BlockCompressed(this.compressionLevel, true);

                // reset the writer
                this.writer.reset(this.blockQueue);

                // start the writer
                this.writer.start();
            }
        } catch(InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * WARNING: flush() affects the output format, because it causes the current contents of uncompressedBuffer
     * to be compressed and written, even if it isn't full.  Unless you know what you're doing, don't call flush().
     * Instead, call close(), which will flush any unwritten data before closing the underlying stream.
     *
     */
    @Override
    public void flush() throws IOException {
        this.flush(true); // restart too
    }

    /**
     * close() must be called in order to flush any remaining buffered bytes.  An unclosed file will likely be
     * defective.
     *
     */
    @Override
    public void close() throws IOException {
        // flush the data
        this.flush(false); // do not restart
        
        // write an empty GZIP block directly to the codec
        this.codec.writeBytes(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
        
        // close
        this.writer.close();
        
        // Can't re-open something that is not a regular file, e.g. a named pipe or an output stream
        if (this.file == null || !this.file.isFile()) return;
        if (BlockCompressedInputStream.checkTermination(this.file) !=
                BlockCompressedInputStream.FileTermination.HAS_TERMINATOR_BLOCK) {
            throw new IOException("Terminator block not found after closing BGZF file " + this.file);
        }
    }

    /**
     * Writes the specified byte to this output stream. The general contract for write is that one byte is written
     * to the output stream. The byte to be written is the eight low-order bits of the argument b.
     * The 24 high-order bits of b are ignored.
     * @param bite
     * @throws IOException
     */
    public void write(final int bite) throws IOException {
        singleByteArray[0] = (byte)bite;
        write(singleByteArray);
    }

    /** Encode virtual file pointer
     * Upper 48 bits is the byte offset into the compressed stream of a block.
     * Lower 16 bits is the byte offset into the uncompressed stream inside the block.
     */
    public long getFilePointer() {
        throw new Error("coding error: getFilePointer() is illegal in multi-threaded mode; use getDelayedFilePointer() instead");
    }

    public DelayedFilePointer getDelayedFilePointer() {
        return new DelayedFilePointer(this.block, this.block.blockOffset);
    }
}
