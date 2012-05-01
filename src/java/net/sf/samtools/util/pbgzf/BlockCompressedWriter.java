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
import net.sf.samtools.util.BinaryCodec;
import net.sf.samtools.util.BlockCompressedStreamConstants;
import net.sf.samtools.util.BlockCompressedFilePointerUtil;

/**
 * Gets block from a queue, and writes them to stream.  Unlike a Thread, this class can be 
 * started and joined multiple times.
 *
 */
public class BlockCompressedWriter {
    /**
     * The underlying writer thread.
     */
    private BlockCompressedWriterThread writerThread = null;

    /**
     * The codec used to write the data.
     */
    private final BinaryCodec codec;

    /**
     * The block offset of the last block written.
     */
    private int mBlockOffset = 0;

    /**
     * The block address of the last block written.
     */
    private long mBlockAddress = 0;

    /**
     * The otuput queue from which to get the blocks to write.
     */
    public BlockCompressedQueue queue= null;
    
    /**
     * True if the writer has written all blocks, false otherwise.
     */
    public boolean isDone =  false;
    
    /**
     * True if the writer has been closed, false otherwise.
     */
    public boolean isClosed = false;

    /**
     * The number of blocks written.
     */
    private long n = 0;

    /**
     * Creates a new writer.
     * @param codec the codec to which to write.
     * @param queue the queue from which to retrieve blocks.
     */
    public BlockCompressedWriter(BinaryCodec codec, BlockCompressedQueue queue) {
        this.codec = codec;
        this.queue = queue;
    }

    /**
     * Writes the given GZIP block, adding the header.
     * @param block the deflated block to write.
     */
    private int writeGzipBlock(final BlockCompressed block) {
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
    
    /**
     * Starts the underlying writer thread.
     */
    public void start() {
        this.writerThread = new BlockCompressedWriterThread(this);
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    /**
     * Joins the underlying writer thread.
     */
    public void join() {
        try {
            this.writerThread.join();
            this.writerThread = null;
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Interrupts the writer thread.
     */
    public void interrupt() {
        this.writerThread.interrupt();
    }
        
    /**
     * @return true if the underlying writer thread is trying to receive blocks, false otherwise.
     */
    public boolean isGetting() {
        return this.writerThread.isGetting();
    }

    /**
     * The underlying writer thread.
     */
    protected class BlockCompressedWriterThread extends Thread {
        /**
         * The writer associated with this thread.
         */
        BlockCompressedWriter writer = null;

        /** 
         * True if the thread is trying to retreive blocks, false otherwise.
         */
        boolean getting = false;

        /**
         * @param the writer associated with this thread.
         */
        public BlockCompressedWriterThread(BlockCompressedWriter writer) {
            this.writer = writer;
        }

        /**
         * Runs the writer.
         */
        public void run()
        {
            BlockCompressed b = null;
            boolean wait = true;

            try {
                //System.err.println("Writer: starting");
                while(!this.writer.isDone) {
                    // get a block
                    //System.err.println("Writer: getting a block from the output");
                    getting = true;
                    b = this.writer.queue.getOutput(false);
                    if(this.isInterrupted()) {
                        //System.err.println("Writer: was interrupted");
                        break;
                    }
                    else if(null == b) {
                        //System.err.println("Writer: getting a null block");
                        break;
                    }
                    getting = false;
                    //System.err.println("Writer: got a block from the output id=" + b.id);

                    // Wait for the block to be consumed
                    b.getConsumed();

                    // write the block
                    //System.err.println("Writer: writing block id=" + b.id + " b.blockLength=" + b.blockLength + " b.priorLength=" + b.priorLength);
                    int totalBlockSize = this.writer.writeGzipBlock(b);
                    b.setBlockAddress(this.writer.mBlockAddress);
                    this.writer.mBlockAddress += totalBlockSize;
                    this.writer.mBlockOffset = b.blockOffset;
                    b = null;

                    this.writer.n++;
                }
            } catch(InterruptedException e) {
                // ignore
            } catch(Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            this.writer.isDone = true;
            //System.err.println("Writer: done, blocks written n=" + this.writer.n);
        }

        /**
         * @return true if the writer is trying to retrieve blocks, false otherwise.
         */
        public boolean isGetting() {
            return this.getting;
        }
    }

    /**
     * Resets the writer.
     * @param queue the new queue.
     */
    public void reset(BlockCompressedQueue queue) {
        this.isDone = this.isClosed = false;
        this.queue = queue;
    }

    /**
     * Closes the writer.
     */
    public void close() throws IOException {
        this.flush();
        this.codec.writeBytes(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK); // write the last empty gzip block
        this.codec.close(); // close the output codec
        this.queue = null;
    }

    /**
     * Flushes the underlying codec.
     */
    public void flush() throws IOException {
        this.codec.getOutputStream().flush();
    }

    /**
     * @return the block address of the last block written.
     */
    public long getBlockAddress() {
        return this.mBlockAddress;
    }

    /**
     * @return a file pointer to the last block written.
     */
    public long getFilePointer() {
        return BlockCompressedFilePointerUtil.makeFilePointer(this.mBlockAddress, this.mBlockOffset);
    }
}
