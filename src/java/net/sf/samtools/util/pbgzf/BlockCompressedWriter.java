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

public class BlockCompressedWriter {
    private BlockCompressedWriterThread writerThread = null;
    private int id;

    private final BinaryCodec codec;

    // TODO: are these necessary?
    private int blockOffset = 0;
    private int mBlockOffset = 0;
    private long mBlockAddress = 0;

    public BlockCompressedBlockingQueue output = null;
    public boolean isDone =  false;
    public boolean isClosed = false;
    public BlockCompressedPool pool = null;
            
    private long n = 0;

    public BlockCompressedWriter(BinaryCodec codec, BlockCompressedBlockingQueue output, int id) {
        this.codec = codec;
        this.output = output;
        this.id = id;
    }

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
    
    public void start() {
        this.writerThread = new BlockCompressedWriterThread(this);
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    public void join() {
        try {
            this.writerThread.join();
            this.writerThread = null;
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void interrupt() {
        this.writerThread.interrupt();
    }
        
    public boolean isGetting() {
        return this.writerThread.isGetting();
    }

    protected class BlockCompressedWriterThread extends Thread {
        BlockCompressedWriter writer = null;
        boolean getting = false;

        public BlockCompressedWriterThread(BlockCompressedWriter writer) {
            this.writer = writer;
        }

        public void run()
        {
            BlockCompressed b = null;
            boolean wait = true;
            long prevId = -1;

            try {
                //System.err.println("Writer: starting");
                while(!this.writer.isDone) {
                    // TODO: use the local pool

                    // get a block
                    //System.err.println("Writer: getting a block from the output");
                    getting = true;
                    b = this.writer.output.get(this.writer.id);
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

                    if(b.id != prevId + 1) {
                        throw new Exception("blocks retrieved out of order! previous:" + prevId + " current:" + b.id);
                    }
                    prevId = b.id;

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

        public boolean isGetting() {
            return this.getting;
        }
    }

    public void reset(int i) {
        this.isDone = this.isClosed = false;
        this.id = id;
    }

    public void close() throws IOException {
        this.flush();
        this.codec.writeBytes(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK); // write the last empty gzip block
        this.codec.close(); // close the output codec
        this.output = null;
        this.pool = null;
    }

    public void flush() throws IOException {
        this.codec.getOutputStream().flush();
    }

    public long getBlockAddress() {
        return this.mBlockAddress;
    }

    public long getFilePointer() {
        return BlockCompressedFilePointerUtil.makeFilePointer(this.mBlockAddress, this.mBlockOffset);
    }
}
