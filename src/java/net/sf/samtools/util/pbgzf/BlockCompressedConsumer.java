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

import java.lang.Thread;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import net.sf.samtools.util.BlockGunzipper;

/**
 * The main class to compress and decompress blocks.  Unlike a Thread, this class can be 
 * started and joined multiple times.
 */
public class BlockCompressedConsumer {
    /**
     * The queue from which to retrieve and put blocks.
     */
    private BlockCompressedConsumerQueue queue = null;

    /**
     * The underlying thread to process the blocks.
     */
    private BlockCompressedConsumerThread consumerThread = null;

    /**
     * The default time to sleep waiting for blocks.
     */
    private static final long THREAD_SLEEP = 10; 
    
    /**
     * A block for temporary buffer.
     */
    private BlockCompressed block = null;

    /**
     * True if the consumer has completed all tasks and will do no more consuming, false otherwise.
     */
    private boolean isDone;

    /**
     * The consumer id.
     */
    private int cid;

    /**
     * The number of blocks consumed.
     */
    private long n;

    /**
     * True to locally buffer blocks into a pool, false otherwise.
     */
    private boolean usePools = false;

    /**
     * A deflator with no compresion.
     */
    private final Deflater noCompressionDeflater = new Deflater(Deflater.NO_COMPRESSION, true);

    /**
     * The deflators for various compression levels.
     */
    private List<Deflater> deflaters = new ArrayList<Deflater>();

    /**
     * The CRC coded checker.
     */
    private final CRC32 crc32 = new CRC32();

    /**
     * The decompressor.
     */
    private final BlockGunzipper blockGunzipper = new BlockGunzipper();

    /**
     * Creates a new consumer ready to consume.
     * @param queue the queue from which to retrieve and add blocks.
     * @param cid the consumer id.
     */
    public BlockCompressedConsumer(BlockCompressedConsumerQueue queue, int cid)
    {
        int i;
        this.queue = queue;
        this.cid = cid;
        this.block = new BlockCompressed(-1);
        this.deflaters = new ArrayList<Deflater>();
        for(i=-1;i<=9;i++) { // -1...9
            this.deflaters.add(new Deflater(i, true));
        }
    }

    /**
     * Determines whether or not the inflater will re-calculated the CRC on the decompressed data
     * and check it against the value stored in the GZIP header.  CRC checking is an expensive
     * operation and should be used accordingly.
     * @param check true to set CRC checking, false otherwise.
     */
    public void setCheckCrcs(final boolean check) {
        this.blockGunzipper.setCheckCrcs(check);
    }

    private int unpackInt16(final byte[] buffer, final int offset) {
        return ((buffer[offset] & 0xFF) |
                ((buffer[offset+1] & 0xFF) << 8));
    }

    private int unpackInt32(final byte[] buffer, final int offset) {
        return ((buffer[offset] & 0xFF) |
                ((buffer[offset+1] & 0xFF) << 8) |
                ((buffer[offset+2] & 0xFF) << 16) |
                ((buffer[offset+3] & 0xFF) << 24));
    }

    /**
     * Inflates the block. Stores the previous block length in block.priorLength.
     * @param block the block to inflate.
     * @return true if successful, false otherwise.
     */
    private boolean inflateBlock(BlockCompressed block)
        throws IOException
    {
        byte[] tmpBuffer = null;
        
        block.priorLength = block.blockLength;

        // swap the block buffer into the consumer buffer
        // NB: consumer buffer stores the compressed data, uncompressed data will go in the block buffer
        tmpBuffer = this.block.buffer;
        this.block.buffer = block.buffer; 
        block.buffer = tmpBuffer;
        this.block.blockLength = block.blockLength; // needed for the unziper 

        final int uncompressedLength = unpackInt32(this.block.buffer, block.blockLength-4);
        
        if(block.buffer == null || uncompressedLength < 0) {
            throw new RuntimeException("BGZF file has invalid uncompressedLength: " + uncompressedLength);
        }
        blockGunzipper.unzipBlock(block.buffer, this.block.buffer, this.block.blockLength);
        
        block.blockLength = uncompressedLength;

        return true;
    }

    /**
     * Deflates the block. Stores the previous block length in block.priorLength and the CRC is updated.
     * @param block the block to deflate.
     * @param deflater the deflater.
     * @return true if successful, false otherwise.
     */
    private boolean deflateBlock(BlockCompressed block, Deflater deflater)
    {
        if(0 == block.blockLength) return true; // no bytes to compress
        
        int bytesToCompress = block.blockLength; 
        block.priorLength = block.blockLength;
        
        
        byte[] uncompressedBuffer = block.buffer; // input
        byte[] compressedBuffer = this.block.buffer; // output
        
        // Compress the input
        deflater.reset();
        deflater.setInput(uncompressedBuffer, 0, bytesToCompress);
        deflater.finish();
        int compressedSize = deflater.deflate(compressedBuffer, 0, compressedBuffer.length);

        // If it didn't all fit in compressedBuffer.length, set compression level to NO_COMPRESSION
        // and try again.  This should always fit.
        if (!deflater.finished()) {
            noCompressionDeflater.reset();
            noCompressionDeflater.setInput(uncompressedBuffer, 0, bytesToCompress);
            noCompressionDeflater.finish();
            compressedSize = noCompressionDeflater.deflate(compressedBuffer, 0, compressedBuffer.length);
            if (!noCompressionDeflater.finished()) {
                throw new IllegalStateException("impossible");
            }
        }
        // Data compressed small enough, so write it out.
        crc32.reset();
        crc32.update(uncompressedBuffer, 0, bytesToCompress);

        block.crc = crc32.getValue();

        // update the block length
        block.blockLength = compressedSize;
        
        // swap the block buffer into the consumer buffer
        byte[] tmpBuffer = this.block.buffer;
        this.block.buffer = block.buffer; 
        block.buffer = tmpBuffer;

        // NB: coudl we not set the blockAddress here?

        //assert(bytesToCompress <= numUncompressedBytes);

        return true;
    }

    /**
     * Starts the underlying consumer thread.
     */
    public void start() {
        this.consumerThread = new BlockCompressedConsumerThread(this);
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
    }

    /**
     * Joins the underlying consumer thread.
     */
    public void join() {
        try {
            this.consumerThread.join();
            this.consumerThread = null;
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * The underlying consumer thread.
     */
    protected class BlockCompressedConsumerThread extends Thread {
        /**
         * The consumer to which this thread belongs.
         */
        private BlockCompressedConsumer consumer = null;

        /**
         * Creates a new consumer thread.
         * @param consumer the consumer to which this thread belongs.
         */
        public BlockCompressedConsumerThread(BlockCompressedConsumer consumer) {
            this.consumer = consumer;
        }

        /**
         * Runs the consumer.
         */
        public void run()
        {
            BlockCompressed b = null;
            BlockCompressedPool poolIn = null;
            BlockCompressedPool poolOut = null;
            boolean wait;

            try {

                this.consumer.n = 0;
                poolIn = new BlockCompressedPool(false);
                poolOut = new BlockCompressedPool(false);

                while(!this.consumer.isDone) {
                    if(usePools) {
                        // get block(s)
                        while(poolIn.n < poolIn.m) { // more to read in
                            if(0 < this.consumer.queue.drainTo(poolIn, poolIn.m - poolIn.n, (0 == poolIn.n && 0 == poolOut.n))) {
                                break;
                            }
                        }
                        if(0 == poolIn.n && 0 == poolOut.n) { // no more data
                            //break; // EOF
                            Thread.currentThread().sleep(THREAD_SLEEP);//sleep for 10 ms
                        }

                        // inflate/deflate
                        while(0 < poolIn.n && poolOut.n < poolIn.m) { // consume while the in has more and the out has room
                            b = poolIn.peek();
                            if(null == b) {
                                throw new Exception("Bug encountered");
                            }
                            if(b.compress) {
                                if(!deflateBlock(b, this.consumer.deflaters.get(b.compressLevel+1))) {
                                    throw new Exception("Bug encountered");
                                }
                            }
                            else {
                                if(!inflateBlock(b)) {
                                    throw new Exception("Bug encountered");
                                }
                            }
                            if(!poolOut.add(b)) {
                                throw new Exception("Bug encountered");
                            }
                            poolIn.get(); // ignore return
                            b = null;
                        }

                        // put back a block
                        while(0 < poolOut.n) {
                            b = poolOut.peek();
                            // NB: only wait if the pools are full
                            wait = (poolIn.m == poolIn.n && poolOut.m == poolOut.n) ? true : false;
                            if(!this.consumer.queue.add(b, wait)) {
                                break; 
                            }
                            poolOut.get(); // ignore return
                            b = null;
                            this.consumer.n++;
                        }
                    }
                    else {
                        // get block
                        //System.err.println("Consumer #" + this.consumer.cid + " getting block");
                        b = this.consumer.queue.get(true);
                        if(null == b) {
                            //break;
                            Thread.currentThread().sleep(THREAD_SLEEP);//sleep for 10 ms
                        }

                        // inflate/deflate
                        if(b.compress) {
                            //System.err.println("Consumer #" + this.consumer.cid + " deflate");
                            if(!deflateBlock(b, this.consumer.deflaters.get(b.compressLevel+1))) {
                                throw new Exception("Bug encountered");
                            }
                        }
                        else {
                            //System.err.println("Consumer #" + this.consumer.cid + " inflate");
                            if(!inflateBlock(b)) {
                                throw new Exception("Bug encountered");
                            }
                        }

                        // put back a block
                        //System.err.println("Consumer #" + this.consumer.cid + " adding a block with id: " + b.id);
                        if(!this.consumer.queue.add(b, true)) {
                            // ignore
                            //break; // EOF
                        }
                        b = null;
                        this.consumer.n++;
                    }
                }
                //System.err.println("Consumer Done #" + this.consumer.cid + " n=" + this.consumer.n);

                this.consumer.isDone = true;
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
    
    /**
     * Set this consumer thread to be done.
     */
    public void setDone() {
        this.isDone = true;
    }

    /**
     * Reset the state of this consumer.
     */
    public void reset() {
        this.isDone = false;
    }
}
