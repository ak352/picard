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

import java.io.ByteArrayInputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.lang.reflect.Method;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMUtils;
import net.sf.samtools.SAMValidationError;
import net.sf.samtools.util.BlockGunzipper;

import static net.sf.samtools.util.BlockCompressedStreamConstants.*;
import static net.sf.samtools.util.BlockCompressedFilePointerUtil.makeFilePointer;

/**
   A Worker thread receives Jobs on its input queue and executes them.
   <p> Currently, three subclasses of {@link Job} can be processed: {@link 
   CompressedBlock}, {@link GroupJob}, and {@link RunnableJob}.  If you
   create a new subclass of Job that is unknown to the Worker class, you
   will need to update the source code for the Worker class in order to
   handle the new type of Job.
   <p> The inflateBlock() and deflateBlock() methods are hosted by
   the Worker threads instead of the Jobs because Worker threads are
   persistent and can reuse the same set of objects for compression and
   decompression without having to recreate them for each Job.
 */
class Worker extends Thread {
    /**
     * The input queue for retrieving new Jobs.
     */
    private BlockingQueue<Job> queue = null;

    /**
     * A Deflater with no compresion.
     */
    private final Deflater noCompressionDeflater = new Deflater(Deflater.NO_COMPRESSION, true);

    /**
     * Deflaters for various compression levels.
     */
    private List<Deflater> deflaters = new ArrayList<Deflater>();

    /**
     * The CRC checker.
     */
    private final CRC32 crc32 = new CRC32();

    /**
     * The decompressor.
     */
    private final BlockGunzipper blockGunzipper = new BlockGunzipper();

    private final int id; // thread identifier

    /**
     * Creates a new worker thread
     * @param queue the job queue
     */
    static int threadCount = 0;
    public Worker(BlockingQueue<Job> queue) {
        super("Worker-" + threadCount); // give thread a name
        id = threadCount++;
        this.queue = queue;
        this.deflaters = new ArrayList<Deflater>();
        for (int i = -1; i <= 9; i++) { // -1...9
            this.deflaters.add(new Deflater(i, true));
        }
    }

    private static int unpackInt32(final byte[] buffer, final int offset) {
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
    private void inflateBlock(CompressedBlock block) {
        if (block == null || block.isCancelled()) return;
        if (block.blockLength == -1) { // EOF
            block.setFinished(true);
            return;
        }
        blockGunzipper.setCheckCrcs(block.checkCrcs);
        block.priorLength = block.blockLength;
        final int uncompressedLength = unpackInt32(block.buffer, block.blockLength-4);
        if (uncompressedLength < 0) {
            throw new RuntimeException("BGZF block has invalid uncompressedLength: " + uncompressedLength);
        }
        byte[] tmpBuffer = MultiThreading.getBgzfBuffer();
        blockGunzipper.unzipBlock(tmpBuffer, block.buffer, block.blockLength);
        MultiThreading.recycleBgzfBuffer(block.buffer);
        block.buffer = tmpBuffer;
        block.blockLength = uncompressedLength;
        block.setFinished(true);
    }

    /**
     * Deflates the block. Stores the previous block length in block.priorLength and the CRC is updated.
     * @param block the block to deflate.
     * @param deflater the deflater.
     * @return true if successful, false otherwise.
     */
    private void deflateBlock(CompressedBlock block) {
        if (block == null || block.isCancelled()) return;
        Deflater deflater = deflaters.get(block.compressLevel + 1);
        int bytesToCompress = block.blockLength;
        block.priorLength = block.blockLength;
        byte[] uncompressedBuffer = block.buffer; // input
        byte[] compressedBuffer = MultiThreading.getBgzfBuffer();
        // Compress the input
        deflater.reset();
        deflater.setInput(uncompressedBuffer, 0, bytesToCompress);
        deflater.finish();
        int compressedSize = deflater.deflate(compressedBuffer, 0, compressedBuffer.length);
        // If the compressed block does not fit because it is bigger than the
        // original block, then set compression level to NO_COMPRESSION
        // and compress again. This should always work.
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
        // recycle old buffer
        MultiThreading.recycleBgzfBuffer(block.buffer);
        block.buffer = compressedBuffer;
        // store compressed buffer
        block.setFinished(true); // block is ready
    }

    void runJob(Job job) {
        if (job == null || job.isCancelled()) return;
        try {
            if (job instanceof RunnableJob) {
                RunnableJob r = (RunnableJob) job;
                r.run();
            }
            else if (job instanceof CompressedBlock) {
                CompressedBlock block = (CompressedBlock) job;
                if (block.compress) deflateBlock(block);
                else inflateBlock(block);
            }
            else if (job instanceof GroupJob) {
                GroupJob j = (GroupJob) job;
                for (Job k : j.jobs) runJob(k);
                j.setFinished(true);
            }
            else throw new Error("coding error: unknown Job type");
        }
        catch (Throwable t) {
            // tell parent thread, but don't exit
            job.setThrowable(t);
        }
    }

    /**
     * Runs the Worker thread.
     */
    public void run() {
        while (!this.interrupted()) {
            // get the next block
            Job job = null;
            try {
                job = queue.take();
            }
            catch (InterruptedException e) {
                // somebody told us to stop
                break;
            }
            if (job.isCancelled()) continue;
            runJob(job);
        }
    }
}
