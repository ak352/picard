/*
 * The MIT License
 *
 * Copyright (c) 2010 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sub-license, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.sf.samtools;

import net.sf.samtools.util.AbstractAsyncWriter;
import net.sf.samtools.util.mt.ParallelBlockCompressedOutputStream.DelayedFilePointer;
import net.sf.samtools.util.BlockCompressedStreamConstants;
import net.sf.samtools.util.BlockCompressedFilePointerUtil;

/**
 * Asynchronous BAMIndexer that stores records in a queue until
 * their file positions have been determined by the associated
 * ParallelBlockCompressedOutputStream.
 *
 * Exceptions experienced by the writer thread will be thrown back to
 * the caller in subsequent calls to either processAlignment() or finish().
 *
 * @author Fred Long
 */
class AsyncBAMIndexer extends AbstractAsyncWriter<AsyncSAMRecord> {
    private final BAMIndexer underlyingWriter;
    // Smallest possible BAM record size.  Do not change this.
    private static final int MIN_RECORD_SIZE = 38;
    // Queue enough SAMRecords to cover a minimum number of BGZF blocks.
    private static final int MIN_BLOCK_COVER = 10;
    private static final int BLOCK_SIZE = BlockCompressedStreamConstants.DEFAULT_UNCOMPRESSED_BLOCK_SIZE;
    private static final int QUEUE_SIZE = BLOCK_SIZE / MIN_RECORD_SIZE * MIN_BLOCK_COVER;

    /**
     * Creates an AsyncBAMIndexer wrapping the provided BAMIndexer.
     */ 
    public AsyncBAMIndexer(final BAMIndexer out) {
        // Allocate a queue large enough to hold all the SAMRecords contained in at least 4 BGZF blocks.
        // At a minimum, we must be able to cover at least one block to avoid deadlock, because this.synchronouslyWrite()
        // must wait until AsyncBlockCompressedOutputStream has written the current block.  The number 38 below is
        // the minimum possible size of a BAM record.
        super(QUEUE_SIZE);
        this.underlyingWriter = out;
    }

    @Override protected void synchronouslyWrite(final AsyncSAMRecord item) {
        // wait for the block to be written, then update the file pointer
        long start = item.start.toLong();
        long end = item.end.toLong();
        // set the alignment's SourceInfo and then prepare its index information
        item.record.setFileSource(new SAMFileSource(null, new BAMFileSpan(new Chunk(start, end))));
        this.underlyingWriter.processAlignment(item.record);
    }

    @Override protected void synchronouslyClose() { this.underlyingWriter.finish(); }

    @Override protected final String getThreadNamePrefix() { return "BAMIndexerThread-"; }

    /**
     * Adds an alignment to the write queue.  This method needs to be called by BAMFileWriter
     * before another AsyncBlockCompressedOutputStream.write() is performed.
     */
    public void processAlignment(final SAMRecord alignment, DelayedFilePointer start, DelayedFilePointer end) {
        AsyncSAMRecord asyncRecord = new AsyncSAMRecord(alignment, start, end);
        write(asyncRecord);
    }

    /**
     * After all the alignment records have been processed, finish is called.
     * Writes any final information and closes the output file.
     */
    public void finish() {
        close();
    }
}

class AsyncSAMRecord {
    public SAMRecord record;
    public DelayedFilePointer start, end;
    public AsyncSAMRecord(SAMRecord record, DelayedFilePointer start, DelayedFilePointer end) {
        this.record = record;
        this.start = start;
        this.end = end;
    }
}
