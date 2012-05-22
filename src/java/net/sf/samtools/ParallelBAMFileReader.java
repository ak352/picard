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
package net.sf.samtools;

import net.sf.samtools.util.*;
import net.sf.samtools.util.mt.*;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import static net.sf.samtools.util.mt.MultiThreading.debug;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Parallel version of {@link BAMFileReader}.
 */
class ParallelBAMFileReader extends BAMFileReader {

    // Allow Reader thread to pass errors up to us
    private final AtomicReference<Throwable> ex = new AtomicReference<Throwable>(null);

    // The Reader thread
    private Reader reader = null;

    // Used by BamDecodeJob to readFully() the encoded BAMRecord
    private DataInputStream dataInputStream;

    /**
     * Prepare to read BAM from a stream (not seekable)
     * @param stream source of bytes.
     * @param eagerDecode if true, decode all BAM fields as reading rather than lazily.
     * @param validationStringency Controls how to handle invalidate reads or header lines.
     */
    ParallelBAMFileReader(final InputStream stream,
                  final File indexFile,
                  final boolean eagerDecode,
                  final ValidationStringency validationStringency,
                  final SAMRecordFactory factory)
        throws IOException {
        super(stream, indexFile, eagerDecode, validationStringency, factory);
        finish();
    }

    /**
     * Prepare to read BAM from a file (seekable)
     * @param file source of bytes.
     * @param eagerDecode if true, decode all BAM fields as reading rather than lazily.
     * @param validationStringency Controls how to handle invalidate reads or header lines.
     */
    ParallelBAMFileReader(final File file,
                  final File indexFile,
                  final boolean eagerDecode,
                  final ValidationStringency validationStringency,
                  final SAMRecordFactory factory)
        throws IOException {
        super(file, indexFile, eagerDecode, validationStringency, factory);
        finish();
    }

    /**
     * Prepare to read BAM from a SeekableStream.
     * @param strm the SeekableStream.
     * @param eagerDecode if true, decode all BAM fields as reading rather than lazily.
     * @param validationStringency Controls how to handle invalidate reads or header lines.
     */
    ParallelBAMFileReader(final SeekableStream strm,
                  final File indexFile,
                  final boolean eagerDecode,
                  final ValidationStringency validationStringency,
                  final SAMRecordFactory factory)
        throws IOException {
        super(strm, indexFile, eagerDecode, validationStringency, factory);
        finish();
    }

    private void finish() {
        debug("ParallelBAMFileReader created, queue size %d, buffer size %d\n",
            MultiThreading.bamDecodeQueueSize(), MultiThreading.bamDecodeBufferSize());
        dataInputStream = new DataInputStream(mCompressedInputStream);
    }

    @Override
    void close() {
        reader.close();
        MultiThreading.checkAndRethrow(ex);
        super.close();
    }

    /**
     * Prepare to iterate through the SAMRecords in file order.
     * Only a single iterator on a BAM file can be extant at a time.  If getIterator() or a query method has been called once,
     * that iterator must be closed before getIterator() can be called again.
     * A somewhat peculiar aspect of this method is that if the file is not seekable, a second call to
     * getIterator() begins its iteration where the last one left off.  That is the best that can be
     * done in that situation.
     */
    @Override
    CloseableIterator<SAMRecord> getIterator() {
        MultiThreading.checkAndRethrow(ex);
        if (mStream == null) {
            throw new IllegalStateException("File reader is closed");
        }
        if (mCurrentIterator != null) {
            throw new IllegalStateException("Iteration in progress");
        }
        if (mIsSeekable) {
            try {
                this.seek(mFirstRecordPointer);
            } catch (IOException exc) {
                throw new RuntimeException(exc.getMessage(), exc);
            }
        }
        mCurrentIterator = new BAMFileIterator();
        return mCurrentIterator;
    }

    @Override
    CloseableIterator<SAMRecord> getIterator(final SAMFileSpan chunks) {
        MultiThreading.checkAndRethrow(ex);
        if (mStream == null) {
            throw new IllegalStateException("File reader is closed");
        }
        if (mCurrentIterator != null) {
            throw new IllegalStateException("Iteration in progress");
        }
        if (!(chunks instanceof BAMFileSpan)) {
            throw new IllegalStateException("ParallelBAMFileReader cannot handle this type of file span.");
        }

        // Create an iterator over the given chunk boundaries.
        mCurrentIterator = new BAMFileIndexIterator(((BAMFileSpan)chunks).toCoordinateArray());
        return mCurrentIterator;
    }

    /**
     * Prepare to iterate through the SAMRecords that match the given interval.
     * Only a single iterator on a BAMFile can be extant at a time.  The previous one must be closed
     * before calling any of the methods that return an iterator.
     *
     * Note that an unmapped SAMRecord may still have a reference name and an alignment start for sorting
     * purposes (typically this is the coordinate of its mate), and will be found by this method if the coordinate
     * matches the specified interval.
     *
     * Note that this method is not necessarily efficient in terms of disk I/O.  The index does not have perfect
     * resolution, so some SAMRecords may be read and then discarded because they do not match the specified interval.
     *
     * @param sequence Reference sequence sought.
     * @param start Desired SAMRecords must overlap or be contained in the interval specified by start and end.
     * A value of zero implies the start of the reference sequence.
     * @param end A value of zero implies the end of the reference sequence.
     * @param contained If true, the alignments for the SAMRecords must be completely contained in the interval
     * specified by start and end.  If false, the SAMRecords need only overlap the interval.
     * @return Iterator for the matching SAMRecords
     */
    @Override
    CloseableIterator<SAMRecord> query(final String sequence, final int start, final int end, final boolean contained) {
        MultiThreading.checkAndRethrow(ex);
        if (mStream == null) {
            throw new IllegalStateException("File reader is closed");
        }
        if (mCurrentIterator != null) {
            throw new IllegalStateException("Iteration in progress");
        }
        if (!mIsSeekable) {
            throw new UnsupportedOperationException("Cannot query stream-based BAM file");
        }
        mCurrentIterator = createIndexIterator(sequence, start, end, contained? QueryType.CONTAINED: QueryType.OVERLAPPING);
        return mCurrentIterator;
    }

    /**
     * Prepare to iterate through the SAMRecords with the given alignment start.
     * Only a single iterator on a BAMFile can be extant at a time.  The previous one must be closed
     * before calling any of the methods that return an iterator.
     *
     * Note that an unmapped SAMRecord may still have a reference name and an alignment start for sorting
     * purposes (typically this is the coordinate of its mate), and will be found by this method if the coordinate
     * matches the specified interval.
     *
     * Note that this method is not necessarily efficient in terms of disk I/O.  The index does not have perfect
     * resolution, so some SAMRecords may be read and then discarded because they do not match the specified interval.
     *
     * @param sequence Reference sequence sought.
     * @param start Alignment start sought.
     * @return Iterator for the matching SAMRecords.
     */
    @Override
    CloseableIterator<SAMRecord> queryAlignmentStart(final String sequence, final int start) {
        MultiThreading.checkAndRethrow(ex);
        if (mStream == null) {
            throw new IllegalStateException("File reader is closed");
        }
        if (mCurrentIterator != null) {
            throw new IllegalStateException("Iteration in progress");
        }
        if (!mIsSeekable) {
            throw new UnsupportedOperationException("Cannot query stream-based BAM file");
        }
        mCurrentIterator = createIndexIterator(sequence, start, -1, QueryType.STARTING_AT);
        return mCurrentIterator;
    }

    @Override
    public CloseableIterator<SAMRecord> queryUnmapped() {
        MultiThreading.checkAndRethrow(ex);
        if (mStream == null) {
            throw new IllegalStateException("File reader is closed");
        }
        if (mCurrentIterator != null) {
            throw new IllegalStateException("Iteration in progress");
        }
        if (!mIsSeekable) {
            throw new UnsupportedOperationException("Cannot query stream-based BAM file");
        }
        try {
            final long startOfLastLinearBin = getIndex().getStartOfLastLinearBin();
            if (startOfLastLinearBin != -1) {
                this.seek(startOfLastLinearBin);
            } else {
                // No mapped reads in file, just start at the first read in file.
                this.seek(mFirstRecordPointer);
            }
            mCurrentIterator = new BAMFileIndexUnmappedIterator();
            return mCurrentIterator;
        } catch (IOException e) {
            throw new RuntimeException("IOException seeking to unmapped reads", e);
        }
    }

    /**
     * Prepare to iterate through SAMRecords matching the target interval.
     * @param sequence Desired reference sequence.
     * @param start 1-based start of target interval, inclusive.
     * @param end 1-based end of target interval, inclusive.
     * @param queryType contained, overlapping, or starting-at query.
     */
    private CloseableIterator<SAMRecord> createIndexIterator(final String sequence,
                                                             final int start,
                                                             final int end,
                                                             final QueryType queryType) {
        long[] filePointers = null;

        // Hit the index to determine the chunk boundaries for the required data.
        final SAMFileHeader fileHeader = getFileHeader();
        final int referenceIndex = fileHeader.getSequenceIndex(sequence);
        if (referenceIndex != -1) {
            final BAMIndex fileIndex = getIndex();
            final BAMFileSpan fileSpan = fileIndex.getSpanOverlapping(referenceIndex, start, end);
            filePointers = fileSpan != null ? fileSpan.toCoordinateArray() : null;
        }

        // Create an iterator over the above chunk boundaries.
        final BAMFileIndexIterator iterator = new BAMFileIndexIterator(filePointers);

        // Add some preprocessing filters for edge-case reads that don't fit into this
        // query type.
        return new BAMQueryFilteringIterator(iterator,sequence,start,end,queryType);
    }

    private class BAMFileIndexIterator extends BAMFileIterator {
        private long[] mFilePointers = null;
        private int mFilePointerIndex = 0;
        private long mFilePointerLimit = -1;

        /**
         * Prepare to iterate through SAMRecords stored in the specified compressed blocks at the given offset.
         * @param filePointers the block / offset combination, stored in chunk format.
         */
        BAMFileIndexIterator(final long[] filePointers) {
            super(false);  // delay advance() until after construction
            mFilePointers = filePointers;
            advance();
        }

        SAMRecord getNextRecord()
            throws IOException {
            // Advance to next file block if necessary
            while (mCompressedInputStream.getFilePointer() >= mFilePointerLimit) {
                if (mFilePointers == null ||
                        mFilePointerIndex >= mFilePointers.length) {
                    return null;
                }
                final long startOffset = mFilePointers[mFilePointerIndex++];
                final long endOffset = mFilePointers[mFilePointerIndex++];
                ParallelBAMFileReader.this.seek(startOffset);
                mFilePointerLimit = endOffset;
            }
            // Pull next record from stream
            return super.getNextRecord();
        }
    }

    private class BAMFileIndexUnmappedIterator extends BAMFileIterator  {
        private BAMFileIndexUnmappedIterator() {
            while (this.hasNext() && peek().getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
                advance();
            }
        }
    }

    /**
     * Gets an unbounded pointer to the first record in the BAM file.  Because the reader doesn't necessarily know
     * when the file ends, the rightmost bound of the file pointer will not end exactly where the file ends.  However,
     * the rightmost bound is guaranteed to be after the last read in the file.
     * @return An unbounded pointer to the first record in the BAM file.
     */
    @Override
    SAMFileSpan getFilePointerSpanningReads() {
        MultiThreading.checkAndRethrow(ex);
        return super.getFilePointerSpanningReads();
    }

    /**
     * Iterator for non-indexed sequential iteration through all SAMRecords in file.
     * Starting point of iteration is wherever current file position is when the iterator is constructed.
     */
    private class BAMFileIterator implements CloseableIterator<SAMRecord> {
        private SAMRecord mNextRecord = null;
        private boolean isClosed = false;
        private Iterator<SAMRecord> iter = null;
        private boolean eof = false;  // EOF after last iterator is finished

        BAMFileIterator() {
            this(true);
        }

        /**
         * @param advance Trick to enable subclass to do more setup before advancing
         */
        BAMFileIterator(final boolean advance) {
            startReader();
            if (advance) advance();
        }

        public void close() {
            MultiThreading.checkAndRethrow(ex);
            if (!isClosed) {
                if (mCurrentIterator != null && this != mCurrentIterator) {
                    throw new IllegalStateException("Attempt to close non-current iterator");
                }
                mCurrentIterator = null;
                isClosed = true;
            }
        }

        public boolean hasNext() {
            MultiThreading.checkAndRethrow(ex);
            if (isClosed) throw new IllegalStateException("Iterator has been closed");
            return (mNextRecord != null);
        }

        public SAMRecord next() {
            MultiThreading.checkAndRethrow(ex);
            if (isClosed) throw new IllegalStateException("Iterator has been closed");
            final SAMRecord result = mNextRecord;
            advance();
            return result;
        }

        public void remove() {
            throw new UnsupportedOperationException("Not supported: remove");
        }

        void advance() {
            try {
                mNextRecord = getNextRecord();
            } catch (IOException exc) {
                throw new RuntimeException(exc.getMessage(), exc);
            }
        }

        /**
         * Read the next record from the input stream.
         */
        SAMRecord getNextRecord() throws IOException {
            if (iter != null && iter.hasNext()) return iter.next();
            try {
                if (iter != null && eof) return null;
                final BamDecodeJob job = (BamDecodeJob) reader.runner.getFinishedJob();
                eof = job.eof;
                iter = job.records.iterator();
                return getNextRecord();
            }
            catch (InterruptedException e) {
                throw new IOException("interrupted");
            }
        }

        /**
         * @return The record that will be return by the next call to next()
         */
        protected SAMRecord peek() {
            return mNextRecord;
        }
    }

    private void seek(long pos) throws IOException {
        stopReader(pos);
        mCompressedInputStream.seek(pos);
        startReader();
    }

    private void stopReader(long pos) {
        long prev = mCompressedInputStream.getFilePointer();
        if (prev == pos) return;
        if (reader == null) return;
        reader.close();
        reader = null;
    }

    private void startReader() {
        if (reader != null) return;
        reader = new Reader();
        reader.setDaemon(true);
        reader.start();
    }

    class Reader extends Thread {
        public OrderedJobRunner runner = MultiThreading.newOrderedJobRunner(MultiThreading.bamDecodeQueueSize());

        public Reader() {
            super("ParallelBAMFileReader.Reader");
        }

        /**
         * Shut down the reader.
         */
        public void close() {
            runner.close();
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

        private boolean stopNow() {
            if (this.isInterrupted()) return true;
            if (runner.isClosed()) return true;
            return false;
        }

        /**
         * Runs the reader.
         */
        public void run() {
            long samRecordIndex = 1; // Record number in the file
            long firstRecordIndex = 1;
            BamDecodeJob job = new BamDecodeJob(runner, ex, getFileHeader(), samRecordFactory,
                mValidationStringency, firstRecordIndex);
            try {
                while (true) {
                    if (stopNow()) break;
                    int recordLength = -1;
                    long start = mCompressedInputStream.getFilePointer();
                    try {
                        recordLength = mStream.readInt();
                    }
                    catch (RuntimeEOFException e) {
                        job.eof = true;
                        runner.add(job);
                        break;
                    }
                    if (stopNow()) break;
                    if (job.add(dataInputStream, mCompressedInputStream, start, recordLength)) {
                        samRecordIndex++;
                    }
                    else {
                        runner.add(job);
                        job = new BamDecodeJob(runner, ex, getFileHeader(), samRecordFactory,
                            mValidationStringency, firstRecordIndex);
                        firstRecordIndex = samRecordIndex;
                        if (job.add(dataInputStream, mCompressedInputStream, start, recordLength)) {
                            samRecordIndex++;
                        }
                        else throw new Error("coding error: first job.add failed");
                    }
                }
            }
            catch (InterruptedException e) {
                // somebody interrupted us, so exit
            }
            catch (ThreadDeath e) {
                // somebody told us to stop, so exit
            }
            catch (Throwable t) {
                // pass everything else up to our parent
                ex.compareAndSet(null, t);
            }
        }
    }

    /**
     * A {@link Job} that requests that a byte array of encoded BAM records
     * be decoded into a list of SAMRecords.
     */
    class BamDecodeJob extends RunnableJob {

        /** Stores the file pointers for the records */
        public ArrayList<Long> filePointers = null;
        /** The resulting list of SAMRecords */
        public ArrayList<SAMRecord> records = new ArrayList<SAMRecord>();
        /** Indicates that the input stream hit end-of-file */
        public boolean eof = false;
        byte[] buffer = MultiThreading.getBamRecBuffer();
        int used; // amount of buffer used
        SAMFileHeader header;
        SAMRecordFactory factory;
        ValidationStringency validationStringency;
        long firstRecordIndex;

        /**
            Reads the next encoded BAM record from the stream and stores it in the buffer.
            Returns false if the buffer is full.
        */
        public boolean add(DataInputStream stream, BlockCompressedInputStream bcis,
                    long start, int recSize) throws IOException {
            int fullSize = 4 + recSize;
            if (used + fullSize > buffer.length) {
                if (used == 0) {
                    // first record doesn't fit, so make buffer bigger
                    MultiThreading.recycleBamRecBuffer(buffer);
                    buffer = new byte[fullSize];
                }
                else return false; // no more room
            }
            // save the start position
            if (mFileReader != null) {
                if (filePointers == null) filePointers = new ArrayList<Long>();
                filePointers.add(start);
            }
            // write the record size to the buffer
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, used, 4);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.putInt(recSize);
            // read the rest of the record
            stream.readFully(buffer, used + 4, recSize);
            // save the stop position position
            if (mFileReader != null) filePointers.add(bcis.getFilePointer());
            used += fullSize; // update buffer position
            return true;
        }

        public void run() {
            BamDecodeJob job = this;
            if (job.isCancelled()) return;
            BAMRecordCodec codec = new BAMRecordCodec(job.header, job.factory);
            ByteArrayInputStream stream = new ByteArrayInputStream(job.buffer, 0, job.used);
            codec.setInputStream(stream); 
            long recordIndex = job.firstRecordIndex;
            int index = 0;
            while (true) {
                SAMRecord rec = codec.decode();
                if (rec == null) break;
                // Because some decoding is done lazily, the record needs to remember the validation stringency.
                rec.setValidationStringency(job.validationStringency);
                if (job.validationStringency != ValidationStringency.SILENT) {
                    final List<SAMValidationError> validationErrors = rec.isValid();
                    SAMUtils.processValidationErrors(validationErrors, recordIndex, job.validationStringency);
                }
                if (eagerDecode) rec.eagerDecode();
                if (mFileReader != null) {
                    long start = job.filePointers.get(index);
                    long stop  = job.filePointers.get(index + 1);
                    if (mFileReader != null && rec != null)
                        rec.setFileSource(new SAMFileSource(mFileReader,new BAMFileSpan(new Chunk(start,stop))));
                }
                job.records.add(rec);
                recordIndex++;
                index += 2;
            }
            MultiThreading.recycleBamRecBuffer(job.buffer);
            job.buffer = null;
            job.setFinished(true);
        }

        public BamDecodeJob(OrderedJobRunner r, AtomicReference<Throwable> ex,
            SAMFileHeader header, SAMRecordFactory factory,
            ValidationStringency validationStringency, long firstRecordIndex)
        {
            super(r, ex);
            this.header = header; this.factory = factory;
            this.validationStringency = validationStringency;
            this.firstRecordIndex = firstRecordIndex;
        }
    }
}
