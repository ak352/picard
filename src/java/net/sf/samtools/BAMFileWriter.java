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

import net.sf.samtools.util.BinaryCodec;
import net.sf.samtools.util.BlockCompressedOutputStream;
import net.sf.samtools.util.pbgzf.ParallelBlockCompressedOutputStream;
import net.sf.samtools.util.pbgzf.DelayedFilePointer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.OutputStream;

/**
 * Concrete implementation of SAMFileWriter for writing gzipped BAM files.
 */
public class BAMFileWriter extends SAMFileWriterImpl {

    private final BinaryCodec outputBinaryCodec;
    private BAMRecordCodec bamRecordCodec = null;
    private final BlockCompressedOutputStream blockCompressedOutputStream;
    private BAMIndexer bamIndexer = null;
    private AsyncBAMIndexer asyncBamIndexer = null;
    private boolean writingIndex = false;

    /**
     * Use multiple threads
     */
    private boolean parallel = (Defaults.NUM_PBGZF_THREADS > 0) && !Defaults.DISABLE_PBGZF_COMPRESSION;

    public BAMFileWriter(final File path) {
        if (parallel) blockCompressedOutputStream = new ParallelBlockCompressedOutputStream(path);
        else blockCompressedOutputStream = new BlockCompressedOutputStream(path);
        outputBinaryCodec = new BinaryCodec(new DataOutputStream(blockCompressedOutputStream));
        outputBinaryCodec.setOutputFileName(path.getAbsolutePath());
    }

    public BAMFileWriter(final File path, final int compressionLevel) {
        if (parallel) blockCompressedOutputStream = new ParallelBlockCompressedOutputStream(path, compressionLevel);
        else blockCompressedOutputStream = new BlockCompressedOutputStream(path, compressionLevel);
        outputBinaryCodec = new BinaryCodec(new DataOutputStream(blockCompressedOutputStream));
        outputBinaryCodec.setOutputFileName(path.getAbsolutePath());
    }

    public BAMFileWriter(final OutputStream os, final File file) {
        if (parallel) blockCompressedOutputStream = new ParallelBlockCompressedOutputStream(os, file);
        else blockCompressedOutputStream = new BlockCompressedOutputStream(os, file);
        outputBinaryCodec = new BinaryCodec(new DataOutputStream(blockCompressedOutputStream));
        outputBinaryCodec.setOutputFileName(getPathString(file));
    }

    public BAMFileWriter(final OutputStream os, final File file, final int compressionLevel) {
        if (parallel) blockCompressedOutputStream = new ParallelBlockCompressedOutputStream(os, file, compressionLevel);
        else blockCompressedOutputStream = new BlockCompressedOutputStream(os, file, compressionLevel);
        outputBinaryCodec = new BinaryCodec(new DataOutputStream(blockCompressedOutputStream));
        outputBinaryCodec.setOutputFileName(getPathString(file));
    }

    private void prepareToWriteAlignments() {
        if (bamRecordCodec == null) {
            bamRecordCodec = new BAMRecordCodec(getFileHeader());
            bamRecordCodec.setOutputStream(outputBinaryCodec.getOutputStream(), getFilename());
        }
    }

    /** @return absolute path, or null if arg is null.  */
    private String getPathString(File path){
        return (path != null) ? path.getAbsolutePath() : null;
    }

   // Allow enabling the bam index construction
   // only enabled by factory method before anything is written
   void enableBamIndexConstruction () {
        if (!getSortOrder().equals(SAMFileHeader.SortOrder.coordinate)){
           throw new SAMException("Not creating BAM index since not sorted by coordinates: " + getSortOrder());
        }
        if(getFilename() == null){
            throw new SAMException("Not creating BAM index since we don't have an output file name");
        }
        setBamIndexer(outputBinaryCodec.getOutputFileName());
        writingIndex = true;
    }

    private void setBamIndexer(String path) {
        try {
            final String indexFileBase = path.endsWith(".bam") ?
                    path.substring(0, path.lastIndexOf(".")) : path;
            final File indexFile = new File(indexFileBase + BAMIndex.BAMIndexSuffix);
            if (indexFile.exists()) {
                if (!indexFile.canWrite()) {
                    throw new SAMException("Not creating BAM index since unable to write index file " + indexFile);
                }
            }
            BAMIndexer indexer = new BAMIndexer(indexFile, getFileHeader());
            if (parallel) asyncBamIndexer = new AsyncBAMIndexer(indexer); //, (AsyncBlockCompressedOutputStream) blockCompressedOutputStream);
            else bamIndexer = indexer;
        } catch (Exception e) {
            throw new SAMException("Not creating BAM index", e);
        }
    }

    protected void writeAlignment(final SAMRecord alignment) {
        prepareToWriteAlignments();

        if (writingIndex) {
            try {
                if (parallel) {
                    ParallelBlockCompressedOutputStream strm = (ParallelBlockCompressedOutputStream) blockCompressedOutputStream;
                    DelayedFilePointer start = strm.getDelayedFilePointer();
                    bamRecordCodec.encode(alignment);
                    DelayedFilePointer end= strm.getDelayedFilePointer();
                    asyncBamIndexer.processAlignment(alignment, start, end);
                } else {
                    final long startOffset = blockCompressedOutputStream.getFilePointer();
                    bamRecordCodec.encode(alignment);
                    final long stopOffset = blockCompressedOutputStream.getFilePointer();
                    // set the alignment's SourceInfo and then prepare its index information
                    alignment.setFileSource(new SAMFileSource(null, new BAMFileSpan(new Chunk(startOffset, stopOffset))));
                    bamIndexer.processAlignment(alignment);
                }
            } catch (Exception e) {
                bamIndexer = null;
                asyncBamIndexer = null;
                throw new SAMException("Exception when processing alignment for BAM index " + alignment, e);
            }
        } else {
            bamRecordCodec.encode(alignment);
        }
    }

    protected void writeHeader(final String textHeader) {
        outputBinaryCodec.writeBytes(BAMFileConstants.BAM_MAGIC);

        // calculate and write the length of the SAM file header text and the header text
        outputBinaryCodec.writeString(textHeader, true, false);

        // write the sequences binarily.  This is redundant with the text header
        outputBinaryCodec.writeInt(getFileHeader().getSequenceDictionary().size());
        for (final SAMSequenceRecord sequenceRecord: getFileHeader().getSequenceDictionary().getSequences()) {
            outputBinaryCodec.writeString(sequenceRecord.getSequenceName(), true, true);
            outputBinaryCodec.writeInt(sequenceRecord.getSequenceLength());
        }
    }

    protected void finish() {
        outputBinaryCodec.close();
        try {
            if (bamIndexer != null) {
                bamIndexer.finish();
                bamIndexer = null;
<<<<<<< HEAD
            }
            if (asyncBamIndexer != null) {
                asyncBamIndexer.finish();
                asyncBamIndexer = null;
            }
=======
            }
            if (asyncBamIndexer != null) {
                asyncBamIndexer.finish();
                asyncBamIndexer = null;
            }
>>>>>>> c7bbee4c40d807510bfa7dfa8188bd8ffaa4335b
        } catch (Exception e) {
            throw new SAMException("Exception writing BAM index file", e);
        }
    }

    /** @return absolute path, or null if this writer does not correspond to a file.  */
    protected String getFilename() {
        return outputBinaryCodec.getOutputFileName();
    }
}
