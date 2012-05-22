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
package net.sf.samtools.util;

import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedOutputStream;
import net.sf.samtools.util.mt.ParallelBlockCompressedInputStream;
import net.sf.samtools.util.mt.ParallelBlockCompressedOutputStream;
import net.sf.samtools.util.mt.MultiThreading;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

/**
 * Creates a BlockCompressedInputStream, BlockCompressedOutputStream,
 * ParallelBlockCompressedInputStream, or ParallelBlockCompressedOutputStream.
 */
public class BlockCompressedStreamFactory {

    /**
     * Note that seek() is not supported if this ctor is used.
     */
    public static BlockCompressedInputStream makeBlockCompressedInputStream(final InputStream stream) {
        if (MultiThreading.isParallelDecompressionEnabled())
            return new ParallelBlockCompressedInputStream(stream);
        else
            return new BlockCompressedInputStream(stream);
    }

    /**
     * Use this ctor if you wish to call seek()
     */
    public static BlockCompressedInputStream makeBlockCompressedInputStream(final File file) throws IOException {
        if (MultiThreading.isParallelDecompressionEnabled())
            return new ParallelBlockCompressedInputStream(file);
        else
            return new BlockCompressedInputStream(file);
    }

    public static BlockCompressedInputStream makeBlockCompressedInputStream(final URL url) {
        if (MultiThreading.isParallelDecompressionEnabled())
            return new ParallelBlockCompressedInputStream(url);
        else
            return new BlockCompressedInputStream(url);
    }

    /**
     * For providing some arbitrary data source.  No additional buffering is
     * provided, so if the underlying source is not buffered, wrap it in a
     * SeekableBufferedStream before passing to this ctor.
     */
    public static BlockCompressedInputStream makeBlockCompressedInputStream(final SeekableStream strm) {
        if (MultiThreading.isParallelDecompressionEnabled())
            return new ParallelBlockCompressedInputStream(strm);
        else
            return new BlockCompressedInputStream(strm);
    }


    /**
     * Uses default compression level, which is 5 unless changed by setDefaultCompressionLevel
     */
    public static OutputStream makeBlockCompressedOutputStream(final String filename) {
        if (MultiThreading.isParallelCompressionEnabled())
            return new ParallelBlockCompressedOutputStream(filename);
        else
            return new BlockCompressedOutputStream(filename);
    }

    /**
     * Uses default compression level, which is 5 unless changed by setDefaultCompressionLevel
     */
    public static OutputStream makeBlockCompressedOutputStream(final File file) {
        if (MultiThreading.isParallelCompressionEnabled())
            return new ParallelBlockCompressedOutputStream(file);
        else
            return new BlockCompressedOutputStream(file);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public static OutputStream makeBlockCompressedOutputStream(final String filename, final int compressionLevel) {
        if (MultiThreading.isParallelCompressionEnabled())
            return new ParallelBlockCompressedOutputStream(filename, compressionLevel);
        else
            return new BlockCompressedOutputStream(filename, compressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public static OutputStream makeBlockCompressedOutputStream(final File file, final int compressionLevel) {
        if (MultiThreading.isParallelCompressionEnabled())
            return new ParallelBlockCompressedOutputStream(file, compressionLevel);
        else
            return new BlockCompressedOutputStream(file, compressionLevel);
    }

    /**
     * Constructors that take output streams
     * file may be null
     */
    public static OutputStream makeBlockCompressedOutputStream(final OutputStream os, File file) {
        if (MultiThreading.isParallelCompressionEnabled())
            return new ParallelBlockCompressedOutputStream(os, file);
        else
            return new BlockCompressedOutputStream(os, file);
    }

    public static OutputStream makeBlockCompressedOutputStream(final OutputStream os, final File file, final int compressionLevel) {
        if (MultiThreading.isParallelCompressionEnabled())
            return new ParallelBlockCompressedOutputStream(os, file, compressionLevel);
        else
            return new BlockCompressedOutputStream(os, file, compressionLevel);
    }
}
