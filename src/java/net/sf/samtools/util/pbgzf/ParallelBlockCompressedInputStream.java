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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedFilePointerUtil;
import net.sf.samtools.util.SeekableStream;
import net.sf.samtools.util.SeekableFileStream;
import net.sf.samtools.util.SeekableBufferedStream;
import net.sf.samtools.util.SeekableHTTPStream;
import net.sf.samtools.util.IOUtil;
import net.sf.samtools.util.BlockCompressedStreamConstants;
import net.sf.samtools.FileTruncatedException;

/*
 * Utility class for reading BGZF block compressed files.  The caller can treat this file like any other InputStream.
 * It probably is not necessary to wrap this stream in a buffering stream, because there is internal buffering.
 * The advantage of BGZF over conventional GZip format is that BGZF allows for seeking without having to read the
 * entire file up to the location being sought.  Note that seeking is only possible if the ctor(File) is used.
 *
 * c.f. http://samtools.sourceforge.net/SAM1.pdf for details of BGZF format
 */
public class ParallelBlockCompressedInputStream extends BlockCompressedInputStream {

    private BlockCompressedReader reader = null;
    private BlockCompressedMultiQueue multiQueue = null;
    private BlockCompressedQueue blockQueue = null;
    private BlockCompressed block = null;
    private boolean eof = false;
    private int blockOffset = 0;
    private long blockAddress = 0;

    private void init() {
        int i;
        //System.err.println("INIT 1 ParallelBlockCompressedInputStream");
        // create the queues
        this.multiQueue = BlockCompressedMultiQueue.getInstance();
        // register
        this.blockQueue = this.multiQueue.register();
        // create the reader
        this.reader = new BlockCompressedReader(mStream, mFile, this.blockQueue);
        // start the reader and consumers
        this.reader.start();
        //System.err.println("INIT 2 ParallelBlockCompressedInputStream");
    }

    /**
     * Note that seek() is not supported if this ctor is used.
     */
    public ParallelBlockCompressedInputStream(final InputStream stream) {
        super(stream);
        init();
    }

    /**
     * Use this ctor if you wish to call seek()
     */
    public ParallelBlockCompressedInputStream(final File file)
        throws IOException {
        super(file);
        init();

    }

    public ParallelBlockCompressedInputStream(final URL url) {
        super(url);
        init();
    }

    /**
     * For providing some arbitrary data source.  No additional buffering is
     * provided, so if the underlying source is not buffered, wrap it in a
     * SeekableBufferedStream before passing to this ctor.
     */
    public ParallelBlockCompressedInputStream(final SeekableStream strm) {
        super(strm);
        init();
    }

    /**
     * Determines whether or not the inflater will re-calculated the CRC on the decompressed data
     * and check it against the value stored in the GZIP header.  CRC checking is an expensive
     * operation and should be used accordingly.
     */
    public void setCheckCrcs(final boolean check) {
        int i;
        // TODO
        // Pass this to the consumers in the queue
    }

    /**
     * @return the number of bytes that can be read (or skipped over) from this input stream without blocking by the
     * next caller of a method for this input stream. The next caller might be the same thread or another thread.
     * Note that although the next caller can read this many bytes without blocking, the available() method call itself
     * may block in order to fill an internal buffer if it has been exhausted.
     */
    public int available()
        throws IOException {
        try {
            if(null == this.block || this.block.blockOffset == this.block.blockLength) { // get a new block
                if(null != this.block) { // destroy this block
                    this.block = null;
                }
                this.block = this.blockQueue.getOutput();
            }
            if(null == this.block) { // no block
                this.blockOffset = 0;
                this.eof = true; // EOF
                return 0;
            }
            else {
                this.blockAddress = this.block.getBlockAddress();
                return (this.block.blockLength - this.block.blockOffset);
            }
        } catch(InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * Closes the underlying InputStream or RandomAccessFile
     */
    public void close()
        throws IOException {
        int i;

        // shut down the reader
        this.reader.setDone();

        // close the queue
        this.multiQueue.deregister(this.blockQueue);

        // join
        this.reader.join();

        // close the reader
        this.reader.close();
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is returned as an int in the range 0 to 255.
     * If no byte is available because the end of the stream has been reached, the value -1 is returned.
     * This method blocks until input data is available, the end of the stream is detected, or an exception is thrown.

     * @return the next byte of data, or -1 if the end of the stream is reached.
     */
    public int read()
        throws IOException {
        //System.err.println("1 read() available()=" + available());
        if(available() > 0) {
            this.blockOffset++;
            return this.block.buffer[this.block.blockOffset++];
        }
        else {
            return -1;
        }
    }

    /**
     * Reads some number of bytes from the input stream and stores them into the buffer array b. The number of bytes
     * actually read is returned as an integer. This method blocks until input data is available, end of file is detected,
     * or an exception is thrown.
     *
     * read(buf) has the same effect as read(buf, 0, buf.length).
     *
     * @param buffer the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 is there is no more data because the end of
     * the stream has been reached.
     */
    public int read(final byte[] buffer)
        throws IOException {
        return read(buffer, 0, buffer.length);
    }

    private volatile ByteArrayOutputStream buf = null;
    private static final byte eol = '\n';
    private static final byte eolCr = '\r';
    
    /**
     * Reads a whole line. A line is considered to be terminated by either a line feed ('\n'), 
     * carriage return ('\r') or carriage return followed by a line feed ("\r\n").
     *
     * @return  A String containing the contents of the line, excluding the line terminating
     *          character, or null if the end of the stream has been reached
     *
     * @exception  IOException  If an I/O error occurs
     * @
     */
    public String readLine() throws IOException {
    	int available = available();
        //System.err.println("2 read() available()=" + available);
        if (available == 0) {
            return null;
        }
        if(null == buf){ // lazy initialisation 
        	buf = new ByteArrayOutputStream(8192);
        }
        buf.reset();
    	boolean done = false;
    	boolean foundCr = false; // \r found flag
        while (!done) {
        	int linetmpPos = this.block.blockOffset;
        	int bCnt = 0;
        	while((available-- > 0)){
        		final byte c = this.block.buffer[linetmpPos++];
        		if(c == eol){ // found \n
        			done = true;
        			break;
        		} else if(foundCr){  // previous char was \r
        			--linetmpPos; // current char is not \n so put it back
        			done = true;
        			break;
        		} else if(c == eolCr){ // found \r
					foundCr = true;
        			continue; // no ++bCnt
        		}
				++bCnt;
        	}
        	if(this.block.blockOffset < linetmpPos){
				buf.write(this.block.buffer, this.block.blockOffset, bCnt);
	        	this.blockOffset = this.block.blockOffset = linetmpPos;
        	}
        	available = available();    
            //System.err.println("2.1 read() available()=" + available);
        	if(available == 0){
        		// EOF
        		done = true;
        	}
        }
        //System.err.println("2.2 read() available()=" + available);
    	return buf.toString();
    }

    /**
     * Reads up to len bytes of data from the input stream into an array of bytes. An attempt is made to read
     * as many as len bytes, but a smaller number may be read. The number of bytes actually read is returned as an integer.
     *
     * This method blocks until input data is available, end of file is detected, or an exception is thrown.
     *
     * @param buffer buffer into which data is read.
     * @param offset the start offset in array b  at which the data is written.
     * @param length the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data because the end of
     * the stream has been reached.
     */
    public int read(final byte[] buffer, int offset, int length)
        throws IOException {
        final int originalLength = length;
        //System.err.println("3.0 read() length=" + length);
        while (length > 0) {
            final int available = available();
            //System.err.println("3.1 read() available()=" + available);
            if (available == 0) {
                // Signal EOF to caller
                if (originalLength == length) {
                    return -1;
                }
                break;
            }
            final int copyLength = Math.min(length, available);
            System.arraycopy(this.block.buffer, this.block.blockOffset, buffer, offset, copyLength);
            this.blockOffset = this.block.blockOffset += copyLength;
            offset += copyLength;
            length -= copyLength;
        }
        //System.err.println("3.2 read() return=" + (originalLength - length));
        return originalLength - length;
    }

    /**
     * Seek to the given position in the file.  Note that pos is a special virtual file pointer,
     * not an actual byte offset.
     *
     * @param pos virtual file pointer
     */
    public void seek(final long pos)
        throws IOException {
        //System.err.println("Seeking to pos=" + pos);
        try {
            if (this.reader.getFile() == null) {
                throw new IOException("Cannot seek on stream based file");
            }

            // close the queue
            this.multiQueue.deregister(this.blockQueue);

            // the reader is done
            this.reader.setDone();

            // join
            this.reader.join();

            // seek
            if (mFile == null) {
                throw new IOException("Cannot seek on stream based file");
            }

            // Decode virtual file pointer
            // Upper 48 bits is the byte offset into the compressed stream of a block.
            // Lower 16 bits is the byte offset into the uncompressed stream inside the block.
            final long blockAddress = BlockCompressedFilePointerUtil.getBlockAddress(pos);
            final int blockOffset = BlockCompressedFilePointerUtil.getBlockOffset(pos);
            final int available;
            if (null != this.block && this.blockAddress == blockAddress) {
                available = this.block.blockLength;
            }
            else {
                mFile.seek(blockAddress);
                // TODO: error checking for this seek
            }
            
            if (super.eof()) { // check to see if mFile is at EOF
                this.block = null;
            }
            else {
                // re-register
                this.blockQueue = this.multiQueue.register();

                // reset
                this.reader.reset(this.blockQueue);

                // restart the reader
                this.reader.start();

                // get a block
                //System.err.println("Seek: get a block");
                this.block = this.blockQueue.getOutput();
            }
            // TODO: will this always get a block?
            if (this.block == null) this.eof = true;

            // reset block offset/address
            this.blockOffset = blockOffset;
            this.blockAddress = blockAddress;
            if (null != this.block) this.block.blockOffset = this.blockOffset;

        } catch(InterruptedException e) {
            throw new IOException(e);
        }
        //System.err.println("Seeked to pos=" + pos);
    }

    protected boolean eof() throws IOException {
        if (this.eof) {
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * @return virtual file pointer that can be passed to seek() to return to the current position.  This is
     * not an actual byte offset, so arithmetic on file pointers cannot be done to determine the distance between
     * the two.
     */
    public long getFilePointer() {
        if (this.block.blockOffset == this.block.blockLength) {
            // If current offset is at the end of the current block, file pointer should point
            // to the beginning of the next block.
            return BlockCompressedFilePointerUtil.makeFilePointer(this.block.getBlockAddress() + this.block.priorLength, 0);
        }
        return BlockCompressedFilePointerUtil.makeFilePointer(this.block.getBlockAddress(), this.block.blockOffset);
    }

    public static long getFileBlock(final long bgzfOffset) {
        return BlockCompressedFilePointerUtil.getBlockAddress(bgzfOffset);
    }
}
