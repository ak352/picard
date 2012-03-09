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
import net.sf.samtools.util.BlockCompressedFilePointerUtil;

public class DelayedFilePointer {
	private BlockCompressed block = null;
	private int offset = 0; // this needs to be set as the block.blockOffset could change...

	public DelayedFilePointer(BlockCompressed block, int offset) {
		this.block = block;
		this.offset = offset;
	}

	/**
	 * Convert the file pointer to its <code>long</code> equivalent.
	 * Will wait until the associated blocks have been written.
	 */
	public long toLong() {
		return BlockCompressedFilePointerUtil.makeFilePointer(this.getBlockAddress(), this.offset);
	}

	/**
	 * Return the block address portion of the file pointer.
	 * Will wait until the associated blocks have been written.
	 * @return File offset of start of BGZF block for this virtual file pointer.
	 */
	public long getBlockAddress() {
		return this.block.getBlockAddress();
	}

	/**
	 * @return Offset into uncompressed block for this virtual file pointer.
	 */
	public int getBlockOffset() {
		return offset;
	}
}
