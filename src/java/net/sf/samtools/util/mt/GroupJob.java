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

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordFactory;
import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.BlockCompressedInputStream;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;

/**
 * A GroupJob represents a list of Jobs that are processed as a group to reduce
 * multi-threading overhead.  Multi-threading is expensive, so we don't want to
 * process a lot of short-duration jobs.  Instead, we want to process a smaller
 * number of medium-duration jobs, so that the Worker threads are busy doing useful
 * work most of the time instead of dealing with multi-threading overhead.
 */
public class GroupJob extends Job {

    public ArrayList<Job> jobs = new ArrayList<Job>();

    public GroupJob(JobRunner r, AtomicReference<Throwable> ex) {
        super(r, ex);
    }

    public void add(Job job) {
        jobs.add(job);
    }
}
