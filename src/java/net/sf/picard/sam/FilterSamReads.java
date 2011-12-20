/*
 * The MIT License
 *
 * Copyright (c) 2011 The Broad Institute
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

/**
 * $Id$
 */
package net.sf.picard.sam;

import net.sf.picard.PicardException;
import net.sf.picard.cmdline.CommandLineProgram;
import net.sf.picard.cmdline.Option;
import net.sf.picard.cmdline.StandardOptionDefinitions;
import net.sf.picard.cmdline.Usage;
import net.sf.picard.io.IoUtil;
import net.sf.picard.util.CollectionUtil;
import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.CloseableIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

/**
 * Produces a new SAM or BAM file by including or excluding mapped
 * or unmapped reads or a list of specific reads names from a SAM or BAM file
 */
public class FilterSamReads extends CommandLineProgram {

    private static final Log log = Log.getInstance(FilterSamReads.class);

    public enum ReadFilterType {INCLUDE, EXCLUDE}
    public enum ReadMappingType {MAPPED, UNMAPPED}

    @Usage
    public String USAGE =
        "Produces a new SAM or BAM file by including or excluding mapped or " +
            "unmapped reads or a list of specific reads names from a SAM or BAM file";

    @Option(doc = "The SAM or BAM file that will be read excluded",
        optional = false,
        shortName = StandardOptionDefinitions.INPUT_SHORT_NAME)
    public File INPUT;

    @Option(
        doc = "Determines if reads should be included or excluded from OUTPUT SAM or BAM file",
        optional = false, shortName = "RFT")
    public ReadFilterType READ_FILTER_TYPE;

    @Option(doc = "Exclude mapped or umapped reads from the SAM or BAM",
        mutex = { "READNAME_LIST_FILE" }, optional = false, shortName = "RMT")
    public ReadMappingType READ_MAPPING_TYPE;

    @Option(
        doc = "A file of read names that will be included or excluded from the SAM or BAM",
        mutex = { "READ_MAPPING_TYPE" }, optional = false, shortName = "RLF")
    public File READNAME_LIST_FILE;

    @Option(doc = "SortOrder of the OUTPUT SAM or BAM file, " +
        "otherwise use the SortOrder of the INPUT file.",
        optional = true, shortName = "SO")
    public SAMFileHeader.SortOrder SORT_ORDER;

    @Option(doc = "SAM or BAM file to write read excluded results to",
        optional = false, shortName = "O")
    public File OUTPUT;

    private void filterByReadNameList() {

        // read RLF file into sorted Tree Set - this will remove dups
        IoUtil.assertFileIsReadable(READNAME_LIST_FILE);
        IoUtil.assertFileSizeNonZero(READNAME_LIST_FILE);
        final BufferedReader is;

        try {
            is = IoUtil.openFileForBufferedReading(READNAME_LIST_FILE);
        } catch (IOException e) {
            throw new PicardException(e.getMessage(), e);
        }

        final Scanner scanner = new Scanner(is);
        final Set<String> readNameFilterSet = new TreeSet<String>();

        while (scanner.hasNext()) {
            final String line = scanner.nextLine();

            if (!line.trim().isEmpty()) {
                readNameFilterSet.add(line.trim());
            }
        }

        scanner.close();

        final SAMFileReader reader = new SAMFileReader(INPUT);
        final SAMFileHeader header = reader.getFileHeader();

        if (SORT_ORDER != null) {
            header.setSortOrder(SORT_ORDER);
        }

        log.info("SORT_ORDER of OUTPUT=" + OUTPUT.getName() + " will be " +
            header.getSortOrder().name());

        final SAMFileWriter writer = new SAMFileWriterFactory()
            .makeSAMOrBAMWriter(header, false, OUTPUT);

        int count = 0;
        for (final SAMRecord rec : reader) {

            if (READ_FILTER_TYPE.equals(ReadFilterType.INCLUDE)) {
                if (readNameFilterSet.contains(rec.getReadName())) {
                    writer.addAlignment(rec);
                    count++;
                    readNameFilterSet.remove(rec.getReadName());
                }
            } else {
                if (readNameFilterSet.contains(rec.getReadName())) {
                    readNameFilterSet.remove(rec.getReadName());
                } else {
                    writer.addAlignment(rec);
                    count++;
                }
            }

            if (count != 0 && count % 1000000 == 0) {
                log.info(new DecimalFormat("#,###").format(count) +
                    " SAMRecords written to " + OUTPUT.getName());
            }
        }

        reader.close();
        writer.close();
        log.info(new DecimalFormat("#,###").format(count) +
                    " SAMRecords written to " + OUTPUT.getName());

        if (!readNameFilterSet.isEmpty()) {
            throw new PicardException(readNameFilterSet.size() +
                " reads could not be found within INPUT=" + INPUT.getPath() +
                " [" + CollectionUtil.join(readNameFilterSet, ",") + "]");
        }

        IoUtil.assertFileIsReadable(OUTPUT);
    }

    private SAMRecord obtainAssertedMate(
        final CloseableIterator<SAMRecord> samRecordIterator,
        final SAMRecord firstOfPair) {

        if (samRecordIterator.hasNext()) {

            final SAMRecord secondOfPair = samRecordIterator.next();

            // Validate paired reads arrive as first of pair, then second of pair
            if (!firstOfPair.getReadName().equals(secondOfPair.getReadName())) {
                throw new PicardException("Second read from pair not found: " +
                    firstOfPair.getReadName() + ", " +
                    secondOfPair.getReadName());
            } else if (!firstOfPair.getFirstOfPairFlag()) {
                throw new PicardException(
                    "First record in unmapped bam is not first of pair: " +
                        firstOfPair.getReadName());
            } else if (!secondOfPair.getReadPairedFlag()) {
                throw new PicardException(
                    "Second record is not marked as paired: " +
                        secondOfPair.getReadName());
            } else if (!secondOfPair.getSecondOfPairFlag()) {
                throw new PicardException(
                    "Second record is not second of pair: " +
                        secondOfPair.getReadName());
            } else {
                return secondOfPair;
            }

        } else {
            throw new PicardException(
                "A second record does not exist: " + firstOfPair.getReadName());
        }
    }

        /**
     * If necessary produce a new bam file sorted by queryname
     *
     * @param fileToSort File requiring sorting
     *
     * @return the queryname sorted file
     */
    private File convertToSortedByQueryName(final File fileToSort) {

        File outputFile = fileToSort;

        if (outputFile != null) {
            // sort INPUT file by queryname
            final SAMFileReader reader = new SAMFileReader(fileToSort);
            final SAMFileHeader header = reader.getFileHeader();

            if (header.getSortOrder() == null || !header.getSortOrder()
                .equals(SAMFileHeader.SortOrder.queryname)) {
                // if not sorted by queryname then sort it by queryname
                log.info("Sorting " + fileToSort.getName() + " by queryname");

                outputFile = new File(OUTPUT.getParentFile(),
                    IoUtil.basename(fileToSort) + ".queryname.sorted.bam");
                IoUtil.assertFileIsWritable(outputFile);

                header.setSortOrder(SAMFileHeader.SortOrder.queryname);
                final SAMFileWriter writer = new SAMFileWriterFactory()
                    .makeBAMWriter(header, false, outputFile);

                int count = 0;
                for (final SAMRecord rec : reader) {
                    writer.addAlignment(rec);

                    if (++count % 1000000 == 0) {
                        log.info(new DecimalFormat("#,###").format(count) +
                            " SAMRecords written to " + outputFile.getName());
                    }
                }

                reader.close();
                writer.close();
                log.info(new DecimalFormat("#,###").format(count) +
                    " SAMRecords written to " + outputFile.getName());
            }
        }

        return outputFile;
    }

    /**
     * Filter reads by ReadMappingType
     */
    private void filterByReadMappingType() {

        final SAMFileReader inputReader = new SAMFileReader(INPUT);
        final SAMFileHeader outPutHeader = new SAMFileReader(INPUT).getFileHeader();
        if (SORT_ORDER != null) {
            outPutHeader.setSortOrder(SORT_ORDER);
        }
        inputReader.close();

        final File inputQnSortedFile = convertToSortedByQueryName(INPUT);
        final SAMFileReader reader = new SAMFileReader(inputQnSortedFile);

        log.info("SORT_ORDER of OUTPUT=" + OUTPUT.getName() + " will be " +
            outPutHeader.getSortOrder().name());

        final SAMFileWriter writer = new SAMFileWriterFactory()
            .makeSAMOrBAMWriter(outPutHeader, false, OUTPUT);

        int count = 0;
        final CloseableIterator<SAMRecord> it = reader.iterator();
        while (it.hasNext()) {
            final SAMRecord r1 = it.next();
            boolean writeToOutput = false;

            if (r1.getReadPairedFlag()) {

                final SAMRecord r2 = obtainAssertedMate(it, r1);

                if ((READ_FILTER_TYPE.equals(ReadFilterType.INCLUDE) &&
                    READ_MAPPING_TYPE.equals(ReadMappingType.MAPPED)) ||
                    (READ_FILTER_TYPE.equals(ReadFilterType.EXCLUDE) &&
                        READ_MAPPING_TYPE.equals(ReadMappingType.UNMAPPED))) {

                    // include mapped or exclude unmapped
                    if (!r1.getReadUnmappedFlag() &&
                        !r2.getReadUnmappedFlag()) {
                        writeToOutput = true;
                    }
                } else {
                    // include unmapped or exclude mapped
                    if (r1.getReadUnmappedFlag() && r2.getReadUnmappedFlag()) {
                        writeToOutput = true;
                    }
                }

                if (writeToOutput) {
                    writer.addAlignment(r1);
                    writer.addAlignment(r2);
                    count++;
                } else {
                    log.debug("Skipping " + READ_FILTER_TYPE + " " +
                        READ_MAPPING_TYPE + " " + r1.toString() + " and " +
                        r2.toString());
                }

            } else {
                if ((READ_FILTER_TYPE.equals(ReadFilterType.INCLUDE) &&
                    READ_MAPPING_TYPE.equals(ReadMappingType.MAPPED)) ||
                    (READ_FILTER_TYPE.equals(ReadFilterType.EXCLUDE) &&
                        READ_MAPPING_TYPE.equals(ReadMappingType.UNMAPPED))) {

                    // include mapped or exclude unmapped
                    if (!r1.getReadUnmappedFlag()) {
                        writeToOutput = true;
                    }
                } else {
                    // include unmapped or exclude mapped
                    if (r1.getReadUnmappedFlag()) {
                        writeToOutput = true;
                    }
                }

                if (writeToOutput) {
                    writer.addAlignment(r1);
                    count++;
                } else {
                    log.info("Skipping " + READ_FILTER_TYPE + " " +
                        READ_MAPPING_TYPE + " " + r1.toString());
                }
            }

            if (count != 0 && (count % 1000000 == 0)) {
                log.info(new DecimalFormat("#,###").format(count) +
                    " SAMRecords written to " + OUTPUT.getName());
            }
        }

        reader.close();
        writer.close();
        log.info(new DecimalFormat("#,###").format(count) +
            " SAMRecords written to " + OUTPUT.getName());

        if (!inputQnSortedFile.equals(INPUT)) {
            log.debug("Removing temporary file " + inputQnSortedFile.getAbsolutePath());
            if (!inputQnSortedFile.delete()) {
                throw new PicardException("Failed to delete " + inputQnSortedFile.getAbsolutePath());
            }
        }
    }

    @Override
    protected int doWork() {

        if (INPUT.equals(OUTPUT)) {
            throw new PicardException(
                "INPUT file and OUTPUT file must differ!");
        }

        try {
            IoUtil.assertFileIsReadable(INPUT);
            IoUtil.assertFileIsWritable(OUTPUT);

            if (READNAME_LIST_FILE != null) {
                filterByReadNameList();
            } else {
                filterByReadMappingType();
            }

            IoUtil.assertFileIsReadable(OUTPUT);

            return 0;

        } catch (Exception e) {
            if (!OUTPUT.delete()) {
                log.warn("Failed to delete " + OUTPUT.getAbsolutePath());
            }

            log.error(e, "Failed to filter " + INPUT.getName());
            return 1;
        }
    }

    /**
     * Stock main method.
     *
     * @param args main arguements
     */
    public static void main(final String[] args) {
        System.exit(new FilterSamReads().instanceMain(args));
    }

}