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

package net.sf.picard.sam;

import net.sf.picard.cmdline.Option;
import net.sf.picard.cmdline.StandardOptionDefinitions;
import net.sf.picard.cmdline.Usage;
import net.sf.picard.metrics.MetricsFile;
import net.sf.picard.util.Histogram;
import net.sf.picard.util.Log;
import net.sf.picard.PicardException;
import net.sf.picard.io.IoUtil;
import net.sf.samtools.*;
import net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.util.CloseableIterator;
import net.sf.samtools.util.SortingCollection;
import net.sf.samtools.util.SortingLongCollection;

import java.io.*;
import java.util.*;

/**
 * A better duplication marking algorithm that handles all cases including clipped
 * and gapped alignments.
 *
 * @author Tim Fennell
 */
public class MarkDuplicates extends AbstractDuplicateFindingAlgorithm {
    private final Log log = Log.getInstance(MarkDuplicates.class);

    /**
     * If more than this many sequences in SAM file, don't spill to disk because there will not
     * be enough file handles.
     */

    @Usage public final String USAGE =
            "Examines aligned records in the supplied SAM or BAM file to locate duplicate molecules. " +
            "All records are then written to the output file with the duplicate records flagged.";

    @Option(shortName= StandardOptionDefinitions.INPUT_SHORT_NAME, doc="One or more input SAM or BAM files to analyze.  Must be coordinate sorted.")
    public List<File> INPUT;

    @Option(shortName=StandardOptionDefinitions.OUTPUT_SHORT_NAME, doc="The output file to right marked records to")
    public File OUTPUT;

    @Option(shortName="M", doc="File to write duplication metrics to")
    public File METRICS_FILE;

    @Option(doc="Comment(s) to include in the output file's header.", optional=true, shortName="CO")
    public List<String> COMMENT = new ArrayList<String>();

    @Option(doc="If true do not write duplicates to the output file instead of writing them with appropriate flags set.")
    public boolean REMOVE_DUPLICATES = false;

    @Option(doc="If true, assume that the input file is coordinate sorted, even if the header says otherwise.",
            shortName=StandardOptionDefinitions.ASSUME_SORTED_SHORT_NAME)
    public boolean ASSUME_SORTED = false;

    @Option(doc="This option is obsolete.  ReadEnds will always be spilled to disk.", shortName="MAX_SEQS")
    public int MAX_SEQUENCES_FOR_DISK_READ_ENDS_MAP = 50000;

    @Option(doc="Maximum number of file handles to keep open when spilling read ends to disk.  " + "" +
            "Set this number a little lower than the per-process maximum number of file that may be open.  " +
            "This number can be found by executing the 'ulimit -n' command on a Unix system.", shortName = "MAX_FILE_HANDLES")
    public int MAX_FILE_HANDLES_FOR_READ_ENDS_MAP = 8000;

    @Option(doc="This number, plus the maximum RAM available to the JVM, determine the memory footprint used by " +
            "some of the sorting collections.  If you are running out of memory, try reducing this number.")
    public double SORTING_COLLECTION_SIZE_RATIO = 0.25;

    private SortingCollection<ReadEnds> pairSort;
    private SortingCollection<ReadEnds> fragSort;
    private SortingLongCollection duplicateIndexes;
    private int numDuplicateIndices = 0;

    final private Map<String,Short> libraryIds = new HashMap<String,Short>();
    private short nextLibraryId = 1;

    // Variables used for optical duplicate detection and tracking
    private final Histogram<Short> opticalDupesByLibraryId = new Histogram<Short>();

    /** Stock main method. */
    public static void main(final String[] args) {
        System.exit(new MarkDuplicates().instanceMain(args));
    }

    /**
     * Main work method.  Reads the BAM file once and collects sorted information about
     * the 5' ends of both ends of each read (or just one end in the case of pairs).
     * Then makes a pass through those determining duplicates before re-reading the
     * input file and writing it out with duplication flags set correctly.
     */
    protected int doWork() {
        for (final File f : INPUT) IoUtil.assertFileIsReadable(f);
        IoUtil.assertFileIsWritable(OUTPUT);
        IoUtil.assertFileIsWritable(METRICS_FILE);

        reportMemoryStats("Start of doWork");
        log.info("Reading input file and constructing read end information.");
        buildSortedReadEndLists();
        reportMemoryStats("After buildSortedReadEndLists");
        //How are duplicate Indexes generated?
        generateDuplicateIndexes();
        reportMemoryStats("After generateDuplicateIndexes");
        log.info("Marking " + this.numDuplicateIndices + " records as duplicates.");
        log.info("Found " + ((long) this.opticalDupesByLibraryId.getSumOfValues()) + " optical duplicate clusters.");

        final Map<String,DuplicationMetrics> metricsByLibrary = new HashMap<String,DuplicationMetrics>();
        final SamHeaderAndIterator headerAndIterator = openInputs();
        final SAMFileHeader header = headerAndIterator.header;

        //Initialise an output SAM file
        final SAMFileHeader outputHeader = header.clone();
        outputHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        for (final String comment : COMMENT) outputHeader.addComment(comment);
        final SAMFileWriter out = new SAMFileWriterFactory().makeSAMOrBAMWriter(outputHeader,
                                                                                true,
                                                                                OUTPUT);

        // Now copy over the file while marking all the necessary indexes as duplicates
        long recordInFileIndex = 0;
        long nextDuplicateIndex = (this.duplicateIndexes.hasNext() ? this.duplicateIndexes.next(): -1);

        for(final SAMReadGroupRecord readGroup : header.getReadGroups()) {
            final String library = readGroup.getLibrary();
            DuplicationMetrics metrics = metricsByLibrary.get(library);
            if (metrics == null) {
                metrics = new DuplicationMetrics();
                metrics.LIBRARY = library;
                metricsByLibrary.put(library, metrics);
            }
        }

        long written = 0;
        final CloseableIterator<SAMRecord> iterator = headerAndIterator.iterator;
        while (iterator.hasNext()) {
            final SAMRecord rec = iterator.next();
            if (!rec.getNotPrimaryAlignmentFlag()) {
                final String library = getLibraryName(header, rec);
                DuplicationMetrics metrics = metricsByLibrary.get(library);
                if (metrics == null) {
                    metrics = new DuplicationMetrics();
                    metrics.LIBRARY = library;
                    metricsByLibrary.put(library, metrics);
                }

                // First bring the simple metrics up to date
                if (rec.getReadUnmappedFlag()) {
                    ++metrics.UNMAPPED_READS;
                }
                else if (!rec.getReadPairedFlag() || rec.getMateUnmappedFlag()) {
                    ++metrics.UNPAIRED_READS_EXAMINED;
                }
                else {
                    ++metrics.READ_PAIRS_EXAMINED; // will need to be divided by 2 at the end
                }


                if (recordInFileIndex == nextDuplicateIndex) {
                    rec.setDuplicateReadFlag(true);

                    // Update the duplication metrics
                    if (!rec.getReadPairedFlag() || rec.getMateUnmappedFlag()) {
                        ++metrics.UNPAIRED_READ_DUPLICATES;
                    }
                    else {
                        ++metrics.READ_PAIR_DUPLICATES;// will need to be divided by 2 at the end
                    }

                    // Now try and figure out the next duplicate index
                    if (this.duplicateIndexes.hasNext()) {
                        nextDuplicateIndex = this.duplicateIndexes.next();
                    } else {
                        // Only happens once we've marked all the duplicates
                        nextDuplicateIndex = -1;
                    }
                }
                else {
                    rec.setDuplicateReadFlag(false);
                }
            }
            recordInFileIndex++;

            if (this.REMOVE_DUPLICATES && rec.getDuplicateReadFlag()) {
                // do nothing
            }
            else {
                out.addAlignment(rec);
                if (++written % 10000000 == 0) {
                    log.info("Written " + written + " records.");
                }
            }
        }

        this.duplicateIndexes.cleanup();

        reportMemoryStats("Before output close");
        out.close();
        reportMemoryStats("After output close");


        // Write out the metrics
        final MetricsFile<DuplicationMetrics,Double> file = getMetricsFile();
        for (final Map.Entry<String,DuplicationMetrics> entry : metricsByLibrary.entrySet()) {
            final String libraryName = entry.getKey();
            final DuplicationMetrics metrics = entry.getValue();

            metrics.READ_PAIRS_EXAMINED = metrics.READ_PAIRS_EXAMINED / 2;
            metrics.READ_PAIR_DUPLICATES = metrics.READ_PAIR_DUPLICATES / 2;

            // Add the optical dupes to the metrics
            final Short libraryId = this.libraryIds.get(libraryName);
            if (libraryId != null) {
                final Histogram<Short>.Bin bin = this.opticalDupesByLibraryId.get(libraryId);
                if (bin != null) {
                    metrics.READ_PAIR_OPTICAL_DUPLICATES = (long) bin.getValue();
                }
            }
            metrics.calculateDerivedMetrics();
            file.addMetric(metrics);
        }

        if (metricsByLibrary.size() == 1) {
            file.setHistogram(metricsByLibrary.values().iterator().next().calculateRoiHistogram());
        }

        file.write(METRICS_FILE);

        return 0;
    }

    /** Little class used to package up a header and an iterable/iterator. */
    private static final class SamHeaderAndIterator {
        final SAMFileHeader header;
        final CloseableIterator<SAMRecord> iterator;

        private SamHeaderAndIterator(final SAMFileHeader header, final CloseableIterator<SAMRecord> iterator) {
            this.header = header;
            this.iterator = iterator;
        }
    }

    /**
     * Since MarkDuplicates reads it's inputs more than once this method does all the opening
     * and checking of the inputs.
     */
    private SamHeaderAndIterator openInputs() {
        final List<SAMFileHeader> headers = new ArrayList<SAMFileHeader>(INPUT.size());
        final List<SAMFileReader> readers = new ArrayList<SAMFileReader>(INPUT.size());

        for (final File f : INPUT) {
            final SAMFileReader reader = new SAMFileReader(f);
            final SAMFileHeader header = reader.getFileHeader();

            if (!ASSUME_SORTED && header.getSortOrder() != SortOrder.coordinate) {
                throw new PicardException("Input file " + f.getAbsolutePath() + " is not coordinate sorted.");
            }

            headers.add(header);
            readers.add(reader);
        }

        if (headers.size() == 1) {
            return new SamHeaderAndIterator(headers.get(0), readers.get(0).iterator());
        }
        else {
            final SamFileHeaderMerger headerMerger = new SamFileHeaderMerger(SortOrder.coordinate, headers, false);
            final MergingSamRecordIterator iterator = new MergingSamRecordIterator(headerMerger, readers, ASSUME_SORTED);
            return new SamHeaderAndIterator(headerMerger.getMergedHeader(), iterator);
        }
    }

    /** Print out some quick JVM memory stats. */
    // Why do we need to print these memory stats? Perhaps it helps debugging cases, where the hash map 
    // takes a lot of space
    private void reportMemoryStats(final String stage) {
        System.gc();
        final Runtime runtime = Runtime.getRuntime();
        log.info(stage + " freeMemory: " + runtime.freeMemory() + "; totalMemory: " + runtime.totalMemory() +
                "; maxMemory: " + runtime.maxMemory());
    }

    /**
     * Goes through all the records in a file and generates a set of ReadEnds objects that
     * hold the necessary information (reference sequence, 5' read coordinate) to do
     * duplication, caching to disk as necssary to sort them.
     */
    private void buildSortedReadEndLists() {
        //Good for diagnostics - tells in advance how many data points can be stored in memory
        final int maxInMemory = (int) ((Runtime.getRuntime().maxMemory() * SORTING_COLLECTION_SIZE_RATIO) / ReadEnds.SIZE_OF);
        log.info("Will retain up to " + maxInMemory + " data points before spilling to disk.");

        //ReadEnds is a class that contains sequences and positions of read pairs and their orientation
        this.pairSort = SortingCollection.newInstance(ReadEnds.class,
                                                      new ReadEndsCodec(),
                                                      new ReadEndsComparator(),
                                                      maxInMemory,
                                                      TMP_DIR);

        this.fragSort = SortingCollection.newInstance(ReadEnds.class,
                                                      new ReadEndsCodec(),
                                                      new ReadEndsComparator(),
                                                      maxInMemory,
                                                      TMP_DIR);

        final SamHeaderAndIterator headerAndIterator = openInputs();// A list iterator for a list of readers
        final SAMFileHeader header = headerAndIterator.header;
        //What is the max file handles for read ends map? What are multiple file handles used for?
        final ReadEndsMap tmp = new DiskReadEndsMap(MAX_FILE_HANDLES_FOR_READ_ENDS_MAP);
        long index = 0;
        //What is a closeable iterator? Is it so that the iterator can be used to close the file stream?
        final CloseableIterator<SAMRecord> iterator = headerAndIterator.iterator;

        //Iterating through all the SAM records
        while (iterator.hasNext()) {
            final SAMRecord rec = iterator.next();
            if (rec.getReadUnmappedFlag()) {
                if (rec.getReferenceIndex() == -1) {
                    // When we hit the unmapped reads with no coordinate, no reason to continue.
                    // Why? Can there not be more mapped reads after the unmapped read with no coordinate?
                    // Does sorting place such reads at the end?
                    break;
                }
                // If this read is unmapped but sorted with the mapped reads, just skip it.
            }
            else if (!rec.getNotPrimaryAlignmentFlag()){
                final ReadEnds fragmentEnd = buildReadEnds(header, index, rec);
                this.fragSort.add(fragmentEnd);

                // If the read is paired and the mate does not have an unmapped flag
                if (rec.getReadPairedFlag() && !rec.getMateUnmappedFlag()) {
                    final String key = rec.getAttribute(ReservedTagConstants.READ_GROUP_ID) + ":" + rec.getReadName();
                    // What is the reference index here? It returns a mate sequence index which must be the same
                    // as when the key was added to the map (to optimize storage and retrieval)
                    // rec.getReferenceIndex refers to the index in the sequence dictionary
                    ReadEnds pairedEnds = tmp.remove(rec.getReferenceIndex(), key);

                    // See if we've already seen the first end or not
                    if (pairedEnds == null) {
                        // If first end not seen, build such a read end and add it to tmp read end map
                        pairedEnds = buildReadEnds(header, index, rec);
                        tmp.put(pairedEnds.read2Sequence, key, pairedEnds);
                    }
                    else {
                        // If the first end has been seen, 
                        final int sequence = fragmentEnd.read1Sequence;
                        final int coordinate = fragmentEnd.read1Coordinate;

                        // If the second read is actually later, just add the second read data, else flip the reads
                        if (sequence > pairedEnds.read1Sequence ||
                                (sequence == pairedEnds.read1Sequence && coordinate >= pairedEnds.read1Coordinate)) {
                            pairedEnds.read2Sequence    = sequence;
                            pairedEnds.read2Coordinate  = coordinate;
                            pairedEnds.read2IndexInFile = index;
                            pairedEnds.orientation = getOrientationByte(pairedEnds.orientation == ReadEnds.R,
                                                                        rec.getReadNegativeStrandFlag());
                        }
                        else {
                            pairedEnds.read2Sequence    = pairedEnds.read1Sequence;
                            pairedEnds.read2Coordinate  = pairedEnds.read1Coordinate;
                            pairedEnds.read2IndexInFile = pairedEnds.read1IndexInFile;
                            pairedEnds.read1Sequence    = sequence;
                            pairedEnds.read1Coordinate  = coordinate;
                            pairedEnds.read1IndexInFile = index;
                            pairedEnds.orientation = getOrientationByte(rec.getReadNegativeStrandFlag(),
                                                                        pairedEnds.orientation == ReadEnds.R);
                        }

                        pairedEnds.score += getScore(rec);
                        this.pairSort.add(pairedEnds);
                    }
                }
            }

            // Print out some stats every 1m reads
            if (++index % 1000000 == 0) {
                log.info("Read " + index + " records. Tracking " + tmp.size() + " as yet unmatched pairs. " +
                tmp.sizeInRam() + " records in RAM.  Last sequence index: " + rec.getReferenceIndex());
            }
        }

        log.info("Read " + index + " records. " + tmp.size() + " pairs never matched.");
        iterator.close();

        // Tell these collections to free up memory if possible.
        this.pairSort.doneAdding();
        this.fragSort.doneAdding();
    }

    /** Builds a read ends object that represents a single read. */
    private ReadEnds buildReadEnds(final SAMFileHeader header, final long index, final SAMRecord rec) {
        final ReadEnds ends = new ReadEnds();
        ends.read1Sequence    = rec.getReferenceIndex();
        ends.read1Coordinate  = rec.getReadNegativeStrandFlag() ? rec.getUnclippedEnd() : rec.getUnclippedStart();
        ends.orientation = rec.getReadNegativeStrandFlag() ? ReadEnds.R : ReadEnds.F;
        ends.read1IndexInFile = index;
        ends.score = getScore(rec);

        // Doing this lets the ends object know that it's part of a pair
        if (rec.getReadPairedFlag() && !rec.getMateUnmappedFlag()) {
            ends.read2Sequence = rec.getMateReferenceIndex();
        }

        // Fill in the library ID
        ends.libraryId = getLibraryId(header, rec);

        // Fill in the location information for optical duplicates
        if (addLocationInformation(rec.getReadName(), ends)) {
            // calculate the RG number (nth in list)
            ends.readGroup = 0;
            final String rg = (String) rec.getAttribute("RG");
            final List<SAMReadGroupRecord> readGroups = header.getReadGroups();

            if (rg != null && readGroups != null) {
                for (SAMReadGroupRecord readGroup : readGroups) {
                    if (readGroup.getReadGroupId().equals(rg)) break;
                    else ends.readGroup++;
                }
            }
        }

        return ends;
    }

    /** Get the library ID for the given SAM record. */
    private short getLibraryId(final SAMFileHeader header, final SAMRecord rec) {
        final String library = getLibraryName(header, rec);
        Short libraryId = this.libraryIds.get(library);

        if (libraryId == null) {
            libraryId = this.nextLibraryId++;
            this.libraryIds.put(library, libraryId);
        }

        return libraryId;
    }

    /**
     * Gets the library name from the header for the record. If the RG tag is not present on
     * the record, or the library isn't denoted on the read group, a constant string is
     * returned.
     */
    private String getLibraryName(final SAMFileHeader header, final SAMRecord rec) {
        final String readGroupId = (String) rec.getAttribute("RG");

        if (readGroupId != null) {
            final SAMReadGroupRecord rg = header.getReadGroup(readGroupId);
            if (rg != null) {
                return rg.getLibrary();
            }
        }

        return "Unknown Library";
    }

    /**
     * Returns a single byte that encodes the orientation of the two reads in a pair.
     */
    private byte getOrientationByte(final boolean read1NegativeStrand, final boolean read2NegativeStrand) {
        if (read1NegativeStrand) {
            if (read2NegativeStrand)  return ReadEnds.RR;
            else return ReadEnds.RF;
        }
        else {
            if (read2NegativeStrand)  return ReadEnds.FR;
            else return ReadEnds.FF;
        }
    }



    /** Calculates a score for the read which is the sum of scores over Q20. */
    private short getScore(final SAMRecord rec) {
        short score = 0;
        for (final byte b : rec.getBaseQualities()) {
            if (b >= 15) score += b;
        }

        return score;
    }

    /**
     * Goes through the accumulated ReadEnds objects and determines which of them are
     * to be marked as duplicates.
     *
     * @return an array with an ordered list of indexes into the source file
     */
    private void generateDuplicateIndexes() {
        final int maxInMemory = (int) ((Runtime.getRuntime().maxMemory() * 0.25) / SortingLongCollection.SIZEOF);
        log.info("Will retain up to " + maxInMemory + " duplicate indices before spilling to disk.");
        this.duplicateIndexes = new SortingLongCollection(maxInMemory, TMP_DIR.toArray(new File[TMP_DIR.size()]));

        ReadEnds firstOfNextChunk = null;
        final List<ReadEnds> nextChunk  = new ArrayList<ReadEnds>(200);

        // First just do the pairs
        log.info("Traversing read pair information and detecting duplicates.");
        for (final ReadEnds next : this.pairSort) {
            if (firstOfNextChunk == null) {
                firstOfNextChunk = next;
                nextChunk.add(firstOfNextChunk);
            }
            else if (areComparableForDuplicates(firstOfNextChunk, next, true)) {
                nextChunk.add(next);
            }
            else {
                if (nextChunk.size() > 1) {
                    markDuplicatePairs(nextChunk);
                }

                nextChunk.clear();
                nextChunk.add(next);
                firstOfNextChunk = next;
            }
        }
        markDuplicatePairs(nextChunk);
        this.pairSort.cleanup();
        this.pairSort = null;

        // Now deal with the fragments
        log.info("Traversing fragment information and detecting duplicates.");
        boolean containsPairs = false;
        boolean containsFrags = false;

        for (final ReadEnds next : this.fragSort) {
            if (firstOfNextChunk != null && areComparableForDuplicates(firstOfNextChunk, next, false)) {
                nextChunk.add(next);
                containsPairs = containsPairs || next.isPaired();
                containsFrags = containsFrags || !next.isPaired();
            }
            else {
                if (nextChunk.size() > 1 && containsFrags) {
                    markDuplicateFragments(nextChunk, containsPairs);
                }

                nextChunk.clear();
                nextChunk.add(next);
                firstOfNextChunk = next;
                containsPairs = next.isPaired();
                containsFrags = !next.isPaired();
            }
        }
        markDuplicateFragments(nextChunk, containsPairs);
        this.fragSort.cleanup();
        this.fragSort = null;

        log.info("Sorting list of duplicate records.");
        this.duplicateIndexes.doneAddingStartIteration();
    }

    private boolean areComparableForDuplicates(final ReadEnds lhs, final ReadEnds rhs, final boolean compareRead2) {
        boolean retval =  lhs.libraryId       == rhs.libraryId &&
                          lhs.read1Sequence   == rhs.read1Sequence &&
                          lhs.read1Coordinate == rhs.read1Coordinate &&
                          lhs.orientation     == rhs.orientation;

        if (retval && compareRead2) {
            retval = lhs.read2Sequence   == rhs.read2Sequence &&
                     lhs.read2Coordinate == rhs.read2Coordinate;
        }

        return retval;
    }

    private void addIndexAsDuplicate(final long bamIndex) {
        this.duplicateIndexes.add(bamIndex);
        ++this.numDuplicateIndices;
    }

    /**
     * Takes a list of ReadEnds objects and removes from it all objects that should
     * not be marked as duplicates.
     *
     * @param list
     */
    private void markDuplicatePairs(final List<ReadEnds> list) {
        short maxScore = 0;
        ReadEnds best = null;

        for (final ReadEnds end : list) {
            if (end.score > maxScore || best == null) {
                maxScore = end.score;
                best = end;
            }
        }

        for (final ReadEnds end : list) {
            if (end != best) {
                addIndexAsDuplicate(end.read1IndexInFile);
                addIndexAsDuplicate(end.read2IndexInFile);
            }
        }

        trackOpticalDuplicates(list);
    }

    /**
     * Looks through the set of reads and identifies how many of the duplicates are
     * in fact optical duplicates, and stores the data in the instance level histogram.
     */
    private void trackOpticalDuplicates(final List<ReadEnds> list) {
        final boolean[] opticalDuplicateFlags = findOpticalDuplicates(list, OPTICAL_DUPLICATE_PIXEL_DISTANCE);

        int opticalDuplicates = 0;
        for (boolean b: opticalDuplicateFlags) if (b) ++opticalDuplicates;
        if (opticalDuplicates > 0) {
            this.opticalDupesByLibraryId.increment(list.get(0).libraryId, opticalDuplicates);
        }
    }

    /**
     * Takes a list of ReadEnds objects and removes from it all objects that should
     * not be marked as duplicates.
     *
     * @param list
     */
    private void markDuplicateFragments(final List<ReadEnds> list, final boolean containsPairs) {
        if (containsPairs) {
            for (final ReadEnds end : list) {
                if (!end.isPaired()) addIndexAsDuplicate(end.read1IndexInFile);
            }
        }
        else {
            short maxScore = 0;
            ReadEnds best = null;
            for (final ReadEnds end : list) {
                if (end.score > maxScore || best == null) {
                    maxScore = end.score;
                    best = end;
                }
            }

            for (final ReadEnds end : list) {
                if (end != best) {
                    addIndexAsDuplicate(end.read1IndexInFile);
                }
            }
        }
    }

    /** Comparator for ReadEnds that orders by read1 position then pair orientation then read2 position. */
    static class ReadEndsComparator implements Comparator<ReadEnds> {
        public int compare(final ReadEnds lhs, final ReadEnds rhs) {
            int retval = lhs.libraryId - rhs.libraryId;
            if (retval == 0) retval = lhs.read1Sequence - rhs.read1Sequence;
            if (retval == 0) retval = lhs.read1Coordinate - rhs.read1Coordinate;
            if (retval == 0) retval = lhs.orientation - rhs.orientation;
            if (retval == 0) retval = lhs.read2Sequence   - rhs.read2Sequence;
            if (retval == 0) retval = lhs.read2Coordinate - rhs.read2Coordinate;
            if (retval == 0) retval = (int) (lhs.read1IndexInFile - rhs.read1IndexInFile);
            if (retval == 0) retval = (int) (lhs.read2IndexInFile - rhs.read2IndexInFile);

            return retval;
        }
    }
}
