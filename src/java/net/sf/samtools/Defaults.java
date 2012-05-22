package net.sf.samtools;

/**
 * Embodies defaults for global values that affect how the SAM JDK operates. Defaults are encoded in the class
 * and are also overridable using system properties.
 *
 * @author Tim Fennell
 */
public class Defaults {
    /** Should BAM index files be created when writing out coordinate sorted BAM files?  Default = false. */
    public static final boolean CREATE_INDEX;

    /** Should MD5 files be created when writing out SAM and BAM files?  Default = false. */
    public static final boolean CREATE_MD5;

    /**
      * Enable asynchronous I/O when writing out SAM and BAM files (one writer thread per file).
      * Default = false.
      * <p> If MT_WORKER_THREADS > 0 and MT_COMPRESS_QUEUE_SIZE > 0, then
      * BAM record output will always be asynchronous, and setting this value to true
      * will probably not increase performance, but may degrade it.
     * <p> This value can be changed by setting the Java property "samjdk.use_async_io".
      */
    public static final boolean USE_ASYNC_IO;

    /**
     * Compresion level to be used for writing BAM and other block-compressed outputs.  Default = 5.
     * <p> This value can be changed by setting the Java property "samjdk.compression_level".
     */
    public static final int COMPRESSION_LEVEL;

    /**
     * Buffer size, in bytes, used whenever reading/writing files or streams.  Default = 128k.
     * <p> This value can be changed by setting the Java property "samjdk.buffer_size".
     */
    public static final int BUFFER_SIZE;

    /**
     * Number of Worker threads.  Default = 0.
     * <p> If this value is non-zero, multi-threading is enabled.
     * <p> This value can be changed by setting the Java property "samjdk.mt.num_workers".
     */
    public static final int MT_WORKER_THREADS;

    /**
     * Size of the job-submission queue for the Worker threads.
     * Default = 200.
     * <p> This value can be changed by setting the Java property "samjdk.mt.worker_queue_size".
     */
    public static final int MT_WORKER_QUEUE_SIZE;

    /**
     * The number of BGZF blocks to batch together for decompression.
     * Default = 200.
     * <p> Used to reduce mult-threading overhead.
     * <p> This value can be changed by setting the Java property "samjdk.mt.decompress_batch_size".
     */
    public static final int MT_DECOMPRESS_BATCH_SIZE;

    /**
       The job-queue size for each ParallelBlockCompressedInputStream instance.
       Default = 6.
       <p>Effects on each ParallelBlockCompressedInputStream instance:<ul>
       <li> Limits the amount of read-ahead to <span style='white-space:nowrap'>
            (MT_DECOMPRESS_BATCH_SIZE * MT_DECOMPRESS_QUEUE_SIZE)</span> blocks.
       <li> Limits the number of concurrent decompression jobs to <span style='white-space:nowrap'>
            min(MT_WORKER_THREADS, MT_DECOMPRESS_QUEUE_SIZE)</span> blocks.</ul>
       <p>Global effects: If MT_DECOMPRESS_QUEUE_SIZE > 0 and MT_WORKER_THREADS > 0,
            a ParallelBlockCompressedInputStream is:<ul>
            <li> returned
            by {@link net.sf.samtools.util.BlockCompressedStreamFactory#makeBlockCompressedInputStream
            BlockCompressedStreamFactory.makeBlockCompressedInputStream}
            <li>used internally by {@link net.sf.samtools.SAMFileReader SAMFileReader} to read BAM files.
       </ul>
       <p> This value can be changed by setting the Java property "samjdk.mt.max_decompress_jobs".
     */
    public static final int MT_DECOMPRESS_QUEUE_SIZE;

    /**
       The job-queue size for each ParallelBlockCompressedOutputStream instance.
       Default = MT_WORKER_THREADS.
       <p> Recommendation: set this value just large enough to keep a subset of the CPUs fully saturated.
       <p>Effects on each ParallelBlockCompressedOutputStream instance:<ul>
       <li> Limits the number of concurrent compression jobs to <span style='white-space:nowrap'>
            min(MT_WORKER_THREADS, MT_COMPRESS_QUEUE_SIZE)</span> blocks.</ul>
       <p>Global effects: If MT_COMPRESS_QUEUE_SIZE > 0 and MT_WORKER_THREADS > 0,
            a ParallelBlockCompressedOutputStream is:<ul>
            <li> returned
            by {@link net.sf.samtools.util.BlockCompressedStreamFactory#makeBlockCompressedOutputStream
            BlockCompressedStreamFactory.makeBlockCompressedOutputStream}
            <li> used internally by {@link BAMFileWriter} as its output stream
       </ul>
       <p> This value can be changed by setting the Java property "samjdk.mt.max_compress_jobs".
     */
    public static final int MT_COMPRESS_QUEUE_SIZE;

    /**
       The job-queue size for each ParallelBAMFileReader instance.
       Default = 10.
       <p>Effects on each ParallelBAMFileReader instance:<ul>
       <li> Limits the number of concurrent decode jobs to <span style='white-space:nowrap'>
            min(MT_WORKER_THREADS, MT_BAMDECODE_QUEUE_SIZE)</span> blocks.</ul>
       <p>Global effects: If MT_BAMDECODE_QUEUE_SIZE > 0 and MT_WORKER_THREADS > 0:<ul>
           <li> a {@link ParallelBAMFileReader} is used by {@link SAMFileReader} instead of
           a {@link BAMFileReader} when reading BAM files.</ul>
       <p> This value can be changed by setting the Java property "samjdk.mt.max_bamdecode_jobs".
     */
    public static final int MT_BAMDECODE_QUEUE_SIZE;

    /**
     * The multi-threading debugging level.
     * Default = 1. <p> Values greater than 0 will cause debugging information to be sent to System.err.
     * <p> This value can be changed by setting the Java property "samjdk.mt.debug_level".
     */
    public static final int MT_DEBUG_LEVEL;

    static {
        CREATE_INDEX      = getBooleanProperty("create_index", false);
        CREATE_MD5        = getBooleanProperty("create_md5", false);
        USE_ASYNC_IO      = getBooleanProperty("use_async_io", false);
        COMPRESSION_LEVEL = getIntProperty("compression_level", 5);
        BUFFER_SIZE       = getIntProperty("buffer_size", 1024 * 128);
        // multi-threading stuff
        MT_WORKER_THREADS = getIntProperty("mt.num_workers", 0);
        MT_WORKER_QUEUE_SIZE  = getIntProperty("mt.worker_queue_size", 100);
        MT_DECOMPRESS_BATCH_SIZE = getIntProperty("mt.decompress_batch_size", 200);
        MT_DECOMPRESS_QUEUE_SIZE = getIntProperty("mt.decompress_queue_size", 6);
        MT_COMPRESS_QUEUE_SIZE = getIntProperty("mt.compress_queue_size", MT_WORKER_THREADS);
        MT_BAMDECODE_QUEUE_SIZE = getIntProperty("mt.bamdecode_queue_size", 10);
        MT_DEBUG_LEVEL = getIntProperty("mt.debug_level", 1);
    }

    /** Gets a string system property, prefixed with "samjdk." using the default if the property does not exist.*/
    private static String getStringProperty(final String name, final String def) {
        return System.getProperty("samjdk." + name, def);
    }

    /** Gets a boolean system property, prefixed with "samjdk." using the default if the property does not exist.*/
    private static boolean getBooleanProperty(final String name, final boolean def) {
        final String value = getStringProperty(name, new Boolean(def).toString());
        return Boolean.parseBoolean(value);
    }

    /** Gets an int system property, prefixed with "samjdk." using the default if the property does not exist.*/
    private static int getIntProperty(final String name, final int def) {
        final String value = getStringProperty(name, new Integer(def).toString());
        return Integer.parseInt(value);
    }
}
