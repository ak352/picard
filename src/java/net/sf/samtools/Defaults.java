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

    /** Should asynchronous I/O be used when writing out SAM and BAM files (one thread per file).  Default = false. */
    public static final boolean USE_ASYNC_IO;

    /** Compresion level to be used for writing BAM and other block-compressed outputs.  Default = 5. */
    public static final int COMPRESSION_LEVEL;

    /** Buffer size, in bytes, used whenever reading/writing files or streams.  Default = 128k. */
    public static final int BUFFER_SIZE;

    /**
     * Number of threads to use for inflating (decompressing) and deflating (compressing) BGZF blocks.  Default = 0.
     */
    public static final int NUM_PBGZF_THREADS;

    /**
     * Size of BGZF block queues used by ParallelBlockCompressedInputStream/ParallelBlockCompressedOutputStream
     * Default = 100. Only effective when NUM_PBGZF_THREADS > 0.
     */
    public static final int PBGZF_QUEUE_SIZE;

    /**
     * Disables parallel compression of blocks.
     * Default = false.
     */
    public static final boolean DISABLE_PBGZF_COMPRESSION;

    /**
     * Disables parallel decompression of blocks.
     * Default = false.
     */
    public static final boolean DISABLE_PBGZF_DECOMPRESSION;

    static {
        CREATE_INDEX      = getBooleanProperty("create_index", false);
        CREATE_MD5        = getBooleanProperty("create_md5", false);
        USE_ASYNC_IO      = getBooleanProperty("use_async_io", false);
        COMPRESSION_LEVEL = getIntProperty("compression_level", 5);
        BUFFER_SIZE       = getIntProperty("buffer_size", 1024 * 128);
        NUM_PBGZF_THREADS = getIntProperty("num_pbgzf_threads", 1);
        PBGZF_QUEUE_SIZE  = getIntProperty("deflate_queue_size", 100);
        DISABLE_PBGZF_COMPRESSION = getBooleanProperty("disable_pbgzf_compression", false);
        DISABLE_PBGZF_DECOMPRESSION = getBooleanProperty("disable_pbgzf_decompression", false);
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
