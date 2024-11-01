package com.yscope.logging.log4j2;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.github.luben.zstd.Zstd;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * This class extends {@link AbstractBufferedRollingFileAppender} to append to CLP compressed
 * IR-stream files and rollover based on the amount of uncompressed and compressed data written to a
 * file. Derived classes are expected to handle synchronization (e.g., uploading to remote storage).
 * <p>
 * Rollover based on the amount of uncompressed data written to file allows us to ensure that the
 * file remains manageable when decompressed for viewing, etc.
 * <p>
 * Rollover based on the amount of compressed data written to file allows us to ensure the file
 * is large enough to amortize filesystem overhead, and small enough to be performant when uploading
 * to remote storage as well as when accessing and searching the compressed file.
 */
public abstract class AbstractClpIrBufferedRollingFileAppender
        extends
        AbstractBufferedRollingFileAppender {
    public static final String CLP_COMPRESSED_IRSTREAM_FILE_EXTENSION = ".clp.zst";
    public static final String PLUGIN_NAME = "AbstractClpIrBufferedRollingFileAppender";

    // Appender settings, some of which may be set by Log4j through reflection.
    // For descriptions of the properties, see their setters below.
    private String outputDir;
    // CLP streaming compression parameters
    private int compressionLevel;
    private boolean useFourByteEncoding;
    private boolean closeFrameOnFlush;
    private long rolloverCompressedSizeThreshold;
    private long rolloverUncompressedSizeThreshold;

    private long compressedSizeSinceLastRollover = 0L; // Bytes
    private long uncompressedSizeSinceLastRollover = 0L; // Bytes

    // This instance variable should be up-to-date at all times
    private ClpIrFileAppender clpIrFileAppender = null;

    public AbstractClpIrBufferedRollingFileAppender(
            final String name,
            boolean ignoreExceptions,
            final PatternLayout layout,
            final Filter filter,
            String baseName,
            boolean closeOnShutdown,
            String csvFlushHardTimeouts,
            String csvFlushSoftTimeouts,
            long shutdownSoftTimeout,
            long shutdownHardTimeout,
            int timeoutCheckPeriod,
            int compressionLevel,
            long rolloverCompressedSizeThreshold,
            long rolloverUncompressedSizeThreshold,
            boolean useFourByteEncoding,
            String outputDir,
            boolean closeFrameOnFlush
    ) {
        super(
                name,
                ignoreExceptions,
                layout,
                filter,
                baseName,
                closeOnShutdown,
                csvFlushHardTimeouts,
                csvFlushSoftTimeouts,
                shutdownSoftTimeout,
                shutdownHardTimeout,
                timeoutCheckPeriod
        );
        _setCloseFrameOnFlush(closeFrameOnFlush);
        _setCompressionLevel(compressionLevel);
        _setRolloverCompressedSizeThreshold(rolloverCompressedSizeThreshold);
        _setRolloverUncompressedSizeThreshold(rolloverUncompressedSizeThreshold);
        _setUseFourByteEncoding(useFourByteEncoding);
        _setOutputDir(outputDir);
    }

    private void _setCloseFrameOnFlush(boolean closeFrameOnFlush) {
        this.closeFrameOnFlush = closeFrameOnFlush;
    }

    /**
     * Sets the compression level for the appender's streaming compressor
     *
     * @param compressionLevel The compression level between 1 and 22
     */
    private void _setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    /**
     * Sets the threshold for the file's compressed size at which rollover should be triggered.
     *
     * @param rolloverCompressedSizeThreshold The threshold size in bytes
     */
    private void _setRolloverCompressedSizeThreshold(long rolloverCompressedSizeThreshold) {
        this.rolloverCompressedSizeThreshold = rolloverCompressedSizeThreshold;
    }

    /**
     * Sets the threshold for the file's uncompressed size at which rollover should be triggered.
     *
     * @param rolloverUncompressedSizeThreshold The threshold size in bytes
     */
    private void _setRolloverUncompressedSizeThreshold(long rolloverUncompressedSizeThreshold) {
        this.rolloverUncompressedSizeThreshold = rolloverUncompressedSizeThreshold;
    }

    /**
     * @param useFourByteEncoding Whether to use CLP's four-byte encoding instead of the default
     * eight-byte encoding
     */
    private void _setUseFourByteEncoding(boolean useFourByteEncoding) {
        this.useFourByteEncoding = useFourByteEncoding;
    }

    /** @param outputDir The output directory path for log files */
    private void _setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    /** @return The uncompressed size of all log events processed by this appender in bytes. */
    public long getUncompressedSize() {
        return uncompressedSizeSinceLastRollover + clpIrFileAppender.getUncompressedSize();
    }

    /** @return The compressed size of all log events processed by this appender in bytes. */
    public long getCompressedSize() {
        return compressedSizeSinceLastRollover + clpIrFileAppender.getCompressedSize();
    }

    public int getCompressionLevel() { return compressionLevel; }

    @Override
    protected void activateOptionsHook(long currentTimestamp) throws IOException {
        String fileName = computeLogFileName(getBaseName(), currentTimestamp);
        String filePath = computeLogFilePath(fileName);
        if (!(getLayout() instanceof PatternLayout)) {
            throw new RuntimeException("log4j2-appender currently only supports Pattern layout");
        }
        clpIrFileAppender = new ClpIrFileAppender(
                filePath,
                getName(),
                ignoreExceptions(),
                (PatternLayout)getLayout(),
                getFilter(),
                compressionLevel,
                useFourByteEncoding,
                closeFrameOnFlush
        );
    }

    @Override
    protected void closeHook() {
        clpIrFileAppender.stop(DEFAULT_STOP_TIMEOUT, DEFAULT_STOP_TIMEUNIT);
    }

    @Override
    protected boolean rolloverRequired() {
        return clpIrFileAppender.getCompressedSize() >= rolloverCompressedSizeThreshold
                || clpIrFileAppender.getUncompressedSize() >= rolloverUncompressedSizeThreshold;
    }

    @Override
    protected void startNewLogFile(long lastEventTimestamp) throws IOException {
        compressedSizeSinceLastRollover += clpIrFileAppender.getCompressedSize();
        uncompressedSizeSinceLastRollover += clpIrFileAppender.getUncompressedSize();
        String fileName = computeLogFileName(getBaseName(), lastEventTimestamp);
        String filePath = computeLogFilePath(fileName);
        clpIrFileAppender.startNewFile(filePath);
    }

    @Override
    public void appendHook(LogEvent event) {
        clpIrFileAppender.append(event);
    }

    @Override
    public void flush() throws IOException {
        clpIrFileAppender.flush();
    }

    @Override
    protected Map<String, Object> computeSyncRequestMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("compressedLogSize", clpIrFileAppender.getCompressedSize());
        metadata.put("uncompressedLogSize", clpIrFileAppender.getUncompressedSize());
        metadata.put("numEventsLogged", getNumEventsLogged());
        return metadata;
    }

    @Override
    protected String computeLogFileName(String baseName, long logRolloverTimestamp) {
        return baseName + "." + logRolloverTimestamp + CLP_COMPRESSED_IRSTREAM_FILE_EXTENSION;
    }

    /**
     * Computes a path for the provided log file name
     *
     * @param logFileName The log file name
     * @return The computed log file path
     */
    protected String computeLogFilePath(String logFileName) {
        return Paths.get(outputDir, logFileName).toString();
    }

    protected static class Builder<B extends Builder<B>>
            extends
            AbstractBufferedRollingFileAppender.Builder<B> {
        @PluginBuilderAttribute("CompressionLevel")
        private int compressionLevel = 3;

        @PluginBuilderAttribute("RolloverCompressedSizeThreshold")
        private long rolloverCompressedSizeThreshold = 16 * 1024 * 1024; // Bytes;

        @PluginBuilderAttribute("RolloverUncompressedSizeThreshold")
        private long rolloverUncompressedSizeThreshold = 1024L * 1024 * 1024; // Bytes;

        @PluginBuilderAttribute("UseFourByteEncoding")
        private boolean useFourByteEncoding = false;

        @PluginBuilderAttribute("outputDir")
        private String outputDir = null;

        @PluginBuilderAttribute("closeFrameOnFlush")
        private boolean closeFrameOnFlush = true;

        /**
         * Sets the compression level for the appender's streaming compressor
         *
         * @param compressionLevel The compression level between 1 and 22
         */
        public B setCompressionLevel(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            return asBuilder();
        }

        /**
         * Sets the threshold for the file's compressed size at which rollover should be triggered.
         *
         * @param rolloverCompressedSizeThreshold The threshold size in bytes
         */
        public B setRolloverCompressedSizeThreshold(long rolloverCompressedSizeThreshold) {
            this.rolloverCompressedSizeThreshold = rolloverCompressedSizeThreshold;
            return asBuilder();
        }

        /**
         * Sets the threshold for the file's uncompressed size at which rollover should be
         * triggered.
         *
         * @param rolloverUncompressedSizeThreshold The threshold size in bytes
         */
        public B setRolloverUncompressedSizeThreshold(long rolloverUncompressedSizeThreshold) {
            this.rolloverUncompressedSizeThreshold = rolloverUncompressedSizeThreshold;
            return asBuilder();
        }

        /**
         * @param useFourByteEncoding Whether to use CLP's four-byte encoding instead of the default
         * eight-byte encoding
         */
        public B setUseFourByteEncoding(boolean useFourByteEncoding) {
            this.useFourByteEncoding = useFourByteEncoding;
            return asBuilder();
        }

        /** @param outputDir The output directory path for log files */
        public B setOutputDir(String outputDir) {
            this.outputDir = outputDir;
            return asBuilder();
        }

        /** @param closeFrameOnFlush Whether to close the compressor's frame on flush */
        public B setCloseFrameOnFlush(boolean closeFrameOnFlush) {
            this.closeFrameOnFlush = closeFrameOnFlush;
            return asBuilder();
        }

        protected boolean validateParameters() {
            if (
                compressionLevel < Zstd.minCompressionLevel() || compressionLevel > Zstd
                        .maxCompressionLevel()
            ) {
                LOGGER.error(
                        "The specified compression level " + compressionLevel + " is out of range"
                );
                return false;
            }

            if (!(getLayout() instanceof PatternLayout)) {
                LOGGER.error(PLUGIN_NAME + " only supports PatternLayout");
                return false;
            }

            return true;
        }
    }
}
