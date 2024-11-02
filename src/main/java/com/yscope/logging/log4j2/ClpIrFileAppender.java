// Copyright (c) 2023 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package com.yscope.logging.log4j2;

import static com.yscope.logging.log4j2.Utils.createOutputFile;

import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import com.yscope.clp.irstream.AbstractClpIrOutputStream;
import com.yscope.clp.irstream.EightByteClpIrOutputStream;
import com.yscope.clp.irstream.FourByteClpIrOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * A Log4j appender that writes log events into a Zstandard-compressed CLP IR
 * stream file.
 * <p>
 * Since this appender buffers data in the process of compressing the output,
 * derived appenders should ensure the appender is closed even when the program
 * exits uncleanly. Otherwise, the compressed output may be truncated. When the
 * appender is used directly from Log4j, we install a shutdown hook for this
 * purpose.
 */
public class ClpIrFileAppender extends AbstractAppender implements Flushable {
    public static final String PLUGIN_NAME = "ClpIrFileAppender";
    private static final int estimatedFormattedTimestampLength = 0;
    private long uncompressedSizeInBytes = 0;

    private AbstractClpIrOutputStream clpIrOutputStream;
    private CountingOutputStream countingOutputStream;

    private boolean activated = false;

    private final int compressionLevel;
    private boolean closeFrameOnFlush;
    private String file;
    private final boolean useFourByteEncoding;

    private CompressionPatternLayoutContainer compressionLayoutContainer;
    protected final Object lock = new Object();

    /**
     * Creates a ClpStreamingCompressionAppender optimized to reduce write wear on SSDs
     *
     * @param filePath The name and path of the file
     * @param name The name of the Appender
     * @param ignoreExceptions If true exceptions encountered when appending events are logged;
     * otherwise they are propagated to the caller.
     * @param layout The PatternLayout to use to format the event.
     * @param filter The filter, if any, to use.
     * @param compressionLevel The compression level to be used for compressing the output CLP
     * intermediate representation stream.
     */
    public ClpIrFileAppender(
            final String filePath,
            final String name,
            boolean ignoreExceptions,
            final PatternLayout layout,
            final Filter filter,
            int compressionLevel,
            boolean useFourByteEncoding,
            boolean closeFrameOnFlush
    )
            throws IOException {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
        this.file = filePath;
        this.useFourByteEncoding = useFourByteEncoding;
        this.compressionLevel = compressionLevel;
        this.closeFrameOnFlush = closeFrameOnFlush;

        compressionLayoutContainer = new CompressionPatternLayoutContainer(layout);
        startHelper(false);
    }

    /**
     * @return The amount of data written to this appender for the current output file, in bytes.
     * <p>
     * NOTE:
     * <ul>
     * <li>This may be slightly inaccurate since we use an estimate of the timestamp length for
     * performance reasons.
     * <li>This will be reset when a new output file is opened.
     * </ul>
     */
    public synchronized long getUncompressedSize() { return uncompressedSizeInBytes; }

    /**
     * @return The amount of data written by this appender to the current output file, in bytes.
     * This will be reset when a new output file is opened.
     */
    public synchronized long getCompressedSize() { return countingOutputStream.getByteCount(); }

    private void setFile(String file) {
        if (file != null) {
            file = file.trim();
        }
        this.file = file;
    }

    protected synchronized void closeStream() {
        try {
            Objects.requireNonNull(clpIrOutputStream).close();
            uncompressedSizeInBytes = 0;
        } catch (IOException ex) {
            error("Failed to close CLP IR stream for appender " + getName(), ex);
        }
        super.stop();
    }

    /**
     * Closes the previous file and starts a new file with the given path
     *
     * @param path Path for the new file
     * @throws IOException on I/O error
     */
    public synchronized void startNewFile(String path) throws IOException {
        if (!activated) { throw new IllegalStateException("Appender not activated."); }

        if (!this.isStarted()) { throw new IllegalStateException("Appender already closed."); }

        Objects.requireNonNull(clpIrOutputStream).close();
        uncompressedSizeInBytes = 0;

        setFile(path);
        sanitizeFilePath();
        createOutputStream();
    }

    @Override
    public synchronized void flush() throws IOException {
        Objects.requireNonNull(clpIrOutputStream).flush();
    }

    @Override
    public void append(final LogEvent event) {
        // We synchronize on the lock to ensure that the appender is not stopped while
        // we are appending and can be called by multiple threads
        synchronized (lock) {
            // No-OP if we do not need to do any work
            if (!isStarted() || isStopping() || isStopped()) { return; }

            event.getLevel();

            try {
                ByteBuffer logMsg = compressionLayoutContainer.encodeLogMsg(event);
                Objects.requireNonNull(clpIrOutputStream).writeLogEvent(
                        event.getTimeMillis(),
                        logMsg
                );
                uncompressedSizeInBytes += estimatedFormattedTimestampLength + logMsg.limit();
            } catch (Exception ex) {
                error("Failed to append log event for appender " + getName(), event, ex);
            }
        }
    }

    /**
     * Activates the appender's options. This should not be called when this appender is
     * instantiated manually.
     */
    @Override
    public void start() {
        if (activated) {
            LOGGER.warn("Already activated.");
            return;
        }

        try {
            startHelper(true);
        } catch (Exception ex) {
            error("Failed to activate appender.", ex);
            super.stop();
        }
    }

    /** Closes the appender. Once closed, the appender cannot be reopened. */
    @Override
    public synchronized boolean stop(long timeout, final TimeUnit timeUnit) {
        synchronized (lock) {
            if (isStopped() || isStopping() || !isStarted()) { return true; }

            setStopping();
        }

        closeStream();
        return super.stop(timeout, timeUnit);
    }

    /**
     * Helper method to activate options.
     *
     * @param enableShutdownHook Whether to enable a shutdown hook to close the appender.
     * @throws IOException on I/O error
     */
    private void startHelper(boolean enableShutdownHook) throws IOException {
        super.start();

        validateOptionsAndInit();

        activated = true;
        if (enableShutdownHook) {
            // log4j2 may not attempt to close the appender when the JVM shuts down, so
            // this hook ensures we try to close the appender before shutdown.
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        }
    }

    /**
     * Validates the appender's settings (e.g., compression level) and initializes the appender with
     * them.
     *
     * @throws IOException on I/O error
     */
    private void validateOptionsAndInit() throws IOException {

        if (
            compressionLevel < Zstd.minCompressionLevel() || Zstd.maxCompressionLevel()
                    < compressionLevel
        ) {
            throw new IllegalArgumentException(
                    "compressionLevel is outside of valid range: [" + Zstd.minCompressionLevel()
                            + ", " + Zstd.maxCompressionLevel() + "]"
            );
        }

        sanitizeFilePath();

        createOutputStream();
    }

    private void sanitizeFilePath() {
        if (null == this.file) { throw new IllegalArgumentException("file option not set."); }
        // Trim surrounding spaces
        this.file = this.file.trim();
    }

    /**
     * Creates the CLP IR output stream, the file output stream, and any necessary streams in
     * between.
     *
     * @throws IOException on I/O error
     */
    private void createOutputStream() throws IOException {
        FileOutputStream fileOutputStream = createOutputFile(file);
        countingOutputStream = new CountingOutputStream(fileOutputStream);
        ZstdOutputStream zstdOutputStream = new ZstdOutputStream(
                countingOutputStream,
                compressionLevel
        );
        zstdOutputStream.setCloseFrameOnFlush(closeFrameOnFlush);
        String timeZoneId = ZonedDateTime.now().getZone().toString();
        if (useFourByteEncoding) {
            clpIrOutputStream = new FourByteClpIrOutputStream(
                    compressionLayoutContainer.getTimestampPattern(),
                    timeZoneId,
                    zstdOutputStream
            );
        } else {
            clpIrOutputStream = new EightByteClpIrOutputStream(
                    compressionLayoutContainer.getTimestampPattern(),
                    timeZoneId,
                    zstdOutputStream
            );
        }

        uncompressedSizeInBytes += compressionLayoutContainer.getTimestampPattern().getBytes(
                StandardCharsets.ISO_8859_1
        ).length;
        uncompressedSizeInBytes += timeZoneId.length();
    }

    protected static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B> {

        @PluginBuilderAttribute("CloseFrameOnFlush")
        private boolean closeFrameOnFlush = true;

        @PluginBuilderAttribute("UseFourByteEncoding")
        private boolean useFourByteEncoding = true;

        @PluginBuilderAttribute("fileName")
        @SuppressWarnings("NullAway")
        private String fileName;

        public B setFileName(String fileName) {
            this.fileName = fileName;
            return asBuilder();
        }

        public B setCloseFrameOnFlush(boolean closeFrameOnFlush) {
            this.closeFrameOnFlush = closeFrameOnFlush;
            return asBuilder();
        }

        public B setUseFourByteEncoding(boolean useFourByteEncoding) {
            this.useFourByteEncoding = useFourByteEncoding;
            return asBuilder();
        }

        protected boolean validateParameters() {
            if (null == fileName || fileName.isEmpty()) {
                LOGGER.error("Invalid file name provided");
                return false;
            }
            return true;
        }
    }
}
