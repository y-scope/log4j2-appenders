package com.yscope.logging.log4j2;

import static java.lang.Thread.sleep;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;

/**
 * Base class for Log4j file appenders with specific design characteristics; namely, the appenders:
 * <ol>
 * <li>Buffer logs, e.g. for streaming compression.
 * <li>Rollover log files based on some policy, e.g. exceeding a threshold size.
 * <li>Flush and synchronize log files (e.g. to remote storage) based on how fresh they are.
 * </ol>
 * For instance, such an appender might compress log events as they are generated, while still
 * flushing and uploading them to remote storage a few seconds after an error log event.
 * <p>
 * This class handles keeping track of how fresh the logs are and the high-level logic to trigger
 * flushing, syncing, and rollover at the appropriate times. Derived classes must implement methods
 * to do the actual flushing, syncing, and rollover as well as indicate whether rollover is
 * necessary.
 * <p>
 * The freshness property maintained by this class allows users to specify the delay between a
 * log event being generated and the log file being flushed and synchronized. There are two types of
 * delays that can be specified, and each can be specified per log level:
 * <ul>
 * <li><b>hard timeouts</b> - these timeouts cause a hard deadline to be set for when flushing
 * must occur. E.g., if log event occurs at time t, then a hard deadline is set at time (t +
 * hardTimeout). This hard deadline may be decreased if a subsequent log event has an associated
 * hard timeout that would result in an earlier hard deadline.
 * <li><b>soft timeouts</b> - these timeouts cause a soft deadline to be set for when flushing
 * should occur. Unlike the hard deadline, this deadline may be increased if a subsequent log event
 * occurs before any deadline is reached. Note however, that the timeout used to calculate the soft
 * deadline is set to the minimum of the timeouts associated with the log events that have occurred
 * before the deadline. E.g., if a log event associated with a 5s soft timeout occurs, and then is
 * followed by a log event associated with a 10s soft timeout, the soft deadline will be set as if
 * the second log event had a had 5s soft timeout.
 * </ul>
 * Once a deadline is reached, the current timeouts and deadlines are reset.
 * <p>
 * For example, let's assume the soft and hard timeouts for ERROR logs are set to 5 seconds and 5
 * minutes respectively. Now imagine an ERROR log event is generated at t = 0s. This class will
 * trigger a flush at t = 5s unless another ERROR log event is generated before then. If one is
 * generated at t = 4s, then this class will omit the flush at t = 5s and trigger a flush at t = 9s.
 * If ERROR log events keep being generated before a flush occurs, then this class will definitely
 * trigger a flush at t = 5min based on the hard timeout.
 * <p>
 * Maintaining these timeouts per log level allows us to flush logs sooner if more important log
 * levels occur. For instance, we can set smaller timeouts for ERROR log events compared to DEBUG
 * log events.
 * <p>
 * This class also allows logs to be collected while the JVM is shutting down. This can be enabled
 * by setting closeOnShutdown to false. When the JVM starts shutting down, the appender will
 * maintain two timeouts before the shutdown is allowed to complete:
 * <ul>
 * <li><b>soft timeout</b> - this is a relative delay from when the shutdown is requested to when
 * the shutdown is allowed to continue. It is soft in the sense that if a log event occurs before
 * this delay expires, the delay is reset based on the current time.
 * <li><b>hard timeout</b> - this is a relative delay from when the shutdown is requested to when
 * the shutdown is allowed to continue.
 * </ul>
 */
public abstract class AbstractBufferedRollingFileAppender extends AbstractAppender
        implements
        Flushable {
    // Volatile members below are marked as such because they are accessed by multiple threads

    private static final long INVALID_FLUSH_TIMEOUT_TIMESTAMP = Long.MAX_VALUE;

    protected final TimeSource timeSource;
    protected long lastRolloverTimestamp;

    // Appender settings, some of which may be set by Log4j through reflection.
    // For descriptions of the properties, see their setters below.
    private String baseName;
    private boolean closeOnShutdown;
    private final HashMap<Level, Long> flushHardTimeoutPerLevel = new HashMap<>();
    private final HashMap<Level, Long> flushSoftTimeoutPerLevel = new HashMap<>();
    private long shutdownSoftTimeout; // milliseconds
    private long shutdownHardTimeout; // milliseconds
    private volatile int timeoutCheckPeriod;

    private long flushHardTimeoutTimestamp;
    private long flushSoftTimeoutTimestamp;

    // The maximum soft timeout allowed. If users wish to continue log collection and
    // synchronization while the JVM is shutting down, this value will be lowered to increase the
    // likelihood of flushing before the shutdown completes.
    private volatile long flushAbsoluteMaximumSoftTimeout;
    private long flushMaximumSoftTimeout;

    private final Thread backgroundFlushThread = new Thread(new BackgroundFlushRunnable());
    private final BackgroundSyncRunnable backgroundSyncRunnable = new BackgroundSyncRunnable();
    private final Thread backgroundSyncThread = new Thread(backgroundSyncRunnable);
    private final Thread shutdownHookThread = new Thread(new ShutdownHookRunnable());
    private volatile boolean closeWithDelayedShutdown = false;

    private final AtomicLong numEventsLogged = new AtomicLong(0L);

    private boolean activated = false;

    private final AtomicBoolean closeStarted = new AtomicBoolean(false);
    private volatile boolean closedForAppends = false;

    public AbstractBufferedRollingFileAppender(
            final String name,
            boolean ignoreExceptions,
            final Layout<? extends Serializable> layout,
            final Filter filter,
            String baseName,
            boolean closeOnShutdown,
            String csvFlushHardTimeouts,
            String csvFlushSoftTimeouts,
            long shutdownSoftTimeout,
            long shutdownHardTimeout,
            int timeoutCheckPeriod
    ) {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
        this.timeSource = new SystemTimeSource();

        // The default flush timeout values below are optimized for high latency remote persistent
        // storage such as object stores or HDFS
        flushHardTimeoutPerLevel.put(Level.FATAL, 5L * 60 * 1000 /* 5 min */);
        flushHardTimeoutPerLevel.put(Level.ERROR, 5L * 60 * 1000 /* 5 min */);
        flushHardTimeoutPerLevel.put(Level.WARN, 10L * 60 * 1000 /* 10 min */);
        flushHardTimeoutPerLevel.put(Level.INFO, 30L * 60 * 1000 /* 30 min */);
        flushHardTimeoutPerLevel.put(Level.DEBUG, 30L * 60 * 1000 /* 30 min */);
        flushHardTimeoutPerLevel.put(Level.TRACE, 30L * 60 * 1000 /* 30 min */);

        flushSoftTimeoutPerLevel.put(Level.FATAL, 5L * 1000 /* 5 sec */);
        flushSoftTimeoutPerLevel.put(Level.ERROR, 10L * 1000 /* 10 sec */);
        flushSoftTimeoutPerLevel.put(Level.WARN, 15L * 1000 /* 15 sec */);
        flushSoftTimeoutPerLevel.put(Level.INFO, 3L * 60 * 1000 /* 3 min */);
        flushSoftTimeoutPerLevel.put(Level.DEBUG, 3L * 60 * 1000 /* 3 min */);
        flushSoftTimeoutPerLevel.put(Level.TRACE, 3L * 60 * 1000 /* 3 min */);

        _setBaseName(baseName);
        _setCloseOnShutdown(closeOnShutdown);
        _setShutdownSoftTimeout(shutdownSoftTimeout);
        _setShutdownHardTimeout(shutdownHardTimeout);
        _setTimeoutCheckPeriod(timeoutCheckPeriod);
        _setFlushHardTimeoutsInMinutes(csvFlushHardTimeouts);
        _setFlushSoftTimeoutsInSeconds(csvFlushSoftTimeouts);
    }

    /** @param baseName The base filename for log files */
    protected void _setBaseName(String baseName) {
        this.baseName = baseName;
    }

    /**
     * Sets whether to close the log appender upon receiving a shutdown signal before the JVM exits.
     * If set to false, the appender will continue appending logs even while the JVM is shutting
     * down and the appender will do its best to sync those logs before the JVM shuts down. This
     * presents a tradeoff between capturing more log events and potential data loss if the log
     * events cannot be flushed and synced before the JVM is killed.
     *
     * @param closeOnShutdown Whether to close the log file on shutdown
     */
    private void _setCloseOnShutdown(boolean closeOnShutdown) {
        this.closeOnShutdown = closeOnShutdown;
    }

    /**
     * Sets the per-log-level hard timeouts for flushing.
     * <p>
     * NOTE: Timeouts for custom log-levels are not supported. Log events with these levels will be
     * assigned the timeout of the INFO level.
     *
     * @param csvTimeouts A CSV string of kv-pairs. The key being the log-level in all caps and the
     * value being the hard timeout for flushing in minutes. E.g. "INFO=30,WARN=10,ERROR=5"
     */
    private void _setFlushHardTimeoutsInMinutes(String csvTimeouts) {
        if (csvTimeouts == null) { return; }
        for (String token : csvTimeouts.split(",")) {
            String[] kv = token.split("=");
            if (isSupportedLogLevel(kv[0])) {
                try {
                    flushHardTimeoutPerLevel.put(
                            Level.toLevel(kv[0]),
                            Long.parseLong(kv[1]) * 60 * 1000
                    );
                } catch (NumberFormatException ex) {
                    error(
                            "Invalid number format for hard flush timeout value " + "for the "
                                    + kv[0] + " verbosity level: " + kv[1],
                            ex
                    );
                } catch (UnsupportedOperationException ex) {
                    error(
                            "Failed to set hard flush timeout value " + "for the " + kv[0]
                                    + " verbosity level: " + kv[1]
                    );
                }
            } else {
                error(
                        "Failed to set hard flush timeout "
                                + "for the following unsupported verbosity level: " + kv[0]
                );
            }
        }
    }

    /**
     * Sets the per-log-level soft timeouts for flushing.
     * <p>
     * NOTE: Timeouts for custom log-levels are not supported. Log events with these levels will be
     * assigned the timeout of the INFO level.
     *
     * @param csvTimeouts A CSV string of kv-pairs. The key being the log-level in all caps and the
     * value being the soft timeout for flushing in seconds. E.g. "INFO=180,WARN=15,ERROR=10"
     */
    private void _setFlushSoftTimeoutsInSeconds(String csvTimeouts) {
        if (csvTimeouts == null) { return; }
        for (String token : csvTimeouts.split(",")) {
            String[] kv = token.split("=");
            if (isSupportedLogLevel(kv[0])) {
                try {
                    flushSoftTimeoutPerLevel.put(
                            Level.toLevel(kv[0]),
                            Long.parseLong(kv[1]) * 1000
                    );
                } catch (NumberFormatException ex) {
                    error(
                            "Invalid number format for soft flush timeout value " + "for the "
                                    + kv[0] + " verbosity level: " + kv[1],
                            ex
                    );
                } catch (UnsupportedOperationException ex) {
                    error(
                            "Failed to set soft flush timeout value " + "for the " + kv[0]
                                    + " verbosity level: " + kv[1]
                    );
                }
            } else {
                error(
                        "Failed to set soft flush timeout "
                                + "for the following unsupported verbosity level: " + kv[0]
                );
            }
        }
    }

    /** @param milliseconds The soft shutdown timeout in milliseconds */
    private void _setShutdownSoftTimeout(long milliseconds) {
        shutdownSoftTimeout = milliseconds;
    }

    /** @param seconds The hard shutdown timeout in seconds */
    private void _setShutdownHardTimeout(long seconds) {
        shutdownHardTimeout = seconds * 1000;
    }

    /**
     * Sets the period between checking for soft/hard timeouts (and then triggering a flush and
     * sync). Care should be taken to ensure this period does not significantly differ from the
     * lowest timeout since that will cause undue delay from when a timeout expires and when a flush
     * occurs.
     *
     * @param milliseconds The period in milliseconds
     */
    private void _setTimeoutCheckPeriod(int milliseconds) {
        timeoutCheckPeriod = milliseconds;
    }

    public String getBaseName() { return baseName; }

    public long getNumEventsLogged() { return numEventsLogged.get(); }

    /**
     * This method is primarily used for testing
     *
     * @return Whether any of the background threads in this class are running
     */
    public boolean backgroundThreadsRunning() {
        return backgroundFlushThread.isAlive() || backgroundSyncThread.isAlive()
                || shutdownHookThread.isAlive();
    }

    /**
     * Simulates the JVM calling this appender's shutdown hook. This method should <i>only</i> be
     * used for testing.
     */
    public void simulateShutdownHook() {
        if (false == shutdownHookThread.isAlive()) {
            shutdownHookThread.start();
        }
    }

    /**
     * Start the appender's options.
     * <p>
     * This method is {@code final} to ensure it is not overridden by derived classes since this
     * base class needs to perform actions before/after the derived class'
     * {@link #activateOptionsHook(long)} method.
     */
    @Override
    public final void start() {
        if (isStopped()) {
            LOGGER.warn("Already closed so cannot activate options.");
            return;
        }

        if (activated) {
            LOGGER.warn("Already activated.");
            return;
        }

        flushAbsoluteMaximumSoftTimeout = flushSoftTimeoutPerLevel.get(Level.TRACE);
        resetFreshnessTimeouts();

        try {
            // Set the first rollover timestamp to the current time
            lastRolloverTimestamp = System.currentTimeMillis();
            activateOptionsHook(lastRolloverTimestamp);
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
            backgroundFlushThread.setDaemon(true);
            backgroundSyncThread.setDaemon(true);
            backgroundFlushThread.start();
            backgroundSyncThread.start();

            activated = true;
            super.start();
        } catch (Exception ex) {
            error("Failed to activate appender.", ex);
            super.stop();
        }
    }

    /**
     * Closes the appender.
     * <p>
     * This method is {@code final} to ensure it is not overridden by derived classes since this
     * base class needs to perform actions before/after the derived class' {@link #closeHook()}
     * method.
     */
    @Override
    public final void stop() {
        // NOTE: This method should not be marked {@code synchronized} since it joins with the
        // background threads, and they may call synchronized methods themselves (e.g.,
        // {@link #sync} in the derived class), leading to a deadlock.

        // Prevent multiple threads from running close concurrently
        if (false == closeStarted.compareAndSet(false, true)) { return; }

        if (closeWithDelayedShutdown) {
            try {
                // Flush now just in case we shut down before a timeout expires
                flush();
            } catch (IOException e) {
                error("Failed to flush", e);
            }

            // Decrease the absolute maximum soft timeout to increase the likelihood that the log
            // will be synced before the app shuts down
            flushAbsoluteMaximumSoftTimeout = Math.min(
                    1000 /* 1 second */,
                    flushSoftTimeoutPerLevel.get(Level.FATAL)
            );
        }

        // Shutdown the flush thread before we close (so we don't try to flush after the appender is
        // already closed)
        backgroundFlushThread.interrupt();
        try {
            backgroundFlushThread.join();
        } catch (InterruptedException e) {
            error("Interrupted while joining backgroundFlushThread");
        }

        // Prevent any further appends
        closedForAppends = true;

        try {
            closeHook();
        } catch (Exception ex) {
            // Just log the failure but continue the close process
            error("closeHook failed.", ex);
        }

        // Perform a rollover (in case an append occurred after flushing the background thread) and
        // then shutdown the sync thread
        backgroundSyncRunnable.addSyncRequest(
                baseName,
                lastRolloverTimestamp,
                true,
                computeSyncRequestMetadata()
        );
        backgroundSyncRunnable.addShutdownRequest();
        try {
            backgroundSyncThread.join();
        } catch (InterruptedException e) {
            error("Interrupted while joining backgroundSyncThread");
        }

        super.stop();
    }

    /**
     * Appends the given log event to the file (subject to any buffering by the derived class). This
     * method may also trigger a rollover and sync if the derived class' {@link #rolloverRequired()}
     * method returns true.
     * <p>
     * This method is {@code final} to ensure it is not overridden by derived classes since this
     * base class needs to perform actions before/after the derived class'
     * {@link #appendHook(LogEvent)} method. This method is also marked {@code synchronized} since
     * it can be called from multiple logging threads.
     *
     * @param loggingEvent The log event
     */
    @Override
    public final synchronized void append(LogEvent loggingEvent) {
        if (false == isStarted()) {
            LOGGER.warn("Appender is not started.");
            return;
        }
        if (closedForAppends) {
            LOGGER.warn("Appender closed for appends.");
            return;
        }

        try {
            appendHook(loggingEvent);
            numEventsLogged.incrementAndGet();

            if (false == rolloverRequired()) {
                updateFreshnessTimeouts(loggingEvent);
            } else {
                backgroundSyncRunnable.addSyncRequest(
                        baseName,
                        lastRolloverTimestamp,
                        true,
                        computeSyncRequestMetadata()
                );
                resetFreshnessTimeouts();
                lastRolloverTimestamp = loggingEvent.getTimeMillis();
                startNewLogFile(lastRolloverTimestamp);
                numEventsLogged.set(0L);
            }
        } catch (Exception ex) {
            getHandler().error("Failed to write log event.", ex);
        }
    }

    /**
     * Activates appender options for derived appenders.
     *
     * @param currentTimestamp Current timestamp (useful for naming the first log file)
     * @throws Exception on error
     */
    protected abstract void activateOptionsHook(long currentTimestamp) throws Exception;

    /**
     * Closes the derived appender. Once closed, the appender cannot be reopened.
     *
     * @throws Exception on error
     */
    protected abstract void closeHook() throws Exception;

    /**
     * @return Whether to trigger a rollover
     * @throws Exception on error
     */
    protected abstract boolean rolloverRequired() throws Exception;

    /**
     * Starts a new log file.
     *
     * @param lastEventTimestamp Timestamp of the last event that was logged before calling this
     * method (useful for naming the new log file).
     * @throws Exception on error
     */
    protected abstract void startNewLogFile(long lastEventTimestamp) throws Exception;

    /**
     * Synchronizes a log file with remote storage. Note that this file may not necessarily be the
     * current log file, but a previously rolled-over one.
     *
     * @param baseName The base filename of the log file to sync
     * @param logRolloverTimestamp The approximate timestamp of when the target log file was rolled
     * over
     * @param deleteFile Whether the log file can be deleted after syncing
     * @param fileMetadata Extra metadata for the file that was captured at the time when the sync
     * request was generated
     * @throws Exception on error
     */
    protected abstract void sync(
            String baseName,
            long logRolloverTimestamp,
            boolean deleteFile,
            Map<String, Object> fileMetadata
    )
            throws Exception;

    /**
     * Appends a log event to the file.
     *
     * @param event The log event
     */
    protected abstract void appendHook(LogEvent event);

    /**
     * Computes the log file name, which includes the provided base name and rollover timestamp.
     *
     * @param baseName The base name of the log file name
     * @param logRolloverTimestamp The approximate timestamp when the target log file was rolled
     * over
     * @return The computed log file name
     */
    protected abstract String computeLogFileName(String baseName, long logRolloverTimestamp);

    /**
     * Computes the file metadata to be included in a synchronization request
     *
     * @return The computed file metadata
     */
    protected Map<String, Object> computeSyncRequestMetadata() {
        return null;
    }

    /**
     * Tests if log level is supported by this appender configuration
     *
     * @param level string passed in from configuration parameter
     * @return true if supported, false otherwise
     */
    public boolean isSupportedLogLevel(String level) {
        // Note that the Level class is able to automatically parses a candidate level string into a
        // log4j level, and if it cannot, it will assign the log4j level as to the default "INFO"
        // level.
        return Level.toLevel(level) != Level.INFO || level.equals("INFO");
    }

    /** Resets the soft/hard freshness timeouts. */
    private void resetFreshnessTimeouts() {
        flushHardTimeoutTimestamp = INVALID_FLUSH_TIMEOUT_TIMESTAMP;
        flushSoftTimeoutTimestamp = INVALID_FLUSH_TIMEOUT_TIMESTAMP;
        flushMaximumSoftTimeout = flushAbsoluteMaximumSoftTimeout;
    }

    /**
     * Updates the soft/hard freshness timeouts based on the given log event's log level and
     * timestamp.
     *
     * @param loggingEvent The log event
     */
    private void updateFreshnessTimeouts(LogEvent loggingEvent) {
        Level level = loggingEvent.getLevel();
        long flushHardTimeout = flushHardTimeoutPerLevel.computeIfAbsent(
                level,
                v -> flushHardTimeoutPerLevel.get(Level.INFO)
        );
        long timeoutTimestamp = loggingEvent.getTimeMillis() + flushHardTimeout;
        flushHardTimeoutTimestamp = Math.min(flushHardTimeoutTimestamp, timeoutTimestamp);

        long flushSoftTimeout = flushSoftTimeoutPerLevel.computeIfAbsent(
                level,
                v -> flushSoftTimeoutPerLevel.get(Level.INFO)
        );
        flushMaximumSoftTimeout = Math.min(flushMaximumSoftTimeout, flushSoftTimeout);
        flushSoftTimeoutTimestamp = loggingEvent.getTimeMillis() + flushMaximumSoftTimeout;
    }

    /**
     * Flushes and synchronizes the log file if one of the freshness timeouts has been reached.
     * <p>
     * This method is marked {@code synchronized} since it can be called from logging threads and
     * the background thread that monitors the freshness timeouts.
     *
     * @throws IOException on I/O error
     */
    private synchronized void flushAndSyncIfNecessary() throws IOException {
        long ts = timeSource.getCurrentTimeInMilliseconds();
        if (ts >= flushSoftTimeoutTimestamp || ts >= flushHardTimeoutTimestamp) {
            flush();
            backgroundSyncRunnable.addSyncRequest(
                    baseName,
                    lastRolloverTimestamp,
                    false,
                    computeSyncRequestMetadata()
            );
            resetFreshnessTimeouts();
        }
    }

    /**
     * Periodically flushes and syncs the current log file if we've exceeded one of the freshness
     * timeouts.
     */
    private class BackgroundFlushRunnable implements Runnable {
        @Override
        public void run() {
            boolean delayedShutdownRequested = false;
            long shutdownSoftTimeoutTimestamp = INVALID_FLUSH_TIMEOUT_TIMESTAMP;
            long shutdownHardTimeoutTimestamp = INVALID_FLUSH_TIMEOUT_TIMESTAMP;
            long lastNumEventsLogged = -1;
            while (true) {
                try {
                    flushAndSyncIfNecessary();

                    if (delayedShutdownRequested) {
                        // Update soft timeout if another event occurred
                        long currentTimestamp = timeSource.getCurrentTimeInMilliseconds();
                        if (numEventsLogged.get() != lastNumEventsLogged) {
                            lastNumEventsLogged = numEventsLogged.get();
                            shutdownSoftTimeoutTimestamp = currentTimestamp + shutdownSoftTimeout;
                        }

                        // Break if we've hit either timeout
                        if (
                            currentTimestamp >= shutdownSoftTimeoutTimestamp || currentTimestamp
                                    >= shutdownHardTimeoutTimestamp
                        ) {
                            break;
                        }
                    }

                    sleep(timeoutCheckPeriod);
                } catch (IOException e) {
                    error("Failed to flush buffered appender in the background", e);
                } catch (InterruptedException e) {
                    LOGGER.debug(
                            "Received interrupt message for graceful shutdown of"
                                    + " BackgroundFlushThread"
                    );

                    delayedShutdownRequested = closeWithDelayedShutdown;
                    if (closeOnShutdown || false == delayedShutdownRequested) {
                        break;
                    }
                    long currentTimestamp = timeSource.getCurrentTimeInMilliseconds();
                    shutdownSoftTimeoutTimestamp = currentTimestamp + shutdownSoftTimeout;
                    shutdownHardTimeoutTimestamp = currentTimestamp + shutdownHardTimeout;
                    lastNumEventsLogged = numEventsLogged.get();

                    // Lower the timeout check period so we react faster when flushing or
                    // shutting down is necessary
                    timeoutCheckPeriod = 100;
                }
            }
        }
    }

    /**
     * Thread to synchronize log files in the background (by calling
     * {@link #sync(String, long, boolean, Map<String, Object>) sync}). The thread maintains a
     * request queue that callers should populate.
     */
    private class BackgroundSyncRunnable implements Runnable {
        private final LinkedBlockingQueue<Request> requests = new LinkedBlockingQueue<>();

        @Override
        public void run() {
            while (true) {
                try {
                    Request request = requests.take();
                    if (request instanceof SyncRequest) {
                        SyncRequest syncRequest = (SyncRequest)request;
                        try {
                            sync(
                                    syncRequest.logFileBaseName,
                                    syncRequest.logRolloverTimestamp,
                                    syncRequest.deleteFile,
                                    syncRequest.fileMetadata
                            );
                        } catch (Exception ex) {
                            String logFilePath = computeLogFileName(
                                    syncRequest.logFileBaseName,
                                    syncRequest.logRolloverTimestamp
                            );
                            error("Failed to sync '" + logFilePath + "'", ex);
                        }
                    } else if (request instanceof ShutdownRequest) {
                        LOGGER.debug("Received shutdown request");
                        break;
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        /** Adds a shutdown request to the request queue */
        public void addShutdownRequest() {
            LOGGER.debug("Adding shutdown request");
            Request shutdownRequest = new ShutdownRequest();
            while (false == requests.offer(shutdownRequest)) {}
        }

        /**
         * Adds a sync request to the request queue
         *
         * @param baseName The base filename of the log file to sync
         * @param logRolloverTimestamp The approximate timestamp of when the target log file was
         * rolled over
         * @param deleteFile Whether the log file can be deleted after syncing.
         * @param fileMetadata Extra metadata for the file
         */
        public void addSyncRequest(
                String baseName,
                long logRolloverTimestamp,
                boolean deleteFile,
                Map<String, Object> fileMetadata
        ) {
            Request syncRequest = new SyncRequest(
                    baseName,
                    logRolloverTimestamp,
                    deleteFile,
                    fileMetadata
            );
            while (false == requests.offer(syncRequest)) {}
        }

        private class Request {}

        private class ShutdownRequest extends Request {}

        private class SyncRequest extends Request {
            public final String logFileBaseName;
            public final long logRolloverTimestamp;
            public final boolean deleteFile;
            public final Map<String, Object> fileMetadata;

            public SyncRequest(
                    String logFileBaseName,
                    long logRolloverTimestamp,
                    boolean deleteFile,
                    Map<String, Object> fileMetadata
            ) {
                this.logFileBaseName = logFileBaseName;
                this.logRolloverTimestamp = logRolloverTimestamp;
                this.deleteFile = deleteFile;
                this.fileMetadata = fileMetadata;
            }
        }
    }

    @Override
    public abstract void flush() throws IOException;

    /**
     * Thread to handle shutting down the appender when the JVM is shutting down. When
     * {@code closeOnShutdown} is false, this thread enables a delayed close procedure so that logs
     * that occur during shutdown can be collected.
     */
    private class ShutdownHookRunnable implements Runnable {
        @Override
        public void run() {
            if (false == closeOnShutdown) {
                closeWithDelayedShutdown = true;
            }
            stop();
        }
    }

    protected static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B> {
        @PluginBuilderAttribute("baseName")
        private String baseName = null;

        @PluginBuilderAttribute("CloseOnShutdown")
        private boolean closeOnShutdown = true;

        @PluginBuilderAttribute("FlushHardTimeoutsInMinutes")
        private String flushHardTimeoutsInMinutes = null;

        @PluginBuilderAttribute("FlushSoftTimeoutsInSeconds")
        private String flushSoftTimeoutsInSeconds = null;

        @PluginBuilderAttribute("ShutdownSoftTimeoutInMilliseconds")
        private long shutdownSoftTimeout = 5000;

        @PluginBuilderAttribute("ShutdownHardTimeoutInSeconds")
        private long shutdownHardTimeout = 30;

        @PluginBuilderAttribute("TimeoutCheckPeriod")
        private int timeoutCheckPeriod = 1000;

        /** @param baseName The base filename for log files */
        public B setBaseName(String baseName) {
            this.baseName = baseName;
            return asBuilder();
        }

        /**
         * Sets whether to close the log appender upon receiving a shutdown signal before the JVM
         * exits. If set to false, the appender will continue appending logs even while the JVM is
         * shutting down and the appender will do its best to sync those logs before the JVM shuts
         * down. This presents a tradeoff between capturing more log events and potential data loss
         * if the log events cannot be flushed and synced before the JVM is killed.
         *
         * @param closeOnShutdown Whether to close the log file on shutdown
         */
        public B setCloseOnShutdown(boolean closeOnShutdown) {
            this.closeOnShutdown = closeOnShutdown;
            return asBuilder();
        }

        /**
         * Sets the per-log-level hard timeouts for flushing.
         * <p>
         * NOTE: Timeouts for custom log-levels are not supported. Log events with these levels will
         * be assigned the timeout of the INFO level.
         *
         * @param flushHardTimeoutsInMinutes A CSV string of kv-pairs. The key being the UPPERCASE
         * log-level in all caps and the value being the hard timeout for flushing in minutes. E.g.,
         * "INFO=30,WARN=10,ERROR=5"
         */
        public B setFlushHardTimeoutsInMinutes(String flushHardTimeoutsInMinutes) {
            this.flushHardTimeoutsInMinutes = flushHardTimeoutsInMinutes;
            return asBuilder();
        }

        /**
         * Sets the per-log-level soft timeouts for flushing.
         * <p>
         * NOTE: Timeouts for custom log-levels are not supported. Log events with these levels will
         * be assigned the timeout of the INFO level.
         *
         * @param flushSoftTimeoutsInSeconds A CSV string of kv-pairs. The key being the UPPERCASE
         * log-level and the value being the soft timeout for flushing in seconds. E.g.,
         * "INFO=180,WARN=15,ERROR=10"
         */
        public B setFlushSoftTimeoutsInSeconds(String flushSoftTimeoutsInSeconds) {
            this.flushSoftTimeoutsInSeconds = flushSoftTimeoutsInSeconds;
            return asBuilder();
        }

        public B setShutdownSoftTimeoutInMilliseconds(long shutdownSoftTimeoutInMilliseconds) {
            this.shutdownSoftTimeout = shutdownSoftTimeoutInMilliseconds;
            return asBuilder();
        }

        public B setShutdownHardTimeoutInSeconds(long shutdownHardTimeoutInSeconds) {
            this.shutdownHardTimeout = shutdownHardTimeoutInSeconds;
            return asBuilder();
        }

        /**
         * Sets the period between checking for soft/hard timeouts (and then triggering a flush and
         * sync). Care should be taken to ensure this period does not significantly differ from the
         * lowest timeout since that will cause undue delay from when a timeout expires and when a
         * flush occurs.
         *
         * @param timeoutCheckPeriod The period in milliseconds
         */
        public B setTimeoutCheckPeriod(int timeoutCheckPeriod) {
            this.timeoutCheckPeriod = timeoutCheckPeriod;
            return asBuilder();
        }
    }
}
