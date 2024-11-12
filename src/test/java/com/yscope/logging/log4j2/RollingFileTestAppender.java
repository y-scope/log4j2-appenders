package com.yscope.logging.log4j2;

import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.ThresholdFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * A rolling file appender used for testing
 * {@link AbstractClpIrBufferedRollingFileAppender}. It specifically allows us to control the time
 * visible to the appender and tracks the number of syncs and rollover events.
 */
public class RollingFileTestAppender extends AbstractClpIrBufferedRollingFileAppender {
    private int numSyncs = 0;
    private int numRollovers = 0;

    public RollingFileTestAppender(PatternLayout patternLayout) {
        super(
                "RollingFileTestAppender",
                ThresholdFilter.createFilter(Level.INFO, Filter.Result.ACCEPT, Filter.Result.DENY),
                patternLayout,
                true,
                Property.EMPTY_ARRAY,
                new ManualTimeSource()
        );
    }

    /**
     * Sets the current time visible to the appender
     * 
     * @param timestamp The current time
     */
    public void setTime(long timestamp) {
        timeSource.setCurrentTimeInMilliseconds(timestamp);
    }

    public synchronized int getNumRollovers() { return numRollovers; }

    public synchronized int getNumSyncs() { return numSyncs; }

    /**
     * Tracks the number of syncs and rollovers
     * 
     * @param baseName {@inheritDoc}
     * @param logRolloverTimestamp {@inheritDoc}
     * @param deleteFile {@inheritDoc}
     * @param fileMetadata {@inheritDoc}
     */
    @Override
    protected synchronized void sync(
            String baseName,
            long logRolloverTimestamp,
            boolean deleteFile,
            Map<String, Object> fileMetadata
    ) {
        if (deleteFile) {
            numRollovers += 1;
        } else {
            numSyncs += 1;
        }
    }
}
