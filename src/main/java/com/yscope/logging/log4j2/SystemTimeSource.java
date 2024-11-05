package com.yscope.logging.log4j2;

/** Time source for the system's current time. */
public class SystemTimeSource implements TimeSource {
    @Override
    public void setCurrentTimeInMilliseconds(long currentTimeInMilliseconds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCurrentTimeInMilliseconds() { return System.currentTimeMillis(); }
}
