package com.yscope.logging.log4j2;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.core.layout.ByteBufferDestination;

/**
 * This class is a BufferDestination compatible with Log4j2 version 2.9+ (August 31, 2017).
 * <p>
 * To understand the utility of a BufferDestination for Log4j, we first need to review how Log4j
 * logging typically works:
 * <ol>
 * <li>Log4j2 is initialized with a PatternLayout and a FileAppender
 * <li>The application invokes one of the logging APIs, e.g., log.info(xxx), log.warn(xxx)
 * <li>Log4j2 creates a Log4j log event for the log and passes it to FileAppender
 * <li>FileAppender then invokes the PatternLayout to format the event (e.g., formatting the
 * timestamp and joining it with the message) and writes the formatted event it to the output.
 * </ol>
 * <p>
 * PatternLayout uses a StringBuilder to format the event, which, if misused, can generate a lot
 * of garbage on the heap. Specifically, when getByteArray() is called on the builder, it will
 * encode the builder's content into a newly allocated byte array, which is expensive if done on
 * every new log event. To mitigate this, Log4j2 implements its own StringBuilderEncoder,
 * specifically to encode the builder's content into a pre-allocated buffer that can be reused for
 * multiple messages.
 * <p>
 * Log4j2 also allows appenders to directly access this buffer by passing in a BufferDestination.
 * Thus, appenders can append the formatted log events without any unnecessary allocations.
 * <p>
 * This class is a simple BufferDestination implementation where all the character encoded bytes
 * are first copied to a pre-allocated internal ByteBuffer within this ByteBufferDestination before
 * presenting the internal ByteBuffer content to CLP as one continuous character-encoding-agnostic
 * chunk of bytes. In the future, if more engineering resources are available, it is possible to
 * achieve higher performance by directly encoding the formatted log events (as one or more bytes
 * chunks) rather than first collecting them in a ByteBuffer. This requires some modification inside
 * IR token encoder to support sequentially parsing multiple chunks of data. However, this
 * optimization would allow us to reduce the amount of data copied between buffers by 50%.
 * <p>
 * Note: The internal buffer must be cleared before it is reused
 */
public class PatternLayoutBufferDestination implements ByteBufferDestination {
    // We grow the buffer by multiples of this unit to avoid frequent capacity increases
    // 4 KiB seems to be a good default because it's usually aligned to a single virtual memory page
    private static final int BUF_ALLOCATION_UNIT = 4096;
    private static final int DEFAULT_LOG_MSG_BUF_SIZE = BUF_ALLOCATION_UNIT;

    // NOTE: This bytebuffer is used by CLP as a container for a byteArray rather than a ByteBuffer.
    // Therefore, we shouldn't be concerned about read/write flipping nor should we touch it.
    private ByteBuffer buf;

    public PatternLayoutBufferDestination() {
        buf = ByteBuffer.allocate(DEFAULT_LOG_MSG_BUF_SIZE);
    }

    /** Clears the internal byteBuffer. Must be invoked before processing the next log msg */
    public void clear() {
        buf.clear();
    }

    /** Provides a reference to Log4j2 */
    @Override
    public ByteBuffer getByteBuffer() { return buf; }

    /**
     * Retrieves and manages the buffer from the current PatternLayoutBufferDestination for encoding
     * log message. If the buffer (srcBuf) is full, Log4j2 triggers a drain operation. To ensure the
     * entire log message is buffered, the buffer's size is increased by
     * {@code BUF_ALLOCATION_UNIT}. This expansion allows Log4j2 to continue encoding and appending
     * any additional log message data into the buffer.
     *
     * @param srcBuf The ByteBuffer, filled with a character-encoded log message.
     * @return The adjusted ByteBuffer, now capable of holding more data.
     */
    @Override
    public ByteBuffer drain(final ByteBuffer srcBuf) {
        // Drain means it ran out of space, so we'll create more space for them
        ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() + BUF_ALLOCATION_UNIT);
        System.arraycopy(buf.array(), 0, newBuf.array(), 0, buf.position());
        newBuf.position(buf.position());

        // Swap the old buffer with the new buffer
        buf = newBuf;
        return buf;
    }

    /**
     * When presented with Log4j's internal buffer containing a character-encoded log event, we copy
     * the available content in the input ByteBuffer to the ByteBuffer in this class.
     * <p>
     * Note that the input ByteBuffer has already been flipped to read mode.
     *
     * @param data ByteBuffer containing some or all of the character-encoded log event
     */
    @Override
    public void writeBytes(final ByteBuffer data) {
        if (data.isDirect()) {
            throw new UnsupportedOperationException("Direct ByteBuffer is unsupported");
        }
        writeBytes(data.array(), data.position(), data.remaining());
    }

    /**
     * When presented with Log4j's internal buffer containing a character-encoded log event, we copy
     * the available content in the input byte array to the ByteBuffer in this class.
     *
     * @param data byteArray containing some or all of the character-encoded log event
     * @param offset where the valid character-encoded log event begins
     * @param length number of valid bytes
     */
    @Override
    public void writeBytes(byte[] data, int offset, int length) {
        // Check if we need to grow buffer
        int requiredBufSize = buf.position() + length;
        if (requiredBufSize > buf.capacity()) {
            int newBufSize = BUF_ALLOCATION_UNIT * (int)(Math.ceil(
                    requiredBufSize / (float)BUF_ALLOCATION_UNIT
            ));
            ByteBuffer newBuf = ByteBuffer.allocate(newBufSize);

            System.arraycopy(buf.array(), 0, newBuf.array(), 0, buf.position());
            newBuf.position(buf.position());

            buf = newBuf;
        }

        buf.put(data, offset, length);
    }
}
