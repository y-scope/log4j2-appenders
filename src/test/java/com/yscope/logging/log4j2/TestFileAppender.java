package com.yscope.logging.log4j2;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.filter.ThresholdFilter;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.Test;

public class TestFileAppender {
    private static final PatternLayout patternLayout = PatternLayout.newBuilder().withPattern(
            "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"
    ).build();
    private static final int compressionLevel = 3;
    private static boolean ignoreExceptions = true;
    private static boolean closeFrameOnFlush = false;
    private static Filter filter = ThresholdFilter.createFilter(
            Level.INFO,
            Filter.Result.ACCEPT,
            Filter.Result.DENY
    );

    @Test
    public void testFourByteIrAppender() {
        testAppender(true);
    }

    @Test
    public void testEightByteIrAppender() {
        testAppender(false);
    }

    private void testAppender(boolean useFourByteEncoding) {
        String fileName = useFourByteEncoding ? "four-byte.clp.zst" : "eight-byte.clp.zst";

        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new ClpIrFileAppender(
                        null,
                        "name1",
                        ignoreExceptions,
                        patternLayout,
                        filter,
                        compressionLevel,
                        useFourByteEncoding,
                        closeFrameOnFlush
                )
        );
        assertThrowsExactly(
                FileNotFoundException.class,
                () -> new ClpIrFileAppender(
                        "",
                        "name2",
                        ignoreExceptions,
                        patternLayout,
                        filter,
                        compressionLevel,
                        useFourByteEncoding,
                        closeFrameOnFlush
                )
        );
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new ClpIrFileAppender(
                        fileName,
                        "name3",
                        ignoreExceptions,
                        null,
                        filter,
                        compressionLevel,
                        useFourByteEncoding,
                        closeFrameOnFlush
                )
        );
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new ClpIrFileAppender(
                        fileName,
                        "name4",
                        ignoreExceptions,
                        patternLayout,
                        filter,
                        Integer.MIN_VALUE,
                        useFourByteEncoding,
                        closeFrameOnFlush
                )
        );
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new ClpIrFileAppender(
                        fileName,
                        "name5",
                        ignoreExceptions,
                        patternLayout,
                        filter,
                        Integer.MAX_VALUE,
                        useFourByteEncoding,
                        closeFrameOnFlush
                )
        );

        // Validate different file paths
        try {
            testEmptyCreation(Paths.get(fileName), patternLayout, useFourByteEncoding);
            testEmptyCreation(Paths.get("a", "b", fileName), patternLayout, useFourByteEncoding);
        } catch (Exception ex) {
            fail(ex);
        }

        // Validate types of layouts
        try {
            testLayouts(fileName, useFourByteEncoding);
        } catch (Exception ex) {
            fail(ex);
        }

        // Test writing
        try {
            testWriting(fileName, false, false, compressionLevel);
            testWriting(fileName, false, true, compressionLevel);
            testWriting(fileName, false, false, compressionLevel + 1);
            testWriting(fileName, true, false, compressionLevel);
            testWriting(fileName, true, true, compressionLevel);
            testWriting(fileName, true, false, compressionLevel + 1);
        } catch (Exception ex) {
            fail(ex);
        }
    }

    /**
     * Tests creating an empty CLP IR stream log with the given path.
     * 
     * @param filePath Path to create. Note that after the test, the entire directory tree specified
     * by the path will be deleted.
     * @param useFourByteEncoding
     * @throws IOException on I/O error
     */
    private void testEmptyCreation(Path filePath, PatternLayout layout, boolean useFourByteEncoding)
            throws IOException {
        String filePathString = filePath.toString();

        ClpIrFileAppender clpIrFileAppender = new ClpIrFileAppender(
                filePathString,
                "name6",
                ignoreExceptions,
                layout,
                filter,
                compressionLevel,
                useFourByteEncoding,
                closeFrameOnFlush
        );
        clpIrFileAppender.closeStream();
        assertTrue(Files.exists(filePath));

        Path parent = filePath.getParent();
        if (null == parent) {
            Files.delete(filePath);
        } else {
            // Get top-level parent
            while (true) {
                Path p = parent.getParent();
                if (null == p) {
                    break;
                }
                parent = p;
            }
            FileUtils.deleteDirectory(parent.toFile());
        }
    }

    /**
     * Test all possible Log4j layouts
     * 
     * @param filePathString
     * @param useFourByteEncoding
     * @throws IOException on I/O error
     */
    private void testLayouts(String filePathString, boolean useFourByteEncoding)
            throws IOException {
        Path filePath = Paths.get(filePathString);

        PatternLayout layout;
        layout = PatternLayout.newBuilder().withPattern("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
                .build();
        testEmptyCreation(filePath, layout, useFourByteEncoding);
    }

    /**
     * Test writing log files
     * 
     * @param fileName
     * @param useFourByteEncoding
     * @param closeFrameOnFlush
     * @param compressionLevel
     * @throws IOException on I/O error
     */
    private void testWriting(
            String fileName,
            boolean useFourByteEncoding,
            boolean closeFrameOnFlush,
            int compressionLevel
    )
            throws IOException {
        // TODO Once decoding support has been added to clp-ffi-java, these tests should all be
        // verified by a decoding the stream and comparing it with the output of an uncompressed
        // file appender.

        Logger logger = LogManager.getLogger(TestFileAppender.class);
        String message = "Static text, dictVar1, 123, 456.7, dictVar2, 987, 654.3";

        ClpIrFileAppender clpIrFileAppender = new ClpIrFileAppender(
                fileName,
                "name7",
                ignoreExceptions,
                patternLayout,
                filter,
                compressionLevel,
                useFourByteEncoding,
                closeFrameOnFlush
        );

        // Log some normal logs
        ;
        for (int i = 0; i < 100; ++i) {
            clpIrFileAppender.append(
                    Log4jLogEvent.newBuilder().setLoggerName("com.yscope.logging.log4j").setLevel(
                            Level.INFO
                    ).setMessage(new SimpleMessage(message)).setThreadName(
                            Thread.currentThread().getName()
                    ).setTimeMillis(0).build()
            );
        }

        // Log with an exception
        clpIrFileAppender.append(
                Log4jLogEvent.newBuilder().setLoggerName("com.yscope.logging.log4j").setLevel(
                        Level.INFO
                ).setMessage(new SimpleMessage(message)).setThreadName(
                        Thread.currentThread().getName()
                ).setTimeMillis(0).setThrown(new FileNotFoundException()).build()
        );

        clpIrFileAppender.flush();

        // Split into a new file
        String fileName2 = fileName + ".2";
        clpIrFileAppender.startNewFile(fileName2);

        // Add some more logs
        for (int i = 0; i < 100; ++i) {
            clpIrFileAppender.append(
                    Log4jLogEvent.newBuilder().setLoggerName("com.yscope.logging.log4j").setLevel(
                            Level.INFO
                    ).setMessage(new SimpleMessage(message)).setThreadName(
                            Thread.currentThread().getName()
                    ).setTimeMillis(0).build()
            );
        }

        clpIrFileAppender.closeStream();

        // Verify file existence
        Path filePath = Paths.get(fileName);
        assertTrue(Files.exists(filePath));
        Files.delete(filePath);
        Path filePath2 = Paths.get(fileName2);
        assertTrue(Files.exists(filePath2));
        Files.delete(filePath2);
    }
}
