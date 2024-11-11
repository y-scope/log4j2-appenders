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

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * CompressionPatternLayoutContainer is a Log4j pattern layout derived from the message's original
 * layout, the only difference being the timestamp pattern is separated from the rest of the
 * pattern. This is because CLP stores timestamps separately from the message's content. During
 * decompression, the timestamp string will be regenerated using the stored timestamp and timestamp
 * pattern.
 * <p>
 * Note: We only support a PatternLayout with a timestamp for now. Otherwise, we error out
 * immediately.
 * <p>
 * Limitations: Currently assumes the timestamp is always at the beginning, if not, error out
 * immediately. This limitation could be removed in future versions if necessary.
 */
public class CompressionPatternLayoutContainer {
    private final PatternLayout compressionPatternLayout;
    private final String timestampPattern;

    private final PatternLayoutBufferDestination logMsgByteBufferDestination;

    public CompressionPatternLayoutContainer(final PatternLayout patternLayout) {
        if (null == patternLayout) {
            throw new IllegalArgumentException("patternLayout is required");
        }

        // Parse the timestamp out into 2 parts
        // - tsPattern,
        // - afterTsPattern (suffix)
        // Limitation: Currently only supports patternLayout with the timestamp converter field
        // "%d{xxx}" at the front
        String timestampPatternRegex = "%d\\{(?<timestampPattern>[^}]*)}(?<suffix>.*)";
        Pattern pattern = Pattern.compile(timestampPatternRegex, Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(patternLayout.getConversionPattern());
        if (false == matcher.find()) {
            throw new UnsupportedOperationException(
                    "Pattern layout must contain timestamp converter. Provided pattern: "
                            + patternLayout.getConversionPattern()
            );
        }
        String compressionPatternLayoutStr = matcher.group("suffix");
        compressionPatternLayout = PatternLayout.newBuilder().withPattern(
                compressionPatternLayoutStr
        ).build();
        timestampPattern = matcher.group("timestampPattern");
        logMsgByteBufferDestination = new PatternLayoutBufferDestination();
    }

    public ByteBuffer encodeLogMsg(final LogEvent event) {
        logMsgByteBufferDestination.clear();
        compressionPatternLayout.encode(event, logMsgByteBufferDestination);
        logMsgByteBufferDestination.getByteBuffer().flip();
        return logMsgByteBufferDestination.getByteBuffer();
    }

    public String getTimestampPattern() { return timestampPattern; }

    /** Method used for debugging only */
    public PatternLayout getCompressionPatternLayout() {
        return compressionPatternLayout;
    }
}
