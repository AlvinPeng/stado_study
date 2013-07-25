/*****************************************************************************
 * Copyright (C) 2008 EnterpriseDB Corporation.
 * Copyright (C) 2011 Stado Global Development Group.
 *
 * This file is part of Stado.
 *
 * Stado is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stado is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stado.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can find Stado at http://www.stado.us
 *
 ****************************************************************************/
package org.postgresql.stado.engine.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

import org.postgresql.stado.exception.XDBDataReaderException;


/**
 * Purpose of Data Reader is to split data source on rows
 * StreamReaderThread assumes the data source is a stream of text (Plain text or
 * CSV format)
 *
 * @author amart
 */
public class StreamReaderThread implements Callable<Boolean> {

    private static final byte CR = '\r';

    private static final byte LF = '\n';

    private DataReaderAndProcessorBuffer<byte[]> loadBuffer;

    private InputStream dataSourceFile;

    // 0 - not determined
    // 1 - CR
    // 2 - LF
    // 3 - CRLF
    // 4 - mixed
    private int lineDelimiterStyle = 0;

    private byte[] readBuffer = new byte[1024];

    private int readPos = 0;

    private int readEnd = 0;

    private boolean eof = false;

    private boolean skipLF = false;

    private byte[] buffer = new byte[128];

    private int bufferIdx;

    private boolean preserveLineEnd;

    /**
     * Creates a new instance of StreamReaderThread
     * @param inputStream
     * @param buffer
     */
    public StreamReaderThread(InputStream inputStream, DataReaderAndProcessorBuffer<byte[]> buffer, boolean preserveLineEnd) {
        if (inputStream == null) {
            throw new NullPointerException("inputStream");
        }
        if (buffer == null) {
            throw new NullPointerException("buffer");
        }
        dataSourceFile = inputStream;
        loadBuffer = buffer;
        this.preserveLineEnd = preserveLineEnd;
    }

    /**
     * Closes data source.
     *
     */
    public void close() {
        try {
            dataSourceFile.close();
        } catch (Exception ex) {
            // Ignore exception message.
        }
    }

    /**
     * Parse out one line from the data source stream
     * @return
     * @throws XDBDataReaderException
     */
    private boolean readLineFromStream() throws XDBDataReaderException {
        try {
            boolean eol = false;
            while (!eol) {
                if (readPos == readEnd) {
                    fill();
                }
                int start = readPos;
                while (!eol && readPos < readEnd) {
                    switch (readBuffer[readPos++]) {
                    case LF:
                        if (skipLF) {
                            start = readPos;
                            setLineDelimiterStyle(3);
                            skipLF = false;
                        } else {
                            ensureBufferCapacity(bufferIdx + readPos - start);
                            System.arraycopy(readBuffer, start, buffer, bufferIdx, readPos - start);
                            bufferIdx += readPos - start - 1;
                            if (checkEOF()) {
                                return false;
                            }
                            setLineDelimiterStyle(2);
                            return true;
                        }
                        break;
                    case CR:
                        skipLF = true;
                        eol = true;
                        break;
                    default:
                        if (skipLF) {
                            setLineDelimiterStyle(1);
                        }
                    skipLF = false;
                    }
                }
                ensureBufferCapacity(bufferIdx + readPos - start);
                System.arraycopy(readBuffer, start, buffer, bufferIdx, readPos - start);
                bufferIdx += readPos - start - (eol ? 1 : 0);
                if (checkEOF()) {
                    return false;
                }
                if (eof) {
                    break;
                }
            }
            return !eof || readPos < readEnd;
        } catch (IOException ioe) {
            throw new XDBDataReaderException(ioe);
        }
    }

    /**
     * Read new portion of data from the data source to the buffer
     * @throws IOException
     */
    private void fill() throws IOException {
        readPos = 0;
        readEnd = dataSourceFile.read(readBuffer);
        eof = readEnd < 0;
    }

    /**
     * Ensure the size of {@link #buffer} is not less then specified
     * Enlarge buffer if needed
     * @param capacity
     */
    private void ensureBufferCapacity(int capacity) {
        if (buffer.length < capacity) {
            byte[] newBuffer = new byte[Math.max(2 * buffer.length, capacity)];
            System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            buffer = newBuffer;
        }
    }

    /**
     * Check if current line is an EOF marker (\.)
     * @return
     */
    private boolean checkEOF() {
        // Check for EOF marker
        if (bufferIdx == 2 && buffer[0] == '\\' && buffer[1] == '.') {
            eof = true;
            readPos = readEnd;
            bufferIdx = 0;
            return true;
        }
        return false;
    }

    /**
     * Register the style of delimiter after last read row
     * <ul>
     * <li>1 - CR (Mac)</li>
     * <li>2 - LF (Unix)</li>
     * <li>3 - CRLF (Windows)</li>
     * </ul>
     * @param style
     */
    private void setLineDelimiterStyle(int style) throws XDBDataReaderException {
        if (preserveLineEnd) {
            byte[] out = new byte[bufferIdx + (style == 3 ? 2 : 1)];
            System.arraycopy(buffer, 0, out, 0, bufferIdx);
            if ((style & 1) == 1) {
                // 1 or 3
                out[bufferIdx++] = '\r';
            }
            if ((style & 2) == 2) {
                // 2 or 3
                out[bufferIdx++] = '\n';
            }
            loadBuffer.putRowValue(out);
            bufferIdx = 0;
        }
        else if (style != lineDelimiterStyle) {
            if (lineDelimiterStyle == 0) {
                lineDelimiterStyle = style;
            } else {
                throw new XDBDataReaderException("The data file has mixed line ends");
            }
        }
    }

    /**
     * Detect if lines in the source stream have had different delimiters
     * @return
     */
    public boolean mixedLineEndsFound() {
        return lineDelimiterStyle == 4;
    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    public Boolean call() throws XDBDataReaderException {
        try {
            while (readLineFromStream()) {
                if (!preserveLineEnd) {
                    byte[] out = new byte[bufferIdx];
                    System.arraycopy(buffer, 0, out, 0, bufferIdx);
                    loadBuffer.putRowValue(out);
                    bufferIdx = 0;
                }
            }
            if (bufferIdx > 0) {
                byte[] out = new byte[bufferIdx];
                System.arraycopy(buffer, 0, out, 0, bufferIdx);
                loadBuffer.putRowValue(out);
                bufferIdx = 0;
            }
            return true;
        } catch (XDBDataReaderException dre) {
            throw dre;
        } catch (Exception ex) {
            throw new XDBDataReaderException(ex);
        } finally {
            close();
            if (loadBuffer != null) {
                loadBuffer.markFinished();
            }
        }
    }
}
