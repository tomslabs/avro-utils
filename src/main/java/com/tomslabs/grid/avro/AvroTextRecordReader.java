/*
 * Copyright 2011, Bestofmedia, Inc.
 * 
 * Bestofmedia licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.tomslabs.grid.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class AvroTextRecordReader<T> implements RecordReader<Text, Text> {

    private FileReader<T> reader;
    private long start;
    private long end;

    public AvroTextRecordReader(JobConf job, FileSplit split) throws IOException {
        this(new DataFileReader<T>(new FsInput(split.getPath(), job), new GenericDatumReader<T>()), split);
    }

    protected AvroTextRecordReader(FileReader<T> reader, FileSplit split) throws IOException {
        this.reader = reader;
        reader.sync(split.getStart()); // sync to start
        this.start = reader.tell();
        this.end = split.getStart() + split.getLength();
    }

    public Text createKey() {
        return new Text();
    }

    public Text createValue() {
        return new Text();
    }

    public boolean next(Text key, Text value) throws IOException {
        if (!reader.hasNext() || reader.pastSync(end))
            return false;
        StringBuilder buf = new StringBuilder();
        toString(reader.next(), buf);
        key.set(buf.toString());
        return true;
    }

    public float getProgress() throws IOException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getPos() - start) / (float) (end - start));
        }
    }

    public long getPos() throws IOException {
        return reader.tell();
    }

    public void close() throws IOException {
        reader.close();
    }

    /**
     * Patch from https://issues.apache.org/jira/browse/AVRO-713 until we switch
     * to Avro 1.5.0
     */
    protected static void toString(Object datum, StringBuilder buffer) {
        if (datum instanceof IndexedRecord) {
            buffer.append("{");
            int count = 0;
            IndexedRecord record = (IndexedRecord) datum;
            for (Field f : record.getSchema().getFields()) {
                toString(f.name(), buffer);
                buffer.append(": ");
                toString(record.get(f.pos()), buffer);
                if (++count < record.getSchema().getFields().size())
                    buffer.append(", ");
            }
            buffer.append("}");
        } else if (datum instanceof Collection) {
            Collection<?> array = (Collection<?>) datum;
            buffer.append("[");
            long last = array.size() - 1;
            int i = 0;
            for (Object element : array) {
                toString(element, buffer);
                if (i++ < last)
                    buffer.append(", ");
            }
            buffer.append("]");
        } else if (datum instanceof Map) {
            buffer.append("{");
            int count = 0;
            @SuppressWarnings(value = "unchecked")
            Map<Object, Object> map = (Map<Object, Object>) datum;
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                toString(entry.getKey(), buffer);
                buffer.append(": ");
                toString(entry.getValue(), buffer);
                if (++count < map.size())
                    buffer.append(", ");
            }
            buffer.append("}");
        } else if (datum instanceof CharSequence) {
            buffer.append("\"");
            writeEscapedString(datum.toString(), buffer);
            buffer.append("\"");
        } else if (datum instanceof ByteBuffer) {
            buffer.append("{\"bytes\": \"");
            ByteBuffer bytes = (ByteBuffer) datum;
            for (int i = bytes.position(); i < bytes.limit(); i++)
                buffer.append((char) bytes.get(i));
            buffer.append("\"}");
        } else {
            buffer.append(datum);
        }
    }

    /* Adapted from http://code.google.com/p/json-simple */
    protected static void writeEscapedString(String string, StringBuilder builder) {
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            switch (ch) {
            case '"':
                builder.append("\\\"");
                break;
            case '\\':
                builder.append("\\\\");
                break;
            case '\b':
                builder.append("\\b");
                break;
            case '\f':
                builder.append("\\f");
                break;
            case '\n':
                builder.append("\\n");
                break;
            case '\r':
                builder.append("\\r");
                break;
            case '\t':
                builder.append("\\t");
                break;
            case '/':
                builder.append("\\/");
                break;
            default:
                // Reference: http://www.unicode.org/versions/Unicode5.1.0/
                if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F') || (ch >= '\u2000' && ch <= '\u20FF')) {
                    String hex = Integer.toHexString(ch);
                    builder.append("\\u");
                    for (int j = 0; j < 4 - builder.length(); j++)
                        builder.append('0');
                    builder.append(string.toUpperCase());
                } else {
                    builder.append(ch);
                }
            }
        }
    }
}
