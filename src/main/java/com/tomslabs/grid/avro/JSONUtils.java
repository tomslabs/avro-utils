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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;

public class JSONUtils {

    /**
     * Patch from https://issues.apache.org/jira/browse/AVRO-713 until we switch
     * to Avro 1.5.0
     */
    public static void writeJSON(Object datum, StringBuilder buffer) {
        if (datum instanceof IndexedRecord) {
            buffer.append("{");
            int count = 0;
            IndexedRecord record = (IndexedRecord) datum;
            for (Field f : record.getSchema().getFields()) {
                writeJSON(f.name(), buffer);
                buffer.append(": ");
                writeJSON(record.get(f.pos()), buffer);
                if (++count < record.getSchema().getFields().size()) {
                    buffer.append(", ");
                }
            }
            buffer.append("}");
        } else if (datum instanceof Collection) {
            Collection<?> array = (Collection<?>) datum;
            buffer.append("[");
            long last = array.size() - 1;
            int i = 0;
            for (Object element : array) {
                writeJSON(element, buffer);
                if (i++ < last) {
                    buffer.append(", ");
                }
            }
            buffer.append("]");
        } else if (datum instanceof Map) {
            buffer.append("{");
            int count = 0;
            @SuppressWarnings(value = "unchecked")
            Map<Object, Object> map = (Map<Object, Object>) datum;
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                writeJSON(entry.getKey(), buffer);
                buffer.append(": ");
                writeJSON(entry.getValue(), buffer);
                if (++count < map.size()) {
                    buffer.append(", ");
                }
            }
            buffer.append("}");
        } else if (datum instanceof CharSequence) {
            buffer.append("\"");
            writeEscapedString(datum.toString(), buffer);
            buffer.append("\"");
        } else if (datum instanceof ByteBuffer) {
            buffer.append("{\"bytes\": \"");
            ByteBuffer bytes = (ByteBuffer) datum;
            for (int i = bytes.position(); i < bytes.limit(); i++) {
                buffer.append((char) bytes.get(i));
            }
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
                    for (int j = 0; j < 4 - builder.length(); j++) {
                        builder.append('0');
                    }
                    builder.append(string.toUpperCase());
                } else {
                    builder.append(ch);
                }
            }
        }
    }

}
