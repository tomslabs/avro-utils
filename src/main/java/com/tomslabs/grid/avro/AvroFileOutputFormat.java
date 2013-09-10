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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroFileOutputFormat<T> extends FileOutputFormat<T, Object> {

   private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileOutputFormat.class);
   
    /**
     * When the map/reduce job outputs Avro record, the String representation of
     * the Avro schema <em>MUST</em> be set on the job's configuration with this key.
     */
    public static final String OUTPUT_SCHEMA = "schema";

    private static final String EXT = org.apache.avro.mapred.AvroOutputFormat.EXT;
    private static final String DEFLATE_LEVEL_KEY = org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY;
    private static final int DEFAULT_DEFLATE_LEVEL = 1;

    public static void setDeflateLevel(Job job, int level) {
        FileOutputFormat.setCompressOutput(job, true);
        job.getConfiguration().setInt(DEFLATE_LEVEL_KEY, level);
    }

    @Override
    public RecordWriter<T, Object> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();

        Schema schema = getWriteSchema(config);
        DatumWriter<T> datumWriter = getDatumWriter(config);

        final DataFileWriter<T> writer = new DataFileWriter<T>(datumWriter);

        if (getCompressOutput(context)) {
            int level = config.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
            writer.setCodec(CodecFactory.deflateCodec(level));
        }

        Path file = getDefaultWorkFile(context, EXT);
        FileSystem fs = file.getFileSystem(config);

        writer.create(schema, fs.create(file));

        return new AvroRecordWriter<T>(writer);
    }

    protected Schema getWriteSchema(Configuration config) {
        String schemaStr = config.get(OUTPUT_SCHEMA);
        return Schema.parse(schemaStr);
    }

    protected DatumWriter<T> getDatumWriter(Configuration config) {
        DatumWriter<T> datumWriter = "specific".equals(config.get("avro.output.api")) ? new SpecificDatumWriter<T>() : new GenericDatumWriter<T>();
        return datumWriter;
    }

    private static final class AvroRecordWriter<T> extends RecordWriter<T, Object> {
        private DataFileWriter<T> writer;

        private AvroRecordWriter(DataFileWriter<T> writer) {
            this.writer = writer;
        }

        @Override
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            try {
               LOGGER.warn("Closing the avro resource");
                writer.close();
            } catch (AvroRuntimeException e) {
                LOGGER.warn("Trying to close a closed avro resource", e);
                arg0.progress();
            }
        }

        @Override
        public void write(T datum, Object nothing) throws IOException, InterruptedException {
            writer.append(datum);
        }
    }
}
