/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tomslabs.grid.avro;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.Progressable;

/** An {@link org.apache.hadoop.mapred.OutputFormat} for Avro data files. */
public class TextTypedBytesToAvroOutputFormat extends FileOutputFormat<TypedBytesWritable, TypedBytesWritable> {


    /** The file name extension for avro data files. */
    public final static String EXT = ".avro";

    /** The configuration key for Avro deflate level. */
    public static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";

    /** The default deflate level. */
    public static final int DEFAULT_DEFLATE_LEVEL = 1;

    /** Enable output compression using the deflate codec and specify its level. */
    public static void setDeflateLevel(JobConf job, int level) {
        FileOutputFormat.setCompressOutput(job, true);
        job.setInt(DEFLATE_LEVEL_KEY, level);
    }

    public RecordWriter<TypedBytesWritable, TypedBytesWritable> getRecordWriter(FileSystem ignore, JobConf job, String name, Progressable prog)
            throws IOException {

        boolean isMapOnly = job.getNumReduceTasks() == 0;
        Schema schema = isMapOnly ? AvroJob.getMapOutputSchema(job) : AvroJob.getOutputSchema(job);

        final DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));

        if (FileOutputFormat.getCompressOutput(job)) {
            int level = job.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
            writer.setCodec(CodecFactory.deflateCodec(level));
        }

        // copy metadata from job
        for (Map.Entry<String, String> e : job) {
            if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
                writer.setMeta(e.getKey().substring(AvroJob.TEXT_PREFIX.length()), e.getValue());
            if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
                writer.setMeta(e.getKey().substring(AvroJob.BINARY_PREFIX.length()), URLDecoder.decode(e.getValue(), "ISO-8859-1").getBytes("ISO-8859-1"));
        }

        Path path = FileOutputFormat.getTaskOutputPath(job, name + EXT);
        writer.create(schema, path.getFileSystem(job).create(path));

        return new AvroRecordWriter(writer, schema);
    }
    
    private final class AvroRecordWriter implements RecordWriter<TypedBytesWritable, TypedBytesWritable> {
        private final DataFileWriter<GenericRecord> writer;
        private final GenericDatumReader<GenericRecord> textReader;
        private final Schema schema;

        public AvroRecordWriter(DataFileWriter<GenericRecord> writer, Schema schema) {
            this.writer = writer;
            this.schema = schema;
            this.textReader = new GenericDatumReader<GenericRecord>(schema);
        }

        public void write(TypedBytesWritable ignore, TypedBytesWritable value) throws IOException {
            String jsonRecord = (String) value.getValue();
            Decoder decoder = DecoderFactory.get().jsonDecoder(this.schema, jsonRecord);
            GenericRecord record = textReader.read(null, decoder);
            writer.append(record);
        }

        public void close(Reporter reporter) throws IOException {
            writer.close();
        }
    }

}
