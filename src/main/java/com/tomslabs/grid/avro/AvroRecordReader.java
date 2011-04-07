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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AvroRecordReader<T> extends RecordReader<T, Object> {

    private FsInput in;
    private DataFileReader<T> reader;
    private T datum = null;
    private long start;
    private long end;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        Configuration config = context.getConfiguration();
        Path path = fileSplit.getPath();

        this.in = new FsInput(path, config);

        DatumReader<T> datumReader = getDatumReader(config);

        this.reader = new DataFileReader<T>(in, datumReader);
        reader.sync(fileSplit.getStart()); // sync to start

        this.start = in.tell();
        this.end = fileSplit.getStart() + split.getLength();
    }

    protected DatumReader<T> getDatumReader(Configuration config) {
        String schemaStr = config.get(AvroJob.INPUT_SCHEMA);
        DatumReader<T> datumReader = "specific".equals(config.get("avro.input.api")) ? new SpecificDatumReader<T>() : new GenericDatumReader<T>();
        // set the expected schema to be what is configured
        // if not configured, the initialization of the file reader
        // will set it to the file contents
        if (schemaStr != null) {
            Schema schema = Schema.parse(schemaStr);
            datumReader.setSchema(schema);
        }
        return datumReader;
    }

    @Override
    public float getProgress() throws IOException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (in.tell() - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        reader.close();
    }

    @Override
    public T getCurrentKey() throws IOException, InterruptedException {
        return datum;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!reader.hasNext() || reader.pastSync(end)) {
            return false;
        }
        datum = reader.next(datum);
        return true;
    }

}
