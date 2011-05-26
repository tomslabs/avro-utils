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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
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
        if (!reader.hasNext() || reader.pastSync(end)) {
            return false;
        }
        StringBuilder buf = new StringBuilder();
        JSONUtils.writeJSON(reader.next(), buf);
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
}
