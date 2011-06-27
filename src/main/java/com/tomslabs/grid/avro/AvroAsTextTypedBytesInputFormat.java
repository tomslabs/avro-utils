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
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

public class AvroAsTextTypedBytesInputFormat extends FileInputFormat<TypedBytesWritable, TypedBytesWritable> {

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
      List<FileStatus> result = new ArrayList<FileStatus>();
      for (FileStatus file : super.listStatus(job))
        if (file.getPath().getName().endsWith(AvroOutputFormat.EXT))
          result.add(file);
      return result.toArray(new FileStatus[0]);
    }

    @Override
    public RecordReader<TypedBytesWritable, TypedBytesWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
      reporter.setStatus(split.toString());
      return new AvroTypedBytesRecordReader<GenericRecord>(job, (FileSplit) split);
    }

    static class AvroTypedBytesRecordReader<T> implements RecordReader<TypedBytesWritable, TypedBytesWritable> {

        private FileReader<T> reader;
        private long start;
        private long end;

        public AvroTypedBytesRecordReader(JobConf job, FileSplit split) throws IOException {
            this(new DataFileReader<T>(new FsInput(split.getPath(), job), new GenericDatumReader<T>()), split);
        }

        protected AvroTypedBytesRecordReader(FileReader<T> reader, FileSplit split) throws IOException {
            this.reader = reader;
            reader.sync(split.getStart()); // sync to start
            this.start = reader.tell();
            this.end = split.getStart() + split.getLength();
        }
        
        public TypedBytesWritable createKey() {
            return new TypedBytesWritable();
        }

        public TypedBytesWritable createValue() {
            return new TypedBytesWritable();
        }
        
        public boolean next(TypedBytesWritable key, TypedBytesWritable value) throws IOException {
            if (!reader.hasNext() || reader.pastSync(end)) {
                return false;
            }

            key.setValue("");
            // until https://github.com/apache/avro/pull/2/ is fixed, we can not rely
            // on toString() to provide a correct JSON string with unicode.
            //value.setValue(reader.next().toString());
            StringBuilder buf = new StringBuilder();
            JSONUtils.writeJSON(reader.next(), buf);
            value.setValue(buf.toString());
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

}
