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
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JSONTextToAvroRecordReducer implements Reducer<Text, Text, AvroWrapper<GenericRecord>, NullWritable> {

    
    private JobConf job;
    private Schema schema;
    
    public void configure(JobConf job) {
        this.job = job;
        this.schema = Schema.parse(job.get(AvroJob.OUTPUT_SCHEMA));
    }

    public void close() throws IOException {
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<AvroWrapper<GenericRecord>, NullWritable> output, Reporter reporter) throws IOException {
        GenericRecord record = from(key.toString(), schema);
        AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>(record);
        output.collect(wrapper, NullWritable.get());
    }

    protected GenericRecord from(String jsonString, Schema schema) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
        return reader.read(null, decoder);
    }
}
