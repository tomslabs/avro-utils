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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvroToTextToAvro {

    public static class AvroToTextMapper extends Mapper<GenericRecord, NullWritable, Text, NullWritable> {

        @Override
        public void map(GenericRecord key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), NullWritable.get());
        }
    }

    public static Job createSubmitableJob(final Configuration conf, final Path inputPath, final Path outputPath) throws IOException {

        conf.set(AvroFileOutputFormat.OUTPUT_SCHEMA, AvroWordCount.WordInputSchema.getSchema().toString());

        conf.setInt("mapred.max.split.size", 1024000);
        conf.setInt("mapred.reduce.tasks", 10);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
        final Job job = new Job(conf, "Word Count");
        job.setJarByClass(AvroToTextToAvro.class);

        job.setInputFormatClass(AvroFileInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(AvroToTextMapper.class);

        job.setReducerClass(JSONTextToAvroRecordReducer.class);

        job.setOutputKeyClass(GenericRecord.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(AvroFileOutputFormat.class);
        AvroFileOutputFormat.setDeflateLevel(job, 3);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}
