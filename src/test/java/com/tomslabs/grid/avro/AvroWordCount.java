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
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvroWordCount {

    public static class WordCountSchema {

        public static final String KEY = "key";
        public static final String VALUE = "value";

        public static Schema getOuputSchema() {
            Schema schema = Pair.getPairSchema(Schema.create(Type.STRING), Schema.create(Type.INT));
            return schema;

        }
    }

    public static class WordCountMapper extends Mapper<GenericRecord, NullWritable, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(GenericRecord key, NullWritable value, Context context) throws IOException, InterruptedException {
            String title = key.get("word").toString();
            context.write(new Text(title), ONE);
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, GenericRecord, NullWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Record r = new Record(Schema.parse(context.getConfiguration().get(AvroFileOutputFormat.OUTPUT_SCHEMA)));
            r.put(WordCountSchema.KEY, key.toString());
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            r.put(WordCountSchema.VALUE, sum);
            context.write(r, NullWritable.get());
        }
    }

    public static Job createSubmitableJob(final Configuration conf, final Path inputPath, final Path outputPath) throws IOException {

        conf.set(AvroFileOutputFormat.OUTPUT_SCHEMA, WordCountSchema.getOuputSchema().toString());

        conf.setInt("mapred.max.split.size", 1024000);
        conf.setInt("mapred.reduce.tasks", 10);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
        final Job job = new Job(conf, "Word Count");
        job.setJarByClass(AvroWordCount.class);

        job.setInputFormatClass(AvroFileInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(WordCountMapper.class);

        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(GenericRecord.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(AvroFileOutputFormat.class);
        AvroFileOutputFormat.setDeflateLevel(job, 3);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}
