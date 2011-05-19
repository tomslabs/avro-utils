package com.tomslabs.grid.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JSONTextToAvroRecordReducer extends Reducer<Text, Text, GenericRecord, NullWritable> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Schema schema = Schema.parse(context.getConfiguration().get(AvroFileOutputFormat.OUTPUT_SCHEMA));
        GenericRecord record = new Record(schema);

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = new JsonDecoder(schema, key.toString());
        record = reader.read(null, decoder);

        context.write(record, NullWritable.get());
    }
}
