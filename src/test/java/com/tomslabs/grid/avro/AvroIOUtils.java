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

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

public class AvroIOUtils {

    public static String createAvroInputFile(File inputDir, String... words) throws Throwable {
        File tempFile = new File(inputDir + File.separator + "word-count.avro");
        Schema schema = AvroWordCount.WordInputSchema.getSchema();
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
        writer.create(schema, tempFile);
        for (String word : words) {
            AvroIOUtils.addRecord("word", word, writer, schema);
        }
    
        writer.close();
        return tempFile.getAbsolutePath();
    }

    public static void addRecord(String key, String value, DataFileWriter<GenericRecord> writer, Schema schema) throws Throwable {
        GenericRecord rec = new Record(schema);
        rec.put(key, value);
        writer.append(rec);
    }

    public static void dumpAvroFiles(File dir) throws IOException {
        if (dir.isDirectory()) {
            for (File f : dir.listFiles()) {
                if (f.getName().endsWith(".avro")) {
                    System.out.println(">>> " + f.getAbsolutePath());
                    DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(f, new GenericDatumReader<GenericRecord>());
                    while (reader.hasNext()) {
                        System.out.println(reader.next().toString());
                    }
                    reader.close();
                }
            }
        }
    }

}
