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

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AvroRecordToJSONStringTest {

    private File tempDir;
    private File inputDir;

    @Before
    public void setup() {
        tempDir = FileUtils.generateTempBaseDir();
        inputDir = FileUtils.generateTempDir(tempDir);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteRecursively(tempDir);
    }

    @Test
    public void test() throws Throwable {
        String[] words = new String[] { "foo", "foo", "bar", "baz", "foo", "baz" };
        String avroFile = AvroIOUtils.createAvroInputFile(inputDir, words);
        JobConf job = new JobConf();
        FileSplit split = new FileSplit(new Path(avroFile), 0, 4096, job);
        AvroTextRecordReader reader = new AvroTextRecordReader(job, split);
        Text value = reader.createKey();
        Text key = reader.createValue();

        for (String word : words) {
            Assert.assertTrue(reader.next(key, value));
            String jsonString = "{\"word\": \"" + word + "\"}";
            Assert.assertEquals(jsonString, key.toString());
            Assert.assertEquals("", value.toString());
        }

        Assert.assertFalse(reader.next(key, value));

    }
}
