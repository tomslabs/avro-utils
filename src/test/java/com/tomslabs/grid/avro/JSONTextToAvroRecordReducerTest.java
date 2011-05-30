package com.tomslabs.grid.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.tomslabs.grid.avro.AvroWordCount.WordCountSchema;


public class JSONTextToAvroRecordReducerTest {

    @Test
    public void testFrom() throws IOException {
        JSONTextToAvroRecordReducer reducer = new JSONTextToAvroRecordReducer();
        
        String jsonString = "{\"key\": \"foo\", \"value\": 3}" ;
        Schema schema = WordCountSchema.getSchema();
        
        GenericRecord record = reducer.from(jsonString, schema);
        assertNotNull(record);
        assertEquals("foo", record.get("key").toString());
        assertEquals(3, record.get("value"));
    }
}
