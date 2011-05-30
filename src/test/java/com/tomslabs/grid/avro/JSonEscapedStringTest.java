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

import org.junit.Assert;
import org.junit.Test;

public class JSonEscapedStringTest {

    @Test
    public void testQuoteInString() {
        String value = "my monitor has a  24\" size";
        String escapedString =  "my monitor has a  24\\\" size";

        StringBuilder buf = new StringBuilder();
        JSONUtils.writeEscapedString(value, buf);
        Assert.assertEquals(escapedString, buf.toString());
    }
}
