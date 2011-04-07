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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

public class FileUtils {

    public static void deleteRecursively(File f) throws IOException {
        if (!f.exists()) {
            return;
        }
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                deleteRecursively(c);
        }
        if (!f.delete()) {
            throw new FileNotFoundException("Failed to delete file: " + f);
        }
    }

    public static File generateTempBaseDir() {
        String tempDir = System.getProperty("java.io.tmpdir");
        File f = new File(tempDir + File.separator + UUID.randomUUID().toString());
        f.mkdir();
        return f;
    }

    public static File generateTempDir(File parent) {
        File f = new File(parent, UUID.randomUUID().toString());
        f.mkdir();
        return f;
    }

    public static File generateTempFileName(File baseDir) {
        return generateTempFileName(baseDir, "");
    }

    public static File generateTempFileName(File baseDir, String suffix) {
        return new File(baseDir + File.separator + UUID.randomUUID().toString() + suffix);
    }

}
