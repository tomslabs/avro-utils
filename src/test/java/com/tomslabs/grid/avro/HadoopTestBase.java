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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class HadoopTestBase {

    protected static FileSystem localFs;
    protected static Configuration localConf;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("hadoop.tmp.dir", new File("target/hadoop-test").getAbsolutePath());
        System.setProperty("test.build.data", new File("target/hadoop-test").getAbsolutePath());

        localConf = new Configuration();
        localConf.set("fs.default.name", "file://" + new File("target/hadoop-test/dfs").getAbsolutePath());
        localFs = FileSystem.getLocal(localConf);
        localConf.set("hadoop.log.dir", new Path("target/hadoop-test/logs").makeQualified(localFs).toString());
        localConf.set("mapred.system.dir", new Path("target/hadoop-test/mapred/sys").makeQualified(localFs).toString());
        localConf.set("mapred.local.dir", new File("target/hadoop-test/mapred/local").getAbsolutePath());
        localConf.set("mapred.temp.dir", new File("target/hadoop-test/mapred/tmp").getAbsolutePath());
        System.setProperty("hadoop.log.dir", localConf.get("hadoop.log.dir"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (localFs != null)
            localFs.close();
    }

    protected Path localResourceToPath(String path, String target) throws IOException {
        try {
            final InputStream resource = this.getClass().getResourceAsStream(path);
            if (resource == null)
                throw new IllegalArgumentException(path + " not found");
            final Path targetPath = new Path(localFs.getWorkingDirectory(), "target/hadoop-test/imported/" + target).makeQualified(localFs);
            localFs.delete(targetPath, true);
            OutputStream out = null;
            try {
                out = localFs.create(targetPath, true);
                IOUtils.copyBytes(resource, out, localConf, true);
            } catch (IOException e) {
                IOUtils.closeStream(out);
                IOUtils.closeStream(resource);
                throw e;
            }
            return targetPath;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected Path localFileToPath(String path) {
        return localFs.makeQualified(new Path(path));
    }
}
