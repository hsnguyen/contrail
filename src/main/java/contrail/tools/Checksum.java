/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.FileHelper;

/**
 * Print the checksums for some files.
 *
 * This works but isn't that useful. HDFS uses a different checksum algorithm
 * than md5 so you can't use this to compare to files on a local fs.
 */
public class Checksum extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(PrettyPrint.class);

  @Override
  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    defs.remove("outputpath");
    // Make outputpath an optional argument. Default will be to use standard
    // output.
    ParameterDefinition output = new ParameterDefinition(
        "outputpath", "The file to write the data to. Default is stdout.",
        String.class, "");
    defs.put(output.getName(), output);
    return Collections.unmodifiableMap(defs);
  }

  // TODO(jeremy@lewi.us): I was just lazy and declared this function
  // to throw an exception we should catch and handle the exceptions.
  public void computeSums() throws Exception {
    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }
    String inputFile = (String) this.stage_options.get("inputpath");
    String outputFile = (String) this.stage_options.get("outputpath");

    Path outPath = null;
    if (!outputFile.isEmpty()) {
      outPath = new Path(outputFile);
    }

    ArrayList<Path> matchingFiles = FileHelper.matchGlobWithDefault(
        getConf(), inputFile, "*");
    try {
      OutputStream outStream = null;
      if (outPath == null) {
        // Write to standard output.
        outStream = System.out;
      } else {
        outStream = outPath.getFileSystem(getConf()).create(outPath);
      }

      for (Path inFile : matchingFiles) {
        FileSystem pathFs = inFile.getFileSystem(getConf());
        FileChecksum sum = pathFs.getFileChecksum(inFile);
        String output = null;
        if (sum == null) {
          output = String.format("%s: %s\n", inFile.toString(), "null");
        } else {
          output = String.format(
              "%s:  %s\n", inFile.toString(), sum.toString());
        }
        outStream.write(output.getBytes(Charset.defaultCharset()));
      }
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("IOException", e);
    }
  }

  @Override
  protected void stageMain() {
    try {
      computeSums();
    } catch (Exception e) {
      sLogger.fatal("There was a problem computing the checksums.", e);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Checksum(), args);
    System.exit(res);
  }
}
