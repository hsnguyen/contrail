/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.scaffolding;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import contrail.sequences.FastUtil;
import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.ContrailLogger;

/**
 * Simple binary to filter all scaffolds less than some length.
 */
public class FilterScaffoldsByLength extends NonMRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(FilterScaffoldsByLength.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition filter = new ParameterDefinition(
        "min_length",
        "The minimum length for the scaffolds",
        Integer.class,
        null);
    defs.put(filter.getName(), filter);
    return Collections.unmodifiableMap(defs);
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    int minLength = (Integer) stage_options.get("min_length");

    int numRead = 0;
    int numWritten = 0;

    FastaFileReader reader = new FastaFileReader(inputPath);

    FileOutputStream outStream = null;
    try {
      outStream = new FileOutputStream(outputPath);
    } catch (FileNotFoundException e) {
      sLogger.fatal("File not found exception.", e);
    }

    while (reader.hasNext()) {
      ++numRead;
      FastaRecord record = reader.next();
      if (record.getRead().length() >= minLength) {
        ++numWritten;
        try {
          FastUtil.writeFastARecord(outStream, record);
        } catch (IOException e) {
          sLogger.fatal("Exception writing record.", e);
        }
      }
    }

    sLogger.info(String.format("Number of input scaffolds: %d", numRead));
    sLogger.info(String.format("Number of output scaffolds: %d", numWritten));

    try {
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("Exception closing output.", e);
    }
    reader.close();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new FilterScaffoldsByLength(), args);
    System.exit(res);
  }
}
