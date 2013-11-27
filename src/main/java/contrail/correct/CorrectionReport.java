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
// Author: Jeremy Lewi(jeremy@lewi.us)

package contrail.correct;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.stages.StageInfo;
import contrail.stages.StageInfoHelper;

/**
 * Take a stage info file containing information about error correction
 * and print out a report of useful information.
 *
 */
public class CorrectionReport extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      CorrectionReport.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String,
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def :
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

  protected OutputStream createOutputStream() {
    String outputFile = (String) this.stage_options.get("outputpath");

    Path outPath = null;
    if (!outputFile.isEmpty()) {
      outPath = new Path(outputFile);
    }

    try {
      OutputStream outStream = null;
      if (outPath == null) {
        // Write to standard output.
        outStream = System.out;
      } else {
        outStream = outPath.getFileSystem(getConf()).create(outPath);
      }

      return outStream;
    } catch (IOException e) {
      sLogger.fatal("IOException", e);
    }
    return null;
  }

  @Override
  protected void stageMain() {
    // TODO Auto-generated method stub
    StageInfoHelper infoHelper = StageInfoHelper.loadMostRecent(
        getConf(), (String) stage_options.get("inputpath"));

    OutputStream outStream = createOutputStream();

    HashMap<String, StageInfo> stages = new HashMap<String, StageInfo>();
    for (StageInfo stageInfo: infoHelper.DFSSearch()) {
      stages.put(stageInfo.getStageClass().toString(), stageInfo);
    }

    if (stages.containsKey(JoinReads.class.getName())) {
      StageInfo stage = stages.get(JoinReads.class.getName());
      System.out.println(stage.getStageClass());

      StageInfoHelper helper = new StageInfoHelper(stage);
      HashMap<String, Long> counters = helper.getCounters();
      for (String counterName :
        new String[] {"Map input records", "Reduce OutputGroups"}) {
        if (counters.containsKey(counterName)) {
          System.out.println(String.format(
              "%s:\t %d", counterName, counters.get(counterName)));
        }
      }
    }

    if (stages.containsKey(InvokeFlash.class.getName())) {
      StageInfo stage = stages.get(InvokeFlash.class.getName());
      System.out.println(stage.getStageClass());

      StageInfoHelper helper = new StageInfoHelper(stage);
      HashMap<String, Long> counters = helper.getCounters();
      for (String counterName :
        new String[] {"Map input records", "Reduce OutputGroups"}) {
        if (counters.containsKey(counterName)) {
          System.out.println(String.format(
              "%s:\t %d", counterName, counters.get(counterName)));
        }
      }
    }

    try {
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("IOException", e);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CorrectionReport(), args);
    System.exit(res);
  }
}
