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

package contrail.stages;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.correct.CorrectionReport;
import contrail.util.FileHelper;

/**
 * Take a stage info file containing information about contig assembly and
 * print out a report of useful information.
 *
 */
public class AssemblyReport extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      CorrectionReport.class);

  private class CounterTuple{
   public String name;
   public Long value;
  }

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
    String inputPath = (String)stage_options.get("inputpath");
    ArrayList<Path> stageFiles = FileHelper.matchGlobWithDefault(
        getConf(), inputPath, "*.json");

    if (stageFiles.isEmpty()) {
      sLogger.fatal("No stage info files matched:" + inputPath);
      System.exit(-1);
    }

    Collections.sort(stageFiles, Collections.reverseOrder());

    HashMap<String, StringBuffer> stageMatches =
        new HashMap<String, StringBuffer>();

    Path stageFile = stageFiles.get(0);
    sLogger.info("Using: " + stageFile.toString());

    StageInfoHelper infoHelper = StageInfoHelper.loadFromPath(
        getConf(), stageFile);

    // Count how many times each stage runs.
    HashMap<String, Integer> stageCounts = new HashMap<String,Integer>();
    for (StageInfo stageInfo : infoHelper.DFSSearch()) {
      String stageClass = stageInfo.getStageClass().toString();
      if (!stageCounts.containsKey(stageClass)) {
        stageCounts.put(stageClass, 0);
      }
      Integer count = stageCounts.get(stageClass) + 1;
      stageCounts.put(stageClass, count);
    }

    OutputStream outStream = createOutputStream();
    PrintStream printStream = new PrintStream(outStream);

    ArrayList<String> stageNames = new ArrayList<String>();
    stageNames.addAll(stageCounts.keySet());
    Collections.sort(stageNames);

    Integer maxLength = 0;
    for (String name : stageNames) {
      maxLength = Math.max(maxLength, name.length());
    }
    String header = "Stage name";
    maxLength = Math.max(maxLength, header.length());

    printStream.print(header + ":");
    printStream.print(StringUtils.repeat(
        " ", maxLength - header.length() + 1));
    printStream.print("Number of times run");
    printStream.print("\n");

    for (String name : stageNames) {
      printStream.print(name + ":");
      printStream.print(StringUtils.repeat(
          " ", maxLength - name.length() + 1));
      printStream.print(String.format("%d", stageCounts.get(name)));
      printStream.print("\n");
    }

    try {
      printStream.close();
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("IOException", e);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AssemblyReport(), args);
    System.exit(res);
  }
}
