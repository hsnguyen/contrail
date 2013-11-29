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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.CounterInfo;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.stages.StageInfo;
import contrail.stages.StageInfoHelper;
import contrail.stages.StageState;
import contrail.util.FileHelper;

/**
 * Take a stage info file containing information about error correction
 * and print out a report of useful information.
 *
 */
public class CorrectionReport extends NonMRStage {
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

    // Stages that you want to print information for.
    ParameterDefinition stages = new ParameterDefinition(
        "stages", "A comma separated list of the stages we want to print the " +
        "information for.", String.class, ".*");
    defs.put(stages.getName(), stages);

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

  private StringBuffer JoinReadsInfo(StageInfo stage) {
    StringBuffer buffer = new StringBuffer();
    buffer.append(stage.getStageClass() + "\n");

    StageInfoHelper helper = new StageInfoHelper(stage);
    HashMap<String, Long> counters = helper.getCounters();
    for (String counterName :
      new String[] {"Map input records", "Reduce output records"}) {
      if (counters.containsKey(counterName)) {
        buffer.append(String.format(
            "%s:\t %d \n", counterName, counters.get(counterName)));
      }
    }
    return buffer;
  }

  /**
   * Write the values of the counters to the buffer.
   * @param names
   * @param stage
   * @param buffer
   */
  private void writeCounters(
      Collection<String> names, StageInfoHelper stage, StringBuffer buffer) {
    // Determine the length of the max string.
    int maxLength = 0;
    for (String name : names) {
      maxLength = Math.max(maxLength, name.length());
    }

    HashMap<String, Long> counters = stage.getCounters();

    for (String name : names) {
      if (counters.containsKey(name)) {
        buffer.append(name);
        buffer.append(":");
        int numSpaces = maxLength - name.length() + 2;
        buffer.append(StringUtils.repeat(" ", numSpaces));
        buffer.append(String.format("%d", counters.get(name)));
        buffer.append("\n");
      }
    }
  }

  private StringBuffer InvokeFlashInfo(StageInfo stage) {
    StringBuffer buffer = new StringBuffer();
    buffer.append(stage.getStageClass() + "\n");

    StageInfoHelper helper = new StageInfoHelper(stage);

    // Compute the number of joined and single reads.
    Long inputPairs = helper.getCounters().get("Map input records");
    Long outputReads = helper.getCounters().get("Map output records");
    Long unjoinedReads = 2 * (outputReads - inputPairs);
    Long joinedReads = outputReads - unjoinedReads;

    CounterInfo joinedCounter = new CounterInfo();
    joinedCounter.setName("Number of joined reads");
    joinedCounter.setValue(joinedReads);

    CounterInfo unjoinedCounter = new CounterInfo();
    unjoinedCounter.setName("Number of unjoined reads");
    unjoinedCounter.setValue(unjoinedReads);

    helper = new StageInfoHelper(stage);

    stage.getCounters().add(joinedCounter);
    stage.getCounters().add(unjoinedCounter);

    writeCounters(
        Arrays.asList(
            "Map input records", "Map output records", "Reduce output records",
            joinedCounter.getName().toString(),
            unjoinedCounter.getName().toString()),
        helper, buffer);
    return buffer;
  }

  private StringBuffer InvokeQuakeInfo(StageInfo stage) {
    StageInfoHelper helper = new StageInfoHelper(stage);
    StringBuffer buffer = new StringBuffer();
    buffer.append(stage.getStageClass() + "\n");
    List<String> counters = Arrays.asList(
        "Map input records", "input-reads",
        "Reduce output records", "quake-reads-trimmed",
        "quake-reads-validated", "quake-reads-trimmed only",
        "quake-reads-removed", "quake-reads-corrected");
    writeCounters(counters, helper, buffer);

    return buffer;
  }

  private StringBuffer processStage(StageInfo stage) {
    String name = stage.getStageClass().toString();
    if (name.equals(JoinReads.class.getName())) {
      return JoinReadsInfo(stage);
    } else if(name.equals(InvokeFlash.class.getName()))  {
      return InvokeFlashInfo(stage);
    } else if (name.equals(InvokeQuake.class.getName())) {
      return InvokeQuakeInfo(stage);
    } else {
      StringBuffer buffer = new StringBuffer();
      buffer.append("No handler for stage: " + name);
      return buffer;
    }
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

    String stageValue = (String) stage_options.get("stages");
    String stages[] = stageValue.split(",");

    HashMap<String, StringBuffer> stageMatches =
        new HashMap<String, StringBuffer>();

    HashSet<String> unmatched = new HashSet<String>();
    unmatched.addAll(Arrays.asList(stages));

    for (Path stageFile : stageFiles) {
      if (unmatched.isEmpty()) {
        break;
      }

      StageInfoHelper infoHelper = StageInfoHelper.loadFromPath(
          getConf(), stageFile);

      for (StageInfo stageInfo : infoHelper.DFSSearch()) {
        String stageClass = stageInfo.getStageClass().toString();
        ArrayList<String> toRemove = new  ArrayList<String>();
        for (String matchExpression : unmatched) {
          if (stageClass.matches(matchExpression) &&
              stageInfo.getState() == StageState.SUCCESS) {
            StringBuffer buffer = processStage(stageInfo);
            stageMatches.put(matchExpression, buffer);
            toRemove.add(matchExpression);
          }
        }
        unmatched.removeAll(toRemove);
      }
    }

    OutputStream outStream = createOutputStream();
    PrintStream printStream = new PrintStream(outStream);

    for (String stage : stages) {
      if (!stageMatches.containsKey(stage)) {
        printStream.println("No entries with state Success matched: " + stage);
        continue;
      }
      printStream.print(stageMatches.get(stage).toString());
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
        new Configuration(), new CorrectionReport(), args);
    System.exit(res);
  }
}
