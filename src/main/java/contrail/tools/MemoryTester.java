/* Licensed under the Apache License, Version 2.0 (the "License");
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
package contrail.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.ContrailLogger;

/**
 * A simple program to make it easy to test that various command line
 * options actually change the amount of memory given to tasks.
 *
 * Input can be any non-empty text input. The input doesn't matter as long
 * as it generates records so the mapper is triggered.
 */
public class MemoryTester extends MRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(MemoryTester.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
       defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  // We need a reducer to force the data to a single file.
  public static class MemoryMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, NullWritable> {
    private Text outKey;
    Runtime runtime;

    @Override
    public void configure(JobConf conf) {
      runtime = Runtime.getRuntime();
      outKey = new Text();
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
        throws IOException {
      outKey.set(
          String.format(
              "Mapper Max Memory:%d ", runtime.maxMemory()));
      collector.collect(outKey, NullWritable.get());
    }
  }

  @Override
  protected void setupConfHook() {
    Runtime runtime = Runtime.getRuntime();
    sLogger.info(String.format(
        "Main class setupConfHook Max Memory:%d ", runtime.maxMemory()));

    JobConf conf = (JobConf) getConf();
    conf.setJobName("MemoryTester");
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    conf.setMapperClass(MemoryMapper.class);

    // Reducer only.
    conf.setNumReduceTasks(0);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MemoryTester(), args);
    System.exit(res);
  }
}
