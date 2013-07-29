/*
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
package contrail.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.CompressibleNodeConverter;
import contrail.stages.CompressibleNodeData;
import contrail.stages.CompressibleStrands;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * Selects compressible nodes.
 *
 * Input is CompressibleNode records. The output are nodes which are
 * compressible on at least one strand.
 *
 * This is primarily for debugging problems with compression. Although
 * we could also use it to implement more efficient compression.
 */
public class SelectCompressibleNodes extends MRStage {
  private static final Logger sLogger = Logger.getLogger(
      SelectCompressibleNodes.class);

  public static class SelectMapper extends
      AvroMapper<CompressibleNodeData, CompressibleNodeData> {
    @Override
    public void map(
        CompressibleNodeData compressibleNode,
        AvroCollector<CompressibleNodeData> collector,
        Reporter reporter) throws IOException {

      if (compressibleNode.getCompressibleStrands() !=
          CompressibleStrands.NONE) {
        collector.collect(compressibleNode);
      }
    }
  }

  /**
   * Get the parameters used by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    CompressibleNodeData node = new CompressibleNodeData();
    AvroJob.setInputSchema(conf, node.getSchema());

    AvroJob.setMapOutputSchema(conf,  node.getSchema());
    AvroJob.setOutputSchema(conf, node.getSchema());

    AvroJob.setMapperClass(conf, SelectMapper.class);

    // This is a mapper only job.
    conf.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new SelectCompressibleNodes(), args);
    System.exit(res);
  }
}
