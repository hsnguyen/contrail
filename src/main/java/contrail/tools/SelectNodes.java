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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
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
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.ContrailLogger;

/**
 * An MR job which filters out all but a small subset of the input records.
 *
 * The input records can either be individual nodes or arrays of nodes.
 * If its an array of nodes then the entire array is outputted if it contains
 * one of the nodes of interest.
 */
public class SelectNodes extends MRStage {
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      SelectNodes.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
      HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition nodes = new ParameterDefinition(
        "nodes", "Comma separated list of nodes of interest.",
        String.class, null);
    defs.put(nodes.getName(), nodes);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * The mapper filters out all but the nodes of interest.
   */
  public static class Mapper extends AvroMapper<Object, Object> {
    HashMap<String, GraphNode> nodes;

    HashSet<String> targets;
    HashSet<String> nodeIds;
    @Override
    public void configure(JobConf job)    {
      SelectNodes stage = new SelectNodes();
      ParameterDefinition parameter =
          stage.getParameterDefinitions().get("nodes");
      String value = (String)parameter.parseJobConf(job);
      String[] pieces = value.split(",");
      targets = new HashSet<String>();
      for (String item : pieces) {
        targets.add(item);
      }
      nodeIds = new HashSet<String>();
    }

    @Override
    public void map(
        Object record, AvroCollector<Object> collector,
        Reporter reporter) throws IOException   {
      nodeIds.clear();

      if (record instanceof GraphNodeData) {
        GraphNodeData data = (GraphNodeData) record;
        nodeIds.add(data.getNodeId().toString());
      } else if (record instanceof List<?>) {
        for (Object item : (List<Object>) record) {
          if (item instanceof GraphNodeData) {
            GraphNodeData data = (GraphNodeData) item;
            nodeIds.add(data.getNodeId().toString());
          } else {
            sLogger.fatal(
                "Item isn't valid type:" + item.getClass().getCanonicalName());
          }
        }
      } else {
        sLogger.fatal(
            "Record isn't valid type:" + record.getClass().getCanonicalName());
      }

      nodeIds.retainAll(targets);
      if (nodeIds.size() > 0) {
        collector.collect(record);
      }
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(new GraphNodeData().getSchema());
    schemas.add(Schema.createArray(new GraphNodeData().getSchema()));
    Schema unionSchema = Schema.createUnion(schemas);
    AvroJob.setInputSchema(conf, unionSchema);

    AvroJob.setMapOutputSchema(conf, unionSchema);
    AvroJob.setOutputSchema(conf, unionSchema);

    AvroJob.setMapperClass(conf, Mapper.class);
    // This is a mapper only job.
    conf.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SelectNodes(), args);
    System.exit(res);
  }
}
