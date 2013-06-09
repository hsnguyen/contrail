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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.tools.LookAtThreads;

/**
 * A simple MR job to find those nodes which have reads which span incoming
 * and outgoing edges.
 */
public class FindNodesWithSpanningReads  extends MRStage {
  private static final Logger sLogger = Logger.getLogger(
      FindNodesWithSpanningReads.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def : ContrailParameters
        .getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }


  public static class FindMapper extends
    AvroMapper<GraphNodeData, Pair<CharSequence, Integer>> {
    private GraphNode node;
    private LookAtThreads looker;
    private Pair<CharSequence, Integer> outPair;
    public void configure(JobConf job)    {
      node = new GraphNode();
      looker = new LookAtThreads();
      outPair = new Pair<CharSequence, Integer>("", 0);
    }

    public void map(
        GraphNodeData graphData,
        AvroCollector<Pair<CharSequence, Integer>> collector,
        Reporter reporter) throws IOException {
      node.setData(graphData);

      Set<String> reads = looker.findSpanningReads(node);

      if (reads.size() <= 0) {
        return;
      }

      outPair.key(node.getNodeId());
      outPair.value(reads.size());
      collector.collect(outPair);
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    Pair<CharSequence, Integer> outPair =
        new Pair<CharSequence, Integer>("", 0);

    GraphNodeData node = new GraphNodeData();
    AvroJob.setInputSchema(conf, node.getSchema());
    AvroJob.setMapOutputSchema(conf, outPair.getSchema());
    AvroJob.setOutputSchema(conf, outPair.getSchema());

    AvroJob.setMapperClass(conf, FindMapper.class);

    // This is a mapper only job.
    conf.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception   {
    int res = ToolRunner.run(
        new Configuration(), new FindNodesWithSpanningReads(), args);
    System.exit(res);
  }
}
