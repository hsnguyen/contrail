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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.BigQueryField;

/**
 * Write bubbles to a json file which can then be imported into BigQuery.
 *
 */
public class WriteBubblesToJson extends MRStage {
  private static final Logger sLogger = Logger.getLogger(WriteBubblesToJson.class);

  /**
   * Get the options required by this stage.
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

  private static class WriteBubblesMapper extends
      AvroMapper<GraphNodeData, Pair<CharSequence, GraphNodeData>> {

    private GraphNode node;
    private Pair<CharSequence, GraphNodeData> outPair;

    public void configure(JobConf job) {
      node = new GraphNode();
      outPair = new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
    }

    public void map(
        GraphNodeData nodeData,
        AvroCollector<Pair<CharSequence, GraphNodeData>> collector,
        Reporter reporter) {
      node.setData(nodeData);

      // Check if this node could be a bubble i.e it has indegree=outdegree=1.
      if (node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING) != 1 ||
          node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING) != 1) {
        return;
      }

      // The key is the neighbor ids in sorted order. This ensures all nodes
      // connecting those two nodes get the same key.
      String key = StringUtils.join(node.getNeighborIds().toArray(), ":");
      outPair.key(key);
      outPair.value(nodeData);

      collector.collect(outPair);
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroInputFormat<GraphNodeData> input_format =
        new AvroInputFormat<GraphNodeData>();
    conf.setInputFormat(input_format.getClass());

    // The output is a text file.
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    // We need to set the comparator because AvroJob.setInputSchema will
    // set it automatically to a comparator for an Avro class which we don't
    // want. We could also change the code to use an AvroMapper.
    conf.setOutputKeyComparatorClass(Text.Comparator.class);
    // We use a single reducer because it is convenient to have all the data
    // in one output file to facilitate uploading to bigquery.
    // TODO(jlewi): Once we have an easy way of uploading multiple files to
    // big query we should get rid of this constraint.
    conf.setNumReduceTasks(1);
    conf.setMapperClass(WriteEdgesMapper.class);
    conf.setReducerClass(IdentityReducer.class);
  }

  protected void postRunHook() {
    // Print out the json schema.
    ArrayList<String> fields = new ArrayList<String>();

    BigQueryField source = new BigQueryField();
    source.name = "source";
    source.type = "record";
    source.fields.add(new BigQueryField("nodeId", "string"));
    source.fields.add(new BigQueryField("strand", "string"));

    BigQueryField dest = new BigQueryField();
    dest.name = "dest";
    dest.type = "record";
    dest.fields.add(new BigQueryField("nodeId", "string"));
    dest.fields.add(new BigQueryField("strand", "string"));


    BigQueryField tags = new BigQueryField();
    tags.name = "tags";
    tags.type = "string";
    tags.mode = "repeated";

    fields.add(source.toString());
    fields.add(dest.toString());
    fields.add(tags.toString());

    String schema = "[" + StringUtils.join(fields, ",") + "]";
    sLogger.info("Schema:\n" + schema);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new WriteBubblesToJson(), args);
    System.exit(res);
  }
}
