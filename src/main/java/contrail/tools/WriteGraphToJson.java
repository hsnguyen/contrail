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
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * Write graph to a JSON file.
 *
 * This MR job writes the data to a JSON file which can be imported to
 * BigQuery and other tools for analyzing the graph. The data isn't a direct
 * transcription of the graph node but rather a set of fields or each node
 * that are likely useful for analyzing the graph.
 */
public class WriteGraphToJson extends Stage {
  private static final Logger sLogger = Logger.getLogger(WriteGraphToJson.class);

  private static class ToJsonMapper extends MapReduceBase
    implements Mapper<AvroWrapper<GraphNodeData>, NullWritable,
                      Text, NullWritable> {

    private GraphNode graphNode;
    private Node jsonNode;
    private Text outKey;
    private ObjectMapper jsonMapper;
    
    public void configure(JobConf job) {
      graphNode = new GraphNode();
      jsonNode = new Node();
      outKey = new Text();
      jsonMapper = new ObjectMapper();
    }

    protected class Node {
      public String nodeId;
      public int length;
      public float coverage;
      public int outDegree;
      public int inDegree;
      public String sequence;
      public HashSet<CharSequence> threads;
           
      public Node() {
        threads = new HashSet<CharSequence>();
      }
      
      public void clear() {
        nodeId = "";
        outDegree = -1;
        inDegree = -1;
        length = -1;
        coverage = -1;
        sequence = "";
        threads.clear();
      }
    }
    
    /**
     * Mapper to do the conversion.
     */
    public void map(AvroWrapper<GraphNodeData> key, NullWritable bytes,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
            throws IOException {
      graphNode.setData(key.datum());
      jsonNode.clear();
      jsonNode.nodeId = graphNode.getNodeId();
      jsonNode.outDegree = graphNode.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING);
      jsonNode.inDegree = graphNode.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING);
      jsonNode.length = graphNode.getSequence().size();
      jsonNode.sequence = graphNode.getSequence().toString();
      jsonNode.coverage = graphNode.getCoverage();
      
      HashSet<String> uniqueThreads = new HashSet<String>();
      for (DNAStrand strand : DNAStrand.values()) {
        for (EdgeTerminal terminal : graphNode.getEdgeTerminals(strand, EdgeDirection.OUTGOING)) {
          jsonNode.threads.addAll(graphNode.getTagsForEdge(strand, terminal));
        }
      }
      outKey.set(jsonMapper.writeValueAsString(jsonNode));
      collector.collect(outKey, NullWritable.get());
   }
  }

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

  @Override
  public RunningJob runJob() throws Exception {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - inputpath: "  + inputPath);
    sLogger.info(" - outputpath: " + outputPath);

    JobConf conf = new JobConf(WriteGraphToJson.class);

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));


    AvroInputFormat<GraphNodeData> input_format =
        new AvroInputFormat<GraphNodeData>();
    conf.setInputFormat(input_format.getClass());
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
    // in one output file to facilitate uploading to helix.
    // TODO(jlewi): Once we have an easy way of uploading multiple files to
    // helix we should get rid of this constraint.
    conf.setNumReduceTasks(1);
    conf.setMapperClass(ToJsonMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      // TODO(jlewi): We should only delete an existing directory
      // if explicitly told to do so.
      sLogger.info("Deleting output path: " + out_path.toString() + " " +
          "because it already exists.");
      FileSystem.get(conf).delete(out_path, true);
    }


    long starttime = System.currentTimeMillis();
    RunningJob job = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();

    float diff = (float) ((endtime - starttime) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    sLogger.info(
        "You can use the following schema with big query:\n" +
        "nodeId:string, out_degree:integer, in_degree:integer, " +
        "length:integer, coverage:float, sequence:string");
    return job;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new WriteGraphToJson(), args);
    System.exit(res);
  }
}
