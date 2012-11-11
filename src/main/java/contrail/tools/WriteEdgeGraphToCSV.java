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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
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
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * Write graph to a CSV file.
 *
 * This MR job writes the data to a CSV file which can be imported to
 * BigQuery and other tools for analyzing the graph.
 *
 * The data isn't a direct transcription of the graph node but rather a set of
 * fields describing each edge that are likely useful for analyzing the graph.
 *
 * TODO(jlewi): This job is incredibly inefficient. It copies much more
 * data then necessary.
 * 1. The mapper outputs complete copies of the graphnode. It
 * would be much more efficient to strip out the information we don't need.
 * 2. The reducer has a unique key for each edge i.e (source id, destination id)
 * pair. It would probably be better to group by source id so that we reduce
 * the number of copies of GraphNodeData for a given source id.
 */
public class WriteEdgeGraphToCSV extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      WriteEdgeGraphToCSV.class);

  private static class ToCSVMapper extends
      AvroMapper<GraphNodeData, Pair<CharSequence, GraphNodeData>> {

    private GraphNode graphNode;
    private Pair<CharSequence, GraphNodeData> outPair;

    public void configure(JobConf job) {
      graphNode = new GraphNode();
      outPair = new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
    }

    /**
     * Function generates the key for the reducer.
     * @param src
     * @param dest
     * @return
     */
    private String key(EdgeTerminal src, EdgeTerminal dest) {
      return src.toString() + ":" + dest.toString();
    }

    @Override
    public void map(GraphNodeData graphNodeData,
        AvroCollector<Pair<CharSequence, GraphNodeData>> collector,
        Reporter reporter) throws IOException {
      graphNode.setData(graphNodeData);

      // Iterate over the incoming edges and send a copy of this node
      // to all the src nodes.
      for (DNAStrand strand: DNAStrand.values()) {
        EdgeTerminal dest = new EdgeTerminal(
            graphNode.getNodeId(), strand);
        for (EdgeTerminal terminal :
          graphNode.getEdgeTerminals(dest.strand, EdgeDirection.INCOMING)) {

          if (terminal.strand != DNAStrand.FORWARD) {
            // Only consider the forward strand of the reverse node.
            // TODO(jlewi): Is this really what we want to do?
            continue;
          }
          outPair.key(key(terminal, dest));
          outPair.value(graphNodeData);
          collector.collect(outPair);
        }
      }


      // Iterate over the outgoing edges and send a copy of this node.
      EdgeTerminal source = new EdgeTerminal(
          graphNode.getNodeId(), DNAStrand.FORWARD);
      for (EdgeTerminal terminal :
        graphNode.getEdgeTerminals(DNAStrand.FORWARD, EdgeDirection.INCOMING)) {
        outPair.key(key(source, terminal));
        outPair.value(graphNodeData);
        collector.collect(outPair);
      }

//      columns[0] = graphNode.getNodeId();
//      columns[1] = Integer.toString(graphNode.degree(
//          DNAStrand.FORWARD, EdgeDirection.OUTGOING));
//      columns[2] = Integer.toString(graphNode.degree(
//          DNAStrand.FORWARD, EdgeDirection.INCOMING));
//      columns[3] = Integer.toString(graphNode.getSequence().size());
//      columns[4] = Float.toString(graphNode.getCoverage());
//
//      outKey.set(StringUtils.join(columns, ","));
//      output.collect(outKey, NullWritable.get());
   }
  }

  private static class TOCSVReducer extends MapReduceBase
      implements Reducer<
      AvroWrapper<CharSequence>, AvroWrapper<GraphNodeData>,
      Text, NullWritable> {

   private GraphNode source;
   private GraphNode dest;
   private String[] columns;

   // Map of column names to their position in the array.
   private HashMap<String, Integer> cols;

   private Text rowText;

   public void configure(JobConf job) {
     source = new GraphNode();
     dest = new GraphNode();

     String[] columnNames = {
         "src_id", "src_coverage", "src_length", "src_indegree",
         "src_outdegree",
         "dest_id", "dest_coverage", "dest_length", "dest_indegree",
         "dest_outdegree",
         "edge_coverage"};

     cols = new HashMap<String, Integer>();
     for (int i = 0; i < columnNames.length; ++i) {
       cols.put(columnNames[i], i);
     }
     columns = new String[columnNames.length];
     rowText = new Text();
   }

   @Override
   public void reduce(
       AvroWrapper<CharSequence> key,
       Iterator<AvroWrapper<GraphNodeData>> values,
       OutputCollector<Text, NullWritable> collector,
       Reporter reporter) throws IOException {

     // Split
     int numNodes = 0;
     while (values.hasNext()) {
       GraphNodeData nodeData = values.next().datum();
       if (numNodes == 0) {
         source.setData(nodeData);
         source = source.clone();
       } else {
         dest.setData(nodeData);
         dest = dest.clone();
       }
       ++numNodes;
     }
     if (numNodes != 2) {
       throw new RuntimeException(
           String.format(
               "There weren't two nodes for key=%s. Rather there were %s " +
               " nodes.", key.datum(), numNodes));
     }

     // Decide which node really is the source.
     Set<DNAStrand> strands = dest.findStrandsWithEdgeToTerminal(
         new EdgeTerminal(source.getNodeId(), DNAStrand.FORWARD),
         EdgeDirection.INCOMING);

     if (strands.size() == 0) {
       // Reverse the nodes.
       GraphNode temp = source;
       source = dest;
       dest = temp;
     }

     // Get the dest strand.
     strands = dest.findStrandsWithEdgeToTerminal(
         new EdgeTerminal(source.getNodeId(), DNAStrand.FORWARD),
         EdgeDirection.INCOMING);

     if (strands.size() != 1) {
       throw new RuntimeException(String.format(
           "The destination node has %d strands with an incoming edge from " +
           "the source node. But there should only be 1.", strands.size()));
     }

     EdgeTerminal destTerminal = new EdgeTerminal(
         dest.getNodeId(), strands.iterator().next());

     for (String prefix : new String[] {"src", "dest"}) {
       GraphNode node;
       DNAStrand strand;
       if (prefix.equals("src")) {
         node = source;
         strand = DNAStrand.FORWARD;
       } else {
         node = dest;
         strand = destTerminal.strand;
       }
       columns[cols.get(prefix + "_id")] = node.getNodeId();
       columns[cols.get(prefix + "_coverage")] = Float.toString(node.getCoverage());
       columns[cols.get(prefix + "_indegree")] = Integer.toString(
           node.degree(strand, EdgeDirection.INCOMING));
       columns[cols.get(prefix + "_outdegree")] = Integer.toString(
           node.degree(strand, EdgeDirection.OUTGOING));
       columns[cols.get(prefix + "_length")] = Integer.toString(
           node.getSequence().size());
     }

     // Get the edge coverage for the edge.
     List<CharSequence> tags =
         source.getTagsForEdge(DNAStrand.FORWARD, destTerminal);
     columns[cols.get("edge_coverage")] = Integer.toString(tags.size());

     rowText.set(StringUtils.join(columns, ","));
     collector.collect(rowText, NullWritable.get());
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

    JobConf conf = new JobConf(WriteEdgeGraphToCSV.class);


    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));


    AvroJob.setMapperClass(conf, ToCSVMapper.class);

    conf.setReducerClass(TOCSVReducer.class);

    //conf.setInputFormat(input_format.getClass());

    conf.setOutputFormat(TextOutputFormat.class);

    AvroWrapper<CharSequence> keyWrapper = new AvroWrapper<CharSequence>();
    AvroWrapper<GraphNodeData> valueWrapper = new AvroWrapper<GraphNodeData>(
        new GraphNodeData());

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);
    Pair<CharSequence, GraphNodeData> mapPair =
        new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
    AvroJob.setMapOutputSchema(conf, mapPair.getSchema());
    //conf.setMapOutputKeyClass(keyWrapper.getClass());
    //conf.setMapOutputValueClass(valueWrapper.getClass());
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    // We use a single reducer because it is convenient to have all the data
    // in one output file to facilitate uploading to helix.
    // TODO(jlewi): Once we have an easy way of uploading multiple files to
    // helix we should get rid of this constraint.
//    Schema.Type.NULL;
    //Pair<CharSequence, Null> pair = new Pair<CharSequence, Null>
    //AvroJob.setMapOutputSchema(conf, pair);
    // This is a mapper only job. To facilitate upload to BigQuery you
    // should probably join the part files together.
    conf.setNumReduceTasks(1);
    //conf.setReducerClass(IdentityReducer.class);

    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      // TODO(jlewi): We should only delete an existing directory
      // if explicitly told to do so.
      sLogger.info("Deleting output path: " + out_path.toString() + " " +
          "because it already exists.");
      FileSystem.get(conf).delete(out_path, true);
    }

    sLogger.info(
        "You can use the following schema with big query:\n" +
         "nodeId:string, out_degree:integer, in_degree:integer, " +
         "length:integer, coverage:float");

    long starttime = System.currentTimeMillis();
    RunningJob job = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();

    float diff = (float) ((endtime - starttime) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return job;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new WriteEdgeGraphToCSV(), args);
    System.exit(res);
  }
}
