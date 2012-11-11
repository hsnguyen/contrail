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

  private final static String[] columnNames = {
    "src_id", "src_strand", "src_coverage", "src_length", "src_indegree",
    "src_outdegree",
    "dest_id", "dest_strand", "dest_coverage", "dest_length",
    "dest_indegree", "dest_outdegree",
    "edge_coverage"};

  /**
   * Return a string which can be used to specify the format when importing
   * the output into BigQuery.
   * @return
   */
  private static String bigQueryFormat() {
    HashMap<String, String> types = new HashMap<String, String>();
    for (String prefix : new String[]{"src_", "dest_"}) {
      types.put(prefix + "id", "string");
      types.put(prefix + "strand", "string");
      types.put(prefix + "coverage", "float");
      types.put(prefix + "length", "integer");
      types.put(prefix + "indegree", "integer");
      types.put(prefix + "outdegree", "integer");
    }
    types.put("edge_coverage", "integer");
    String[] values = new String[columnNames.length];
    for (int i = 0; i < columnNames.length; ++i) {
      values[i] = columnNames[i] + ":" + types.get(columnNames[i]);
    }
    return StringUtils.join(values, ",");
  }

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
    private String key(String neighborId, String nodeId) {
      if (neighborId.compareTo(nodeId) < 0) {
        return neighborId + ":" + nodeId;
      } else {
        return nodeId + ":" + neighborId;
      }
    }

    @Override
    public void map(GraphNodeData graphNodeData,
        AvroCollector<Pair<CharSequence, GraphNodeData>> collector,
        Reporter reporter) throws IOException {
      graphNode.setData(graphNodeData);

      // Iterate over the neighbors and send this node to each pair.
      for (String neighborId : graphNode.getNeighborIds()) {
        outPair.key(key(neighborId, graphNode.getNodeId()));
        outPair.value(graphNodeData);
        collector.collect(outPair);
      }
    }
  }

  private static class TOCSVReducer extends MapReduceBase
  implements Reducer<
  AvroWrapper<CharSequence>, AvroWrapper<GraphNodeData>,
  Text, NullWritable> {
    private GraphNode[] nodes;
    private String[] columns;

    // Map of column names to their position in the array.
    private HashMap<String, Integer> cols;

    private Text rowText;

    public void configure(JobConf job) {
      cols = new HashMap<String, Integer>();
      for (int i = 0; i < columnNames.length; ++i) {
        cols.put(columnNames[i], i);
      }
      columns = new String[columnNames.length];
      rowText = new Text();
      nodes = new GraphNode[2];
      nodes[0] = new GraphNode();
      nodes[1] = new GraphNode();
    }

    /**
     * Fill in the columns array with the information for the given node.
     * @param prefix
     * @param node
     */
    private void fillNodeColumns(
        String prefix, GraphNode node, DNAStrand strand) {
      columns[cols.get(prefix + "_id")] = node.getNodeId();
      columns[cols.get(prefix + "_coverage")] =
          Float.toString(node.getCoverage());
      columns[cols.get(prefix + "_strand")] = strand.toString();
      columns[cols.get(prefix + "_indegree")] = Integer.toString(
          node.degree(strand, EdgeDirection.INCOMING));
      columns[cols.get(prefix + "_outdegree")] = Integer.toString(
          node.degree(strand, EdgeDirection.OUTGOING));
      columns[cols.get(prefix + "_length")] = Integer.toString(
          node.getSequence().size());
    }

    /**
     * Output info about edges from source to dest if any exist.
     * @param source
     * @param dest
     * @param collector
     */
    private void outputEdge(
        GraphNode source, GraphNode dest,
        OutputCollector<Text, NullWritable> collector) throws IOException {
      for (DNAStrand srcStrand : DNAStrand.values()) {
        Set<EdgeTerminal> terminals = source.getEdgeTerminalsSet(
            srcStrand, EdgeDirection.OUTGOING);
        for (DNAStrand destStrand : DNAStrand.values()) {
          EdgeTerminal destTerminal =
              new EdgeTerminal(dest.getNodeId(), destStrand);
          if (!terminals.contains(destTerminal)) {
            continue;
          }
          fillNodeColumns("src", source, srcStrand);
          fillNodeColumns("dest", dest, destStrand);

          // Get the edge coverage for the edge.
          List<CharSequence> tags =
              source.getTagsForEdge(srcStrand, destTerminal);
          columns[cols.get("edge_coverage")] = Integer.toString(tags.size());

          rowText.set(StringUtils.join(columns, ","));
          collector.collect(rowText, NullWritable.get());
        }
      }
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

        if (numNodes >= 2) {
          throw new RuntimeException(
              String.format(
                  "Too many nodes for for key=%s.", key.datum()));
        }
        nodes[numNodes].setData(nodeData);
        nodes[numNodes] = nodes[numNodes].clone();
        numNodes++;
      }

      if (numNodes > 2) {
        throw new RuntimeException(
            String.format(
                "There were more than two nodes for key=%s. There were %s " +
                    " nodes.", key.datum(), numNodes));
      }

      if (numNodes == 1) {
        // Edge to self.
        outputEdge(nodes[0], nodes[0], collector);
      } else {
        outputEdge(nodes[0], nodes[1], collector);
        outputEdge(nodes[1], nodes[0], collector);
      }
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
    conf.setOutputFormat(TextOutputFormat.class);

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);
    Pair<CharSequence, GraphNodeData> mapPair =
        new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
    AvroJob.setMapOutputSchema(conf, mapPair.getSchema());
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    // We use a single reducer because it is convenient to have all the data
    // in one output file to facilitate uploading to helix.
    // TODO(jlewi): Once we have an easy way of uploading multiple files to
    // helix we should get rid of this constraint.
    conf.setNumReduceTasks(1);

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
        bigQueryFormat());
    return job;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new WriteEdgeGraphToCSV(), args);
    System.exit(res);
  }
}
