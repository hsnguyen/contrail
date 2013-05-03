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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
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
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.BigQueryField;
import contrail.util.BigQuerySchema;
import contrail.util.ContrailLogger;

/**
 * Write bubbles to a json file which can then be imported into BigQuery.
 *
 */
public class WriteBubblesToJson extends MRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(WriteBubblesToJson.class);

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
        Reporter reporter) throws IOException {
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

  /**
   * Information about a single node.
   */
  protected static class NodeInfo {
    public String nodeId;
    public DNAStrand strand;
    public int length;
    public float coverage;

    /**
     * Returns a schema describing this record.
     * @return
     */
    public static BigQuerySchema bigQuerySchema() {
      BigQuerySchema schema = new BigQuerySchema();

      schema.add(new BigQueryField("nodeId", "string"));
      schema.add(new BigQueryField("strand", "string"));
      schema.add(new BigQueryField("length", "integer"));
      schema.add(new BigQueryField("coverage", "float"));

      return schema;
    }
  }

  /**
   * Compare two nodes forming a bubble.
   */
  protected static class PairInfo {
    public NodeInfo major;
    public NodeInfo minor;
    public int editDistance;
    public float editRate;

    /**
     * Returns a schema describing this record.
     * @return
     */
    public static BigQuerySchema bigQuerySchema() {
      BigQuerySchema schema = new BigQuerySchema();

      BigQueryField major = new BigQueryField("major", "record");
      major.fields.addAll(NodeInfo.bigQuerySchema());
      schema.add(major);

      BigQueryField minor = new BigQueryField("minor", "record");
      minor.fields.addAll(NodeInfo.bigQuerySchema());
      schema.add(minor);

      schema.add(new BigQueryField("editDistance", "integer"));
      schema.add(new BigQueryField("editRate", "float"));

      return schema;
    }
  }

  /**
   * Represent a path from the major to minor node.
   */
  protected static class PathInfo {
    public DNAStrand majorStrand;
    public DNAStrand minorStrand;
    public ArrayList<PairInfo> pairs;

    public PathInfo() {
      pairs = new ArrayList<PairInfo>();
    }

    /**
     * Returns a schema describing this record.
     * @return
     */
    public static BigQuerySchema bigQuerySchema() {
      BigQuerySchema schema = new BigQuerySchema();
      schema.add(new BigQueryField("majorStrand", "string"));
      schema.add(new BigQueryField("minorStrand", "string"));

      BigQueryField pairs = new BigQueryField("pairs", "record");
      pairs.mode = "repeated";
      pairs.fields.addAll(PairInfo.bigQuerySchema());

      schema.add(pairs);
      return schema;
    }
  }

  /**
   * This class represents the output for a pair of nodes.
   */
  protected static class BubbleInfo {
    // The id's for the two nodes we are considering.
    public String majorId;
    public String minorId;
    public ArrayList<PathInfo> paths;

    public BubbleInfo() {
      paths = new ArrayList<PathInfo>();
    }

    /**
     * Returns a schema describing this record.
     * @return
     */
    public static BigQuerySchema bigQuerySchema() {
      BigQuerySchema schema = new BigQuerySchema();
      schema.add(new BigQueryField("majorId", "string"));
      schema.add(new BigQueryField("minorId", "string"));

      BigQueryField pairs = new BigQueryField("paths", "record");
      pairs.mode = "repeated";
      pairs.fields.addAll(PathInfo.bigQuerySchema());
      schema.add(pairs);
     return schema;
    }
  }

  
  protected static class WriteBubblesReducer extends
      MapReduceBase implements
          Reducer<AvroKey<CharSequence>, AvroValue<GraphNodeData>,
                  Text, NullWritable> {
    private GraphNode node;
    private ObjectMapper jsonMapper;
    private Text outKey;

    /**
     * A comparator for sorting terminals by node id.
     */
    public static class TerminalNodeIdComparator
        implements Comparator<EdgeTerminal> {
      @Override
      public int compare(EdgeTerminal o1, EdgeTerminal o2) {
        return o1.nodeId.compareTo(o2.nodeId);
      }
    }

    public void configure(JobConf job) {
      node = new GraphNode();
      jsonMapper = new ObjectMapper();
      outKey = new Text();
    }

    protected class Alignment {
  	  public DNAStrand major;
  	  public DNAStrand middle;
  	  public DNAStrand minor;
    }
    
    /**
     * Find the strands for the major, middle and minor nodes such that their is a
     * path from major->middle->minor
     * 
     * @param node
     * @param majorId
     * @param minorId
     * @return
     */
    protected Alignment alignMiddle(GraphNode middle, String majorId, String minorId) {
    	Set<StrandsForEdge> majorSet = middle.findStrandsForEdge(
    			majorId, EdgeDirection.INCOMING);

    	if (majorSet.size() != 1) {
    		sLogger.fatal(String.format(
    			"Node: %s couldn't be aligned. No incoming edge from majorID: %s", middle.getNodeId(), 
    			majorId),
    			new RuntimeException("Unable to align node."));
    	}

    	Set<StrandsForEdge> minorSet = middle.findStrandsForEdge(
    			minorId, EdgeDirection.OUTGOING);
    	if (minorSet.size() != 1) {
    		sLogger.fatal(String.format(
    			"Node: %s couldn't be aligned. No outgoing edge to minorId: %s.", 
    			middle.getNodeId(), minorId),
    			new RuntimeException("Unable to align node."));
    	}
    	
    	StrandsForEdge majorStrands = majorSet.iterator().next();
    	StrandsForEdge minorStrands = minorSet.iterator().next();
    	if (StrandsUtil.dest(majorStrands) != StrandsUtil.src(minorStrands)) {
    		sLogger.fatal(String.format(
    			"Node: %s couldn't be aligned. Strands don't match: %s %s", middle.getNodeId(),
    			StrandsUtil.dest(majorStrands), StrandsUtil.src(minorStrands)),
    			new RuntimeException("Unable to align node."));
    	}
    	
    	Alignment alignment = new Alignment();
    	alignment.major= StrandsUtil.src(majorStrands);
    	alignment.middle = StrandsUtil.dest(majorStrands);
    	alignment.minor= StrandsUtil.dest(minorStrands);
    	return alignment;
    }
    
    @Override
    public void reduce(
        AvroKey<CharSequence> key,
        Iterator<AvroValue<GraphNodeData>> values,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
        throws IOException {

      HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
      while (values.hasNext()) {
        node.setData(values.next().datum());
        nodes.put(node.getNodeId(), node.clone());
      }

      // Find the major and minor nodes for these paths.
      BubbleInfo bubble = new BubbleInfo();

      // Get the neighbor nodes in sorted order.
      List<String> neighbors = new ArrayList<String>();
      neighbors.addAll(nodes.values().iterator().next().getNeighborIds());
      Collections.sort(neighbors);

      bubble.minorId = neighbors.get(0);
      bubble.majorId = neighbors.get(1);

      // We align the nodes by finding the path major -> middle -> minor.
      // We group the nodes based on the strands for the major and minor
      // node and then the terminal for the middle node.
      // The key are the strands for the major and minor node. The values
      // are middle terminals for that path.
      HashMap<StrandsForEdge, ArrayList<EdgeTerminal>> groups =
          new HashMap<StrandsForEdge, ArrayList<EdgeTerminal>>();

      for (GraphNode middle : nodes.values()) {        
    	Alignment alignment = alignMiddle(middle, bubble.majorId, bubble.minorId);
    	
        StrandsForEdge strandsKey = StrandsUtil.form(alignment.major, alignment.minor);

        if (!groups.containsKey(strandsKey)) {
          groups.put(strandsKey, new ArrayList<EdgeTerminal>());
        }

        groups.get(strandsKey).add(new EdgeTerminal(middle.getNodeId(), alignment.middle));
      }

      // Iterate over each set of nodes forming a bubble and compare the
      // paths.
      for (StrandsForEdge keyStrands : groups.keySet()) {
        ArrayList<EdgeTerminal> group = groups.get(keyStrands);
        if (group.size() == 1) {
          continue;
        }

        // Sort the group by alphabetical order in descending order by
        // nodeId. This way when iterating over pairs, the node which comes
        // first is always the major node.
        Collections.sort(group, new TerminalNodeIdComparator());
        Collections.reverse(group);

        PathInfo pathInfo = new PathInfo();
        pathInfo.majorStrand = StrandsUtil.src(keyStrands);
        pathInfo.minorStrand = StrandsUtil.dest(keyStrands);

        bubble.paths.add(pathInfo);
        // Compare all pairs.
        for (int i = 0;  i < group.size(); ++i) {
          EdgeTerminal major = group.get(i);
          Sequence majorSequence = nodes.get(major.nodeId).getSequence();

          NodeInfo majorInfo = new NodeInfo();
          majorInfo.nodeId = major.nodeId;
          majorInfo.strand = major.strand;
          majorInfo.length = majorSequence.size();
          majorInfo.coverage = nodes.get(major.nodeId).getCoverage();

          for (int j = i + 1;  j < group.size(); ++j) {
            EdgeTerminal minor = group.get(j);
            Sequence minorSequence = nodes.get(minor.nodeId).getSequence();

            NodeInfo minorInfo = new NodeInfo();
            minorInfo.nodeId = minor.nodeId;
            minorInfo.strand = minor.strand;
            minorInfo.length = minorSequence.size();
            minorInfo.coverage = nodes.get(minor.nodeId).getCoverage();

            PairInfo pairInfo = new PairInfo();
            pairInfo.major = majorInfo;
            pairInfo.minor = minorInfo;
            pairInfo.editDistance = majorSequence.computeEditDistance(
                minorSequence);
            // Edit rate is the editDistance divided by the average length.
            pairInfo.editRate =
                2.0f * pairInfo.editDistance /
                (majorInfo.length + minorInfo.length);

           pathInfo.pairs.add(pairInfo);
          }
        }
      }
      outKey.set(jsonMapper.writeValueAsString(bubble));
      collector.collect(outKey, NullWritable.get());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);
    Pair<CharSequence, GraphNodeData> pair =
        new Pair<CharSequence, GraphNodeData>("",  new GraphNodeData());
    AvroJob.setMapOutputSchema(conf, pair.getSchema());
    AvroJob.setMapperClass(conf, WriteBubblesToJson.WriteBubblesMapper.class);
    conf.setReducerClass(WriteBubblesToJson.WriteBubblesReducer.class);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroInputFormat<GraphNodeData> input_format =
        new AvroInputFormat<GraphNodeData>();
    conf.setInputFormat(input_format.getClass());

    // The output is a text file.
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);
  }

  protected void postRunHook() {
    sLogger.info("Schema:\n" + BubbleInfo.bigQuerySchema().toString());
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new WriteBubblesToJson(), args);
    System.exit(res);
  }
}
