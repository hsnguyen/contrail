package contrail.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
//import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

/**
 * This class is used to walk a graph starting at some seed nodes
 * and walking outwards. All nodes visited are then outputted.
 * The input files must be SortedKeyValueFile's so that we can efficiently
 * lookup nodes in the files.
 */
public class WalkGraph extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(WalkGraph.class);
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition startNodes = new ParameterDefinition(
        "start_nodes", "Comma separated list of the nodes to start the walk " +
        "from", String.class, null);

    defs.put(startNodes.getName(), startNodes);

    ParameterDefinition numHops = new ParameterDefinition(
        "num_hops", "Number of hops to take starting at start_nodes.",
        Integer.class, null);

    defs.put(numHops.getName(), numHops);

    return Collections.unmodifiableMap(defs);
  }

  private SortedKeyValueFile.Reader<CharSequence, GraphNodeData> createReader()
      {
    SortedKeyValueFile.Reader.Options readerOptions =
        new SortedKeyValueFile.Reader.Options();

    String inputPath = (String) stage_options.get("inputpath");
    readerOptions.withPath(new Path(inputPath));

    GraphNodeData nodeData = new GraphNodeData();
    readerOptions.withConfiguration(getConf());
    readerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    readerOptions.withValueSchema(nodeData.getSchema());

    SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader = null;
    try {
//      SortedKeyValueFile.Reader<CharSequence, CharSequence> readerTest = null;
//      readerTest = new SortedKeyValueFile.Reader<CharSequence,CharSequence> (
//          testOptions)

      reader = new SortedKeyValueFile.Reader<CharSequence,GraphNodeData> (
          readerOptions);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return reader;
  }

  /**
   * Walk the graph from the start node.
   * @param startId
   * @param numHops
   * @param writer
   * @param exclude: List of nodes already outputted so we exclude them.
   * @return: List of all nodes visited.
   */
  private HashSet<String> walk(
      SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader,
      String startId, int numHops,
      DataFileWriter<GraphNodeData> writer, HashSet<String> exclude) {
    HashSet<String> visited = new HashSet<String>();
    visited.addAll(exclude);

    // Use two lists so we can keep track of the hops.
    HashSet<String> thisHop = new HashSet<String>();
    HashSet<String> nextHop = new HashSet<String>();

    int hop = 0;
    thisHop.add(startId);
    GraphNodeData nodeData = null;
    GraphNode node = new GraphNode();
    while (hop <= numHops && thisHop.size() > 0) {
      // Fetch each node in thisHop.
      for (String nodeId : thisHop) {
        if (!exclude.contains(nodeId)) {
          try{
            Object value = reader.get(nodeId);
            nodeData = (GraphNodeData) value;
          } catch (IOException e) {
            sLogger.fatal("There was a problem reading from the file.", e);
            System.exit(-1);
          }
          if (nodeData == null) {
            sLogger.fatal(
                "Could not find node:" + nodeId,
                new RuntimeException("Couldn't find node"));
          }
          try{
            writer.append(nodeData);
          } catch (IOException e) {
            sLogger.fatal("There was a problem writing the node", e);
            System.exit(-1);
          }
          exclude.add(nodeId);
        }
        // Even if the nodeId is in the exclude set we still want to
        // process its edges because its possible we have multiple seeds
        // and the walk overlaps.
        node.setData(nodeData);
        nextHop.addAll(node.getNeighborIds());
      }
      thisHop.clear();
      thisHop.addAll(nextHop);
      nextHop.clear();
      ++hop;
    }
    return visited;
  }

  /**
   * Find the subgraph by starting at the indicated node and walking the
   * specified number of hops.
   */
  private void writeSubGraph() {
    String outputPath = (String) stage_options.get("outputpath");
    String startNodes = (String) stage_options.get("start_nodes");
    int numHops = (Integer) stage_options.get("num_hops");

    String[] nodeids = startNodes.split(",");

    SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader =
        createReader();

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }

    GraphNodeData node = new GraphNodeData();

    // TODO(jeremy@lewi.us): Output path must exist.
    try {
      if (!fs.exists(new Path(outputPath))) {
        sLogger.info("Creating output path:" + outputPath);
        fs.mkdirs(new Path(outputPath));
      }
    } catch (IOException e) {
      sLogger.fatal("Could not create the outputpath:" + outputPath, e);
      System.exit(-1);
    }

    String outputFile = FilenameUtils.concat(outputPath, "subgraph.avro");
    FSDataOutputStream outStream = null;
    DataFileWriter<GraphNodeData> avroStream = null;
    SpecificDatumWriter<GraphNodeData> writer = null;
    try {
      outStream = fs.create(new Path(outputFile));
      writer =
          new SpecificDatumWriter<GraphNodeData>(GraphNodeData.class);
      avroStream =
          new DataFileWriter<GraphNodeData>(writer);
      avroStream.create(node.getSchema(), outStream);
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }

    HashSet<String> visited = new HashSet<String>();
    for (String nodeId : nodeids) {
      visited = walk(reader, nodeId, numHops, avroStream, visited);
    }
    try {
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close the output stream.", e);
      System.exit(-1);
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {
        "inputpath", "outputpath", "start_nodes", "num_hops"};
    checkHasParametersOrDie(required_args);

    writeSubGraph();
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WalkGraph(), args);
    System.exit(res);
  }
}
