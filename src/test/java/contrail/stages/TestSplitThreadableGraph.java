package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAStrand;
import contrail.util.AvroFileContentsIterator;
import contrail.util.CharUtil;
import contrail.util.FileHelper;

public class TestSplitThreadableGraph {
  private static final Logger sLogger =
      Logger.getLogger(TestSplitThreadableGraph.class);

  private static class TestCase {
    public HashMap<String, GraphNode> nodes;

    // Maping from component to expected ids in that component.
    public HashMap<String, ArrayList<String>> expectedOutput;

    public TestCase() {
      nodes = new HashMap<String, GraphNode>();
      expectedOutput = new HashMap<String, ArrayList<String>>();
    }
  }

  private TestCase createGraph() {
    GraphNode node = GraphTestUtil.createNode("node", "AAA");
    TestCase testCase = new TestCase();
    HashMap<String, GraphNode> nodes = testCase.nodes;
    int maxThreads = 100;

    List<String> tags1 = Arrays.asList("read1");
    List<String> tags2 = Arrays.asList("read2");
    List<String> allTags = new ArrayList<String>();
    allTags.addAll(tags1);
    allTags.addAll(tags2);

    // Add two incoming edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        allTags, maxThreads);

    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
        tags1, maxThreads);

    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out2", DNAStrand.FORWARD),
        tags2, maxThreads);

    nodes.put(node.getNodeId(), node.clone());
    {
      GraphNode inNode = GraphTestUtil.createNode("in", "AAA");
      inNode.addOutgoingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      nodes.put(inNode.getNodeId(), inNode);
    }

    {
      GraphNode outNode = GraphTestUtil.createNode("out1", "AAA");
      outNode.addIncomingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      nodes.put(outNode.getNodeId(), outNode);
    }

    {
      GraphNode outNode = GraphTestUtil.createNode("out2", "AAA");
      outNode.addIncomingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      nodes.put(outNode.getNodeId(), outNode);
    }

    ArrayList<String> expectedMembers = new ArrayList<String>();
    expectedMembers.addAll(nodes.keySet());

    testCase.expectedOutput.put("00000", expectedMembers);
    return testCase;
  }

  @Test
  public void testMain() {
    TestCase testCase = createGraph();

    File temp = FileHelper.createLocalTempDir();
    String graphPath = FilenameUtils.concat(temp.getAbsolutePath(), "graph");

    GraphUtil.writeGraphToPath(
        new Configuration(), new Path(graphPath), testCase.nodes.values());

    // Run it.
    SplitThreadableGraph stage = new SplitThreadableGraph();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    //stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", graphPath);
    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());
    stage.setParameters(params);

    assertTrue(stage.execute());

    AvroFileContentsIterator<Pair<CharSequence, List<CharSequence>>> outIterator
      = new AvroFileContentsIterator<Pair<CharSequence, List<CharSequence>>>(
          Arrays.asList(stage.getOutPath().toString()), new Configuration());

    HashMap<String, List<String>> outputs =
        new HashMap<String, List<String>>();
    for (Pair<CharSequence, List<CharSequence>> outPair : outIterator) {
      outputs.put(
          outPair.key().toString(), CharUtil.toStringList(outPair.value()));
    }
    assertEquals(testCase.expectedOutput, outputs);
    System.out.println("Files should be in:" + outputPath.toString());
  }
}
