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
package contrail.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphError;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphUtil;
import contrail.graph.NodeMerger;
import contrail.sequences.DNAStrand;
import contrail.stages.QuickMergeAvro;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/**
 * This test is uses a graph which caused problems with the Staph dataset
 * on 2013 10 23.
 *
 * Currently this test just reproduces the invalid graph we were seeing
 * in the staph dataset.
 */
public class TestStaph20131023 {
  private static final Logger sLogger =
      Logger.getLogger(TestStaph20131023.class);

  private final int K = 41;

  /**
   * Read and validate the json records.
   */
  private ArrayList<GraphNode> readJsonInput() {
    // Read the json file containing the graph.
    // TODO(jlewi): This is hardcoded we should figure out how to include
    // it as a resource.
    Path inPath = new Path(
        "/home/jlewi/git_contrail/src/test/java/contrail/integration/" +
        "DebugGraphToJson.json");

    Schema schema = new GraphNodeData().getSchema();
    Configuration conf = new Configuration();
    ArrayList<GraphNodeData> records =
        AvroFileUtil.readJsonRecords(conf, inPath, schema);

    // Make sure the input graph is valid.
    ArrayList<GraphNode> inputNodes = new ArrayList<GraphNode>();
    for (GraphNodeData data : records) {
        inputNodes.add(new GraphNode(data).clone());
    }
    List<GraphError> inputErrors = GraphUtil.validateGraph(inputNodes, 41);
    assertEquals(0, inputErrors.size());

    return inputNodes;
  }

  private boolean isValid(HashMap<String, GraphNode> nodes) {
    List<GraphError> errors = GraphUtil.validateGraph(nodes, K);
    for (GraphError error : errors) {
      System.out.println(error.toString());
    }
    return errors.isEmpty();
  }

  /**
   * Merge some of the nodes to try to simplify the graph
   * while still reproducing the error.
   */
  private void mergeLoop(
      HashMap<String, GraphNode> nodes) {
    NodeMerger merger = new NodeMerger();

    // Terminals to merge.
    ArrayList<EdgeTerminal> terminals = new ArrayList<EdgeTerminal>();
    terminals.add(new EdgeTerminal("BvPgzf8RsLLxDwA", DNAStrand.FORWARD));
    terminals.add(new EdgeTerminal("MEGZz_-fwdzbQAI", DNAStrand.FORWARD));
    terminals.add(new EdgeTerminal("-QRBHCgBHw9xNgI", DNAStrand.FORWARD));
    terminals.add(new EdgeTerminal("4PTchw4OQ_HcIAM", DNAStrand.REVERSE));

    NodeMerger.MergeResult result =
        merger.mergeNodes("merged_loop_bv", terminals, nodes, K - 1 );

    nodes.put(result.node.getNodeId(), result.node);
    for (EdgeTerminal terminal : terminals) {
      nodes.remove(terminal.nodeId);
    }

    assertTrue(isValid(nodes));
  }

  /**
   * Remove a bunch of nodes
   */
  private void removeNodes(HashMap<String, GraphNode> nodes) {
    nodes.remove("4Mc0IIi8vuNxCQM");
    nodes.remove("xM9_IQgPiBuCHAA");
    nodes.remove("zdMC_HE8U3dFvwA");
    nodes.remove("xi8_gjzzJJnL_wA");
    nodes.get("zIbzA2DB8t9s_AI").removeNeighbor("xi8_gjzzJJnL_wA");

    assertTrue(isValid(nodes));
  }

  private void remove2ndTail(HashMap<String, GraphNode> nodes) {
    // "xXdCwdS1DTC1MQA"
    String[] ids = {
        "YdHDIAM_zzCzDgA", "hUUPgwz8PMPMOgA",
        "D67t9PIf_QA8BwA", "jBSSxmf0x9gMzQA", "yBrML8P_gAtw4AA"};

    for (String id : ids) {
      nodes.remove(id);
    }

    //nodes.get("hXwSLP_Exi2D_AE").removeNeighbor("xXdCwdS1DTC1MQA");
    nodes.get("xXdCwdS1DTC1MQA").removeNeighbor("YdHDIAM_zzCzDgA");
    assertTrue(isValid(nodes));
  }

  @Test
  public void testGraph() {
    ArrayList<GraphNode> inputNodes = readJsonInput();

    File tempDir = FileHelper.createLocalTempDir();
    String inputPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "input");
    String mergedPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "merged");

    HashMap<String, GraphNode> graph = new HashMap<String, GraphNode>();
    for (GraphNode n : inputNodes) {
      graph.put(n.getNodeId(), n);
    }
    removeNodes(graph);
    //remove2ndTail(graph);
    mergeLoop(graph);


    Configuration conf = new Configuration();
    GraphUtil.writeGraphToPath(
        conf,
        new Path(FilenameUtils.concat(inputPath, "graph.avro")),
        graph.values());

    // Lets run quick merge.
    QuickMergeAvro quickMerge = new QuickMergeAvro();
    quickMerge.setParameter("inputpath", inputPath);
    quickMerge.setParameter("outputpath", mergedPath);
    quickMerge.setParameter("K", K);
    if (!quickMerge.execute()) {
      fail("QuickMerge failed.");
    }

//    ValidateGraph validate = new ValidateGraph();
//    validate.setParameter("inputpath", mergedPath);
//    validate.setParameter("outputpath",
//        FilenameUtils.concat(tempDir.getAbsolutePath(), "validated"));
//    validate.setParameter("K", K);
//    if (!validate.execute()) {
//      fail("validate failed.");
//    }

    GraphNodeFilesIterator outputs = GraphNodeFilesIterator.fromGlob(
        new Configuration(),
        FilenameUtils.concat(mergedPath, "*.avro"));

    HashMap<String, GraphNode> mergedGraph = new HashMap<String, GraphNode>();
    for (GraphNode n : outputs) {
      mergedGraph.put(n.getNodeId(), n.clone());
    }

    // The graph shouldn't be valid because we are trying to reproduce the
    // error at this point. Once we have fixed the problem we will want to
    // get rid of this and ensure the graph is valid.
    assertFalse(isValid(mergedGraph));
  }
}
