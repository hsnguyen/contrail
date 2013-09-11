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

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAStrand;
import contrail.stages.FindBubblesAvro;
import contrail.stages.PopBubblesAvro;
import contrail.tools.WriteGephiFile;
import contrail.util.FileHelper;

/**
 * Some integration tests for bubble correction.
 */
public class TestBubbleCorrection {

  @Test
  public void testPalindromes() {
    // Test bubbles involving palindromes.
    // We construct the graph:
    // X->{A, R{A}}>Z
    // B->Z
    // A=R(A)

    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "ATAT");
    // Set the node id's so that node2X is the major id.
    GraphNode nodeX = GraphTestUtil.createNode("node2X", "CAT");
    GraphNode nodeZ = GraphTestUtil.createNode("node1Z", "ATT");
    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "GAT");

    final int K = 3;
    GraphUtil.addBidirectionalEdge(
        nodeX, DNAStrand.FORWARD, nodeA, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeX, DNAStrand.FORWARD, nodeA, DNAStrand.REVERSE);
    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeZ, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.REVERSE, nodeZ, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeB, DNAStrand.FORWARD, nodeZ, DNAStrand.FORWARD);

    List<GraphNode> nodes =
        Arrays.asList(nodeA, nodeB, nodeX, nodeZ);
    GraphUtil.validateGraph(nodes, K);

    File tempDir = FileHelper.createLocalTempDir();
    String inputPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "input");
    String outputPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "output");

    (new File(inputPath)).mkdirs();
    GraphUtil.writeGraphToFile(
        new File(FilenameUtils.concat(inputPath, "graph.avro")), nodes);

    // Plot the result for convenient analysis.
    WriteGephiFile inputGraphStage = new WriteGephiFile();
    inputGraphStage.setParameter("inputpath", inputPath);
    inputGraphStage.setParameter(
        "outputpath", FilenameUtils.concat(inputPath, "input_graph.gexf"));
    inputGraphStage.setParameter("disallow_tmp", false);
    inputGraphStage.execute();

    String findDir = FilenameUtils.concat(outputPath, "FindBubbles");
    FindBubblesAvro findBubbles = new FindBubblesAvro();
    findBubbles.setParameter("K", K);
    findBubbles.setParameter("inputpath", inputPath);
    findBubbles.setParameter("outputpath", findDir);
    // Make the threshold large enough so that all nodes are eligible to be
    // bubbles.
    findBubbles.setParameter("bubble_length_threshold", 10);
    findBubbles.setParameter("bubble_edit_rate", 10f);

    assertTrue(findBubbles.execute());

    String popDir = FilenameUtils.concat(outputPath, "PopBubbles");
    PopBubblesAvro popBubbles = new PopBubblesAvro();
    popBubbles.setParameter("inputpath", findDir);
    popBubbles.setParameter("outputpath", popDir);
    assertTrue(popBubbles.execute());

    CharSequence majorID = GraphUtil.computeMajorID(
        nodeX.getNodeId(), nodeZ.getNodeId());
    assertEquals(nodeX.getNodeId(), majorID);

    GraphNodeFilesIterator outNodesIter = GraphNodeFilesIterator.fromGlob(
        popBubbles.getConf(), FilenameUtils.concat(popDir, "*.avro"));

    HashMap<String, GraphNode> outputNodes = new HashMap<String, GraphNode>();
    for (GraphNode n : outNodesIter) {
      outputNodes.put(n.getNodeId(), n.clone());
    }

    GraphNode outNodeZ = outputNodes.get(nodeZ.getNodeId());
    GraphNode outNodeX = outputNodes.get(nodeX.getNodeId());

    // nodeX should no longer have edges to the reverse strand of A.
    Set<EdgeTerminal> outEdgesXF = outNodeX.getEdgeTerminalsSet(
        DNAStrand.FORWARD, EdgeDirection.OUTGOING);

    assertFalse(outEdgesXF.contains(
        new EdgeTerminal(nodeA.getNodeId(), DNAStrand.REVERSE)));

    // Plot the result for convenient analysis.
    WriteGephiFile graphStage = new WriteGephiFile();
    graphStage.setParameter("inputpath", popDir);
    graphStage.setParameter(
        "outputpath", FilenameUtils.concat(outputPath, "graph.gexf"));
    graphStage.setParameter("disallow_tmp", false);
    graphStage.execute();
  }
}
