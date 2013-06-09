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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAStrand;
import contrail.util.FileHelper;

public class TestFindNodesWithSpanningReads {
  @Test
  public void testExecute() {
    GraphNode node = GraphTestUtil.createNode("node", "ACGGT");

    int maxThreads = 100;
    // Add two incoming edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in2", DNAStrand.FORWARD),
        Arrays.asList("read2"), maxThreads);

    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);


    GraphNode nodeNoEdges = GraphTestUtil.createNode("noReads", "CCGT");

    File tempDir = FileHelper.createLocalTempDir();
    File avroFile = new File(tempDir, "graph.avro");

    GraphUtil.writeGraphToFile(avroFile, Arrays.asList(node, nodeNoEdges));

    // Run it.
    FindNodesWithSpanningReads stage = new FindNodesWithSpanningReads();
    File outputPath = new File(tempDir, "output");
    String[] args =
      {"--inputpath=" + tempDir.toURI().toString(),
        "--outputpath=" + outputPath.toURI().toString(),
      };
    try {
      stage.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }

    // Read the output.
    Pair<CharSequence, Integer> outPair =
        new Pair<CharSequence, Integer>("", 0);
    ArrayList<Pair<CharSequence, Integer>> outputs =
        new ArrayList<Pair<CharSequence, Integer>>();
    String outputAvroFile = FilenameUtils.concat(
        outputPath.toString(), "part-00000.avro");
    DatumReader<Pair<CharSequence, Integer>> datumReader =
        new SpecificDatumReader<Pair<CharSequence, Integer>>(
            outPair.getSchema());
    DataFileReader<Pair<CharSequence, Integer>> reader = null;
    try {
        reader = new DataFileReader<Pair<CharSequence, Integer>>(
            new File(outputAvroFile), datumReader);
    } catch (IOException io) {
      fail("Could not open the output:" + io.getMessage());
    }

    while (reader.hasNext()) {
      // Do we need to make a copy?
      outputs.add(reader.next());
    }

    assertEquals(1, outputs.size());
    assertEquals("node", outputs.get(0).key().toString());
  }
}
