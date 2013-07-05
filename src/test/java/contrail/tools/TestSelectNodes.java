/**
 * Licensed under the Apache License, Version 2.0 (the "License");
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.io.AvroFileContentsIterator;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestSelectNodes {
  @Test
  public void testNodes() {
    // Test the job when the input is GraphNodeData.
    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "ACTGT");
    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "ACTGT");

    File temp = FileHelper.createLocalTempDir();
    String graphPath = FilenameUtils.concat(
        temp.getAbsolutePath(), "graph.avro");

    GraphUtil.writeGraphToPath(
        new Configuration(), new Path(graphPath), Arrays.asList(nodeA, nodeB));

    SelectNodes stage = new SelectNodes();
    stage.setParameter("inputpath", graphPath);

    String outputPath = FilenameUtils.concat(temp.getAbsolutePath(), "output");
    stage.setParameter("outputpath", outputPath);
    stage.setParameter("nodes", "nodeA");
    assertTrue(stage.execute());

    AvroFileContentsIterator<GraphNodeData> iterator =
        AvroFileContentsIterator.fromGlob(
            new Configuration(),
            FilenameUtils.concat(outputPath, "*avro"));

    assertTrue(iterator.hasNext());
    assertEquals(nodeA, new GraphNode(iterator.next()));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testNodeArray() {
    // Test the job when the input is a list of GraphNodeData.
    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "ACTGT");
    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "ACTGT");

    File temp = FileHelper.createLocalTempDir();
    String graphPath = FilenameUtils.concat(
        temp.getAbsolutePath(), "graph.avro");


    List<GraphNodeData> nodes = new ArrayList<GraphNodeData>();
    nodes.add(nodeA.getData());
    nodes.add(nodeB.getData());

    List<List<GraphNodeData>> groups = new ArrayList<List<GraphNodeData>>();
    groups.add(nodes);
    Schema inputSchema = Schema.createArray(new GraphNodeData().getSchema());
    AvroFileUtil.writeRecords(
        new Configuration(), new Path(graphPath), groups, inputSchema);

    SelectNodes stage = new SelectNodes();
    stage.setParameter("inputpath", graphPath);

    String outputPath = FilenameUtils.concat(temp.getAbsolutePath(), "output");
    stage.setParameter("outputpath", outputPath);
    stage.setParameter("nodes", "nodeA");
    assertTrue(stage.execute());

    AvroFileContentsIterator<List<GraphNodeData>> iterator =
        AvroFileContentsIterator.fromGlob(
            new Configuration(),
            FilenameUtils.concat(outputPath, "*avro"));

    assertTrue(iterator.hasNext());
    List<GraphNodeData> output = iterator.next();
    assertEquals(2, output.size());
    assertFalse(iterator.hasNext());
  }
}
