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
package contrail.stages;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.util.AvroFileContentsIterator;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestRekeyByComponentId {
  @Test
  public void testMR() {
    // Create some graph nodes.
    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "ACTGCT");
    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "ACTGCT");

    File temp = FileHelper.createLocalTempDir();
    String graphPath = FilenameUtils.concat(
        temp.getAbsolutePath(), "nodes.avro");

    GraphUtil.writeGraphToPath(
        new Configuration(), new Path(graphPath), Arrays.asList(nodeA, nodeB));

    Schema ComponentSchema = Pair.getPairSchema(
          Schema.create(Schema.Type.STRING),
          Schema.createArray(Schema.create(Schema.Type.STRING)));

    // Write components.
    ArrayList<Pair<CharSequence, List<CharSequence>>> components =
        new ArrayList<Pair<CharSequence, List<CharSequence>>>();

    Pair<CharSequence, List<CharSequence>> component1 =
        new Pair<CharSequence, List<CharSequence>>(ComponentSchema);
    component1.key("1");
    component1.value(Arrays.asList((CharSequence)"nodeA"));
    components.add(component1);

    Pair<CharSequence, List<CharSequence>> component2 =
        new Pair<CharSequence, List<CharSequence>>(ComponentSchema);
    component2.key("2");
    component2.value(Arrays.asList((CharSequence)"nodeB"));
    components.add(component1);

    Path componentPath = new Path(FilenameUtils.concat(
        temp.getAbsolutePath(), "component.avro"));
    AvroFileUtil.writeRecords(new Configuration(), componentPath, components);

    RekeyByComponentId stage = new RekeyByComponentId();
    stage.setParameter(
        "inputpath", graphPath + "," + componentPath.toString());
    stage.setParameter(
        "outputpath", FilenameUtils.concat(temp.getPath(), "outputpath"));

    assertTrue(stage.execute());

    // Open the output.
    AvroFileContentsIterator<Pair<CharSequence, GraphNodeData>> outputs =
        AvroFileContentsIterator.fromGlob(
            new Configuration(),
            FilenameUtils.concat(temp.getPath(), "part*avro"));

    for (Pair<CharSequence, GraphNodeData> pair : outputs) {
      if (pair.key().toString().equals("1")) {
        assertEquals("nodeA", pair.value().getNodeId().toString());
      } else if (pair.key().toString().equals("2")) {
        assertEquals("nodeB", pair.value().getNodeId().toString());
      } else {
        fail("Component is wrong.");
      }
    }

  }
}
