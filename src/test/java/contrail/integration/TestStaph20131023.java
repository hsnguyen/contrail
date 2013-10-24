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

import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.GraphError;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphUtil;
import contrail.stages.QuickMergeAvro;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/**
 * This test is uses a graph which caused problems with the Staph dataset
 * on 2013 10 23.
 */
public class TestStaph20131023 {
  private static final Logger sLogger =
      Logger.getLogger(TestStaph20131023.class);
  @Test
  public void testGraph() {
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

    File tempDir = FileHelper.createLocalTempDir();
    String inputPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "input");
    String mergedPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "merged");

    AvroFileUtil.writeRecords(
        conf,
        new Path(FilenameUtils.concat(inputPath, "graph.avro")),
        records);

    // Lets run quick merge.
    QuickMergeAvro quickMerge = new QuickMergeAvro();
    quickMerge.setParameter("inputpath", inputPath);
    quickMerge.setParameter("outputpath", mergedPath);
    quickMerge.setParameter("K", new Integer(41));
    if (!quickMerge.execute()) {
      fail("QuickMerge failed.");
    }

    GraphNodeFilesIterator outputs = GraphNodeFilesIterator.fromGlob(
        new Configuration(),
        FilenameUtils.concat(mergedPath, "*.avro"));

    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
    for (GraphNode node : outputs) {
        nodes.add(node.clone());
    }
    // Validate the graph.
    List<GraphError> errors = GraphUtil.validateGraph(nodes, 41);
    for (GraphError error : errors) {
      System.out.println(error.toString());
    }
  }
}
