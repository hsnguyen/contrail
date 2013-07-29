/*
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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.stages.CompressibleNodeData;
import contrail.stages.CompressibleStrands;
import contrail.io.AvroFileContentsIterator;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestSelectCompressibleNodes {
  @Test
  public void testSelect() {
    String testDir = FileHelper.createLocalTempDir().getPath();

    GraphNode node = new GraphNode();
    node.setNodeId("nodeA");
    node.setSequence(new Sequence("GCTAG", DNAAlphabetFactory.create()));

    CompressibleNodeData compressA = new CompressibleNodeData();
    compressA.setNode(node.getData());
    compressA.setCompressibleStrands(CompressibleStrands.NONE);

    CompressibleNodeData compressB = new CompressibleNodeData();
    compressB.setNode(node.getData());
    compressB.setCompressibleStrands(CompressibleStrands.BOTH);

    String inputPath = FilenameUtils.concat(testDir, "inputpath");
    String outputPath = FilenameUtils.concat(testDir, "outputpath");

    Configuration conf = new Configuration();
    AvroFileUtil.writeRecords(
        conf, new Path(FilenameUtils.concat(inputPath, "input.avro")),
        Arrays.asList(compressA, compressB));

    SelectCompressibleNodes stage = new SelectCompressibleNodes();
    stage.setParameter("inputpath", inputPath);
    stage.setParameter("outputpath", outputPath);

    assertTrue(stage.execute());
    AvroFileContentsIterator<CompressibleNodeData> iterator =
        AvroFileContentsIterator.fromGlob(
            conf, FilenameUtils.concat(outputPath, "*.avro"));

    ArrayList<CompressibleNodeData> outputs =
        new ArrayList<CompressibleNodeData>();
    for (CompressibleNodeData n : iterator) {
      outputs.add(n);
    }
    assertEquals(1, outputs.size());
  }
}
