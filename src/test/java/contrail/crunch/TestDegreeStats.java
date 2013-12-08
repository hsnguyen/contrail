/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package contrail.crunch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.graph.MinMaxDegreeStats;
import contrail.io.AvroFileContentsIterator;
import contrail.sequences.DNAStrand;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestDegreeStats {
  @Test
  public void test() throws Exception {
    File tempDir = FileHelper.createLocalTempDir();
    File inputDir = new File(FilenameUtils.concat(tempDir.getPath(), "input"));
    inputDir.mkdirs();
    
    GraphNode nodeA = GraphTestUtil.createNode("A", "ACTG");
    GraphNode nodeB = GraphTestUtil.createNode("B", "ACTG");
    GraphNode nodeC = GraphTestUtil.createNode("C", "ACTG");
    GraphNode nodeD = GraphTestUtil.createNode("D", "ACTG");
    
    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeB, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeC, DNAStrand.FORWARD, nodeA, DNAStrand.FORWARD);
    
    GraphUtil.writeGraphToFile(
        new File(FilenameUtils.concat(inputDir.toString(), "graph.avro")),
        Arrays.asList(nodeA, nodeB, nodeC, nodeD));
    
    DegreeStats stage = new DegreeStats();
    stage.setParameter("inputpath", inputDir.toString());
    
    String outputPath = FilenameUtils.concat(tempDir.toString(), "output");
    stage.setParameter("outputpath", outputPath);
    
    stage.execute();
    
    AvroFileContentsIterator<MinMaxDegreeStats> results = 
        AvroFileContentsIterator.fromGlob(new Configuration(), 
            FilenameUtils.concat(outputPath, "*.avro"));
    
    System.out.println("tempDir:" + tempDir.toString());
    
    // Results should be sorted by max, min.
    MinMaxDegreeStats expected = new MinMaxDegreeStats();
    // Node D.
    expected.setMax(0);
    expected.setMin(0);
    expected.setCount(1L);
    assertEquals(expected, results.next());
    
    // Node B, C
    expected.setMax(1);
    expected.setMin(0);
    expected.setCount(2L);
    assertEquals(expected, results.next());
    
    // Node A
    expected.setMax(2);
    expected.setMin(1);
    expected.setCount(1L);
    assertEquals(expected, results.next());
    
    assertFalse(results.hasNext());    
  }
}
