package contrail.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.util.FileHelper;

public class TestWriteBubblesToJson extends WriteBubblesToJson {
  @Test 
  public void testAlign() {
    WriteBubblesReducer reducer = new WriteBubblesReducer();

    for (DNAStrand majorStrand : DNAStrand.values()) {
      for (DNAStrand midStrand : DNAStrand.values()) {
        for (DNAStrand minorStrand : DNAStrand.values()) {
          GraphNode head = GraphTestUtil.createNode("head", "ACTG");
          GraphNode middle = GraphTestUtil.createNode("mid1", "CTGACTG");          
          GraphNode tail = GraphTestUtil.createNode("tail", "CTGT");         

          GraphUtil.addBidirectionalEdge(head, majorStrand, middle, midStrand);
          GraphUtil.addBidirectionalEdge(middle, midStrand, tail, minorStrand);
          WriteBubblesReducer.Alignment alignment = 
              reducer.alignMiddle(middle, head.getNodeId(), tail.getNodeId());
          assertEquals(majorStrand, alignment.major);
          assertEquals(midStrand, alignment.middle);
          assertEquals(minorStrand, alignment.minor);
        }
      }
    }
  }

  @Test 
  public void testAlignCycle() {
    // Test we properly align cycles.
    WriteBubblesReducer reducer = new WriteBubblesReducer();

    for (DNAStrand majorStrand : DNAStrand.values()) {
      for (DNAStrand midStrand : DNAStrand.values()) {
        GraphNode head = GraphTestUtil.createNode("head", "ACTG");
        GraphNode middle = GraphTestUtil.createNode("mid1", "CTGTTACT");          
             
        GraphUtil.addBidirectionalEdge(head, majorStrand, middle, midStrand);
        GraphUtil.addBidirectionalEdge(middle, midStrand, head, majorStrand);
        WriteBubblesReducer.Alignment alignment = 
            reducer.alignMiddle(middle, head.getNodeId(), head.getNodeId());
        assertEquals(DNAStrand.FORWARD, alignment.major);
        if (majorStrand == DNAStrand.FORWARD) {
          assertEquals(midStrand, alignment.middle);
        } else {
          assertEquals(midStrand, DNAStrandUtil.flip(alignment.middle));
        }
        assertEquals(DNAStrand.FORWARD, alignment.minor);
      }
    }
  }
  
  @Test
  public void testWrite() {
    // Create a graph with a bubble.
    GraphNode head = GraphTestUtil.createNode("head", "ACTG");
    GraphNode tail = GraphTestUtil.createNode("tail", "CTGT");

    GraphNode middle1 = GraphTestUtil.createNode("mid1", "CTGACTG");
    middle1.setCoverage(5.0f);

    GraphNode middle2 = GraphTestUtil.createNode("mid2", "CTGTCTG");
    middle2.setCoverage(7.0f);

    GraphUtil.addBidirectionalEdge(
        head, DNAStrand.FORWARD, middle1, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        head, DNAStrand.FORWARD, middle2, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        middle1, DNAStrand.FORWARD, tail, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        middle2, DNAStrand.FORWARD, tail, DNAStrand.FORWARD);

    File tempDir = FileHelper.createLocalTempDir();
    Path avroPath = new Path(
        FilenameUtils.concat(tempDir.getPath(), "graph.avro"));
    GraphUtil.writeGraphToFile(
        new File(avroPath.toString()),
        Arrays.asList(head, middle1, middle2, tail));

    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("inputpath", avroPath.toString());
    String outPath = FilenameUtils.concat(tempDir.getPath(), "json");
    parameters.put("outputpath", outPath);

    WriteBubblesToJson stage = new WriteBubblesToJson();
    stage.setParameters(parameters);
    stage.setConf(new JobConf());

    assertTrue(stage.execute());

    String jsonFile = FilenameUtils.concat(outPath, "part-00000");
    System.out.println("Output:" + jsonFile);
    try {
      FileReader reader = new FileReader(jsonFile);
      BufferedReader buffered = new BufferedReader(reader);

      String line = buffered.readLine();
      String expected =
          "{\"majorId\":\"tail\",\"minorId\":\"head\",\"paths\":[" +
          "{\"majorStrand\":\"REVERSE\",\"minorStrand\":\"REVERSE\",\"pairs\":[{\"major\":{\"id\":\"mid2\",\"strand\":\"REVERSE\",\"length\":7,\"coverage\":7.0},\"minor\":{\"id\":\"mid1\",\"strand\":\"REVERSE\",\"length\":7,\"coverage\":5.0},\"editDistance\":1,\"editRate\":0.14285715}]}]}";
    } catch(FileNotFoundException e) {
      fail(e.getMessage());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
