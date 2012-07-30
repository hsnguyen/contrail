package contrail.avro;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphError;
import contrail.graph.GraphErrorCodes;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.graph.ValidateEdge;
import contrail.graph.ValidateMessage;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;

public class TestValidateGraph extends ValidateGraph {
  // This class serves as a container for the data for testing the mapper.
  private static class MapperTestCase {
    // The input to the mapper.
    public GraphNodeData input;

    // The expected outputs;
    public HashMap<String, ValidateMessage> outputs;
    public int K;
    public MapperTestCase() {
      outputs = new HashMap<String, ValidateMessage>();
    }
  }

  // Create a basic test for the mapper.
  private MapperTestCase createMapTest() {
    GraphNode node = new GraphNode();
    node.setNodeId("some_node");
    Sequence sequence = new Sequence("ACTGC", DNAAlphabetFactory.create());
    node.setSequence(sequence);

    EdgeTerminal forwardEdge = new EdgeTerminal("forward", DNAStrand.FORWARD);
    EdgeTerminal reverseEdge = new EdgeTerminal("reverse", DNAStrand.REVERSE);

    node.addOutgoingEdge(DNAStrand.FORWARD, forwardEdge);
    node.addOutgoingEdge(DNAStrand.REVERSE, reverseEdge);

    MapperTestCase test = new MapperTestCase();
    test.input = node.clone().getData();
    test.K = 3;
    ValidateMessage nodeMessage = new ValidateMessage();
    nodeMessage.setNode(node.clone().getData());

    test.outputs.put(node.getNodeId(), nodeMessage);

    {
      ValidateMessage forwardMessage = new ValidateMessage();
      ValidateEdge edgeInfo = new ValidateEdge();
      edgeInfo.setSourceId(node.getNodeId());
      edgeInfo.setStrands(StrandsForEdge.FF);

      Sequence overlap = sequence.subSequence(
          sequence.size() - test.K + 1, sequence.size());
      edgeInfo.setOverlap(overlap.toCompressedSequence());
      forwardMessage.setEdgeInfo(edgeInfo);
      test.outputs.put(forwardEdge.nodeId, forwardMessage);
    }

    {
      ValidateMessage message = new ValidateMessage();
      ValidateEdge edgeInfo = new ValidateEdge();
      edgeInfo.setSourceId(node.getNodeId());
      edgeInfo.setStrands(StrandsForEdge.RR);

      Sequence overlap = DNAUtil.reverseComplement(sequence) ;
      overlap = overlap.subSequence(
          sequence.size() - test.K + 1, sequence.size());
      edgeInfo.setOverlap(overlap.toCompressedSequence());
      message.setEdgeInfo(edgeInfo);
      test.outputs.put(reverseEdge.nodeId, message);
    }
    return test;
  }

  private void assertMapperOutput(
      MapperTestCase testCase,
      AvroCollectorMock<Pair<CharSequence, ValidateMessage>> collector) {
    HashMap<String, ValidateMessage> actualOutputs =
        new HashMap<String, ValidateMessage>();
    for (Pair<CharSequence, ValidateMessage> outPair: collector.data) {
      actualOutputs.put(outPair.key().toString(), outPair.value());
    }

    assertEquals(testCase.outputs, actualOutputs);
  }

  @Test
  public void testMapper() {
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();
    test_cases.add(createMapTest());

    ValidateGraphMapper mapper = new ValidateGraphMapper();
    JobConf job = new JobConf(ValidateGraphMapper.class);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapperTestCase test_case: test_cases) {
      job.setInt("K", test_case.K);
      mapper.configure(job);

      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<CharSequence, ValidateMessage>>
        collector_mock =
          new AvroCollectorMock<Pair<CharSequence, ValidateMessage>>();

      try {
        mapper.map(
            test_case.input, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertMapperOutput(test_case, collector_mock);
    }
  }

  // A container class used for organizing the data for the reducer tests.
  private class ReducerTestCase {
    public ReducerTestCase() {
      input = new ArrayList<ValidateMessage>();
    }

    public String reducerKey;
    // The input to the reducer.
    public List<ValidateMessage> input;
    // The expected error or null if no error.
    public GraphErrorCodes errorCode;
  }

  private ReducerTestCase createNoNodeTest() {
    // Create a test case where the node is missing.
    ValidateMessage message = new ValidateMessage();
    ValidateEdge edgeInfo = new ValidateEdge();
    edgeInfo.setSourceId("a");
    Sequence sequence = new Sequence("ACGT", DNAAlphabetFactory.create());
    edgeInfo.setOverlap(sequence.toCompressedSequence());
    edgeInfo.setStrands(StrandsForEdge.FF);
    message.setEdgeInfo(edgeInfo);

    ReducerTestCase test = new ReducerTestCase();
    test.input.add(message);
    test.reducerKey = "B";
    test.errorCode = GraphErrorCodes.MISSING_NODE;
    return test;
  }

  private ReducerTestCase createDuplicateNodeTest() {
    // Create a test case where the node is duplicated.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    int K = 3;
    builder.addKMersForString("ACGT", K);

    GraphNode source = builder.getNode(builder.findNodeIdForSequence("ACG"));
    GraphNode dest = builder.getNode(builder.findNodeIdForSequence("CGT"));

    ReducerTestCase test = new ReducerTestCase();
    for (int i = 0; i < 2; ++i) {
      ValidateMessage message = new ValidateMessage();
      GraphNode node = dest.clone();

      // Remove the edge to the source.
      node.removeNeighbor(source.getNodeId());
      message.setNode(node.getData());
      test.input.add(message);
    }
    test.reducerKey = dest.getNodeId();
    test.errorCode = GraphErrorCodes.DUPLICATE_NODE;
    return test;
  }

  private ReducerTestCase createMissingEdgeTest() {
    // Create a test case where the node is missing an edge.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    int K = 3;
    builder.addKMersForString("ACGT", K);

    GraphNode source = builder.getNode(builder.findNodeIdForSequence("ACG"));
    GraphNode dest = builder.getNode(builder.findNodeIdForSequence("CGT"));

    ReducerTestCase test = new ReducerTestCase();
    {
      ValidateMessage message = new ValidateMessage();
      GraphNode node = dest.clone();

      // Remove the edge to the source.
      node.removeNeighbor(source.getNodeId());
      message.setNode(node.getData());
      test.input.add(message);
    }
    {
      ValidateMessage message = new ValidateMessage();
      ValidateEdge edgeInfo = new ValidateEdge();
      edgeInfo.setSourceId(source.getNodeId());
      Sequence sequence = new Sequence("CGT", DNAAlphabetFactory.create());
      edgeInfo.setOverlap(sequence.toCompressedSequence());
      edgeInfo.setStrands(StrandsForEdge.FR);
      message.setEdgeInfo(edgeInfo);
      test.input.add(message);
    }

    test.reducerKey = dest.getNodeId();
    test.errorCode = GraphErrorCodes.MISSING_EDGE;
    return test;
  }

  private ReducerTestCase createValidGraphTest() {
    // Create a test case where the graph is valid.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    int K = 3;
    builder.addKMersForString("ACGT", K);

    GraphNode source = builder.getNode(builder.findNodeIdForSequence("ACG"));
    GraphNode dest = builder.getNode(builder.findNodeIdForSequence("CGT"));

    ReducerTestCase test = new ReducerTestCase();
    {
      ValidateMessage message = new ValidateMessage();
      GraphNode node = dest.clone();
      message.setNode(node.getData());
      test.input.add(message);
    }
    {
      ValidateMessage message = new ValidateMessage();
      ValidateEdge edgeInfo = new ValidateEdge();
      edgeInfo.setSourceId(source.getNodeId());
      Sequence sequence = new Sequence("CGT", DNAAlphabetFactory.create());
      edgeInfo.setOverlap(sequence.toCompressedSequence());
      edgeInfo.setStrands(StrandsForEdge.FR);
      message.setEdgeInfo(edgeInfo);
      test.input.add(message);
    }

    test.reducerKey = dest.getNodeId();
    test.errorCode = null;
    return test;
  }

  // Asserts that the output of the reducer is correct for this test case.
  private void assertReducerTestCase(
      ReducerTestCase test,
      AvroCollectorMock<GraphError> collector) {

    if (test.errorCode == null) {
      assertEquals(0, collector.data.size());
      return;
    }
    assertEquals(test.errorCode, collector.data.get(0).getErrorCode());
  }

  @Test
  public void testReducer() {
    ArrayList<ReducerTestCase> testCases = new ArrayList<ReducerTestCase>();
    testCases.add(createNoNodeTest());
    testCases.add(createMissingEdgeTest());
    testCases.add(createDuplicateNodeTest());
    testCases.add(createValidGraphTest());
    ValidateGraphReducer reducer = new ValidateGraphReducer();

    JobConf job = new JobConf(ValidateGraphReducer.class);
    reducer.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (ReducerTestCase test: testCases) {
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<GraphError> collector =
        new AvroCollectorMock<GraphError>();

      try {
        reducer.reduce(
            test.reducerKey, test.input, collector, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertReducerTestCase(test, collector);
    }
  }
  /**
   * Create a temporary directory.
   * @return
   */
  private File createTempDir() {
    File temp = null;
    try {
      temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
    }
    if(!(temp.delete())){
      throw new RuntimeException(
          "Could not delete temp file: " + temp.getAbsolutePath());
    }

    if(!(temp.mkdir())) {
      throw new RuntimeException(
          "Could not create temp directory: " + temp.getAbsolutePath());
    }
    return temp;
  }

  private void writeGraph(File avroFile, Map<String, GraphNode> nodes) {
    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datumWriter =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (GraphNode node: nodes.values()) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }
  }

  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGATT", 3);

    // Add some tips.
    builder.addEdge("ATT", "TTG", 2);
    builder.addEdge("ATT", "TTC", 2);

    File temp = createTempDir();
    File avroFile = new File(temp, "graph.avro");

    writeGraph(avroFile, builder.getAllNodes());

    // Run it.
    ValidateGraph stage = new ValidateGraph();
    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());

    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
       "--K=3"};

    // Catch the following after debugging.
    try {
      stage.run(args);
    } catch (Exception exception) {
      exception.printStackTrace();
      fail("Exception occured:" + exception.getMessage());
    }
  }
}