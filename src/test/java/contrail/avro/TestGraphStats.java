package contrail.avro;

import static org.junit.Assert.fail;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphStatsData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

public class TestGraphStats extends GraphStats {
  private static class MapTestData {
    public GraphNodeData input;
    public int expectedBin;
    public GraphStatsData expectedStats;
  }

  private MapTestData createMapTestCase () {
    MapTestData test = new MapTestData();
    GraphNode node = new GraphNode();
    node.setNodeId("1");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));
    node.addOutgoingEdge(
        DNAStrand.FORWARD, new EdgeTerminal("2", DNAStrand.FORWARD));

    node.setCoverage(5.0F);


    test.input = node.getData();

    test.expectedBin = 6;
    test.expectedStats = new GraphStatsData();
    test.expectedStats.setCount(1L);
    test.expectedStats.setLengthSum(4);
    test.expectedStats.setDegreeSum(4 * 1);
    test.expectedStats.setCoverageSum(5.0 * 4);
    return test;
  }

  private void assertMapperOutput(
      MapTestData test,
      AvroCollectorMock<Pair<Integer, GraphStatsData>> collector) {
    assertEquals(1, collector.data.size());
    Pair<Integer, GraphStatsData> pair = collector.data.get(0);

    assertEquals(test.expectedBin, pair.key().intValue());
    assertEquals(test.expectedStats, pair.value());
  }

  @Test
  public void testMapper() {
    ArrayList<MapTestData> testCases = new ArrayList<MapTestData>();
    testCases.add(createMapTestCase());

    GraphStats.GraphStatsMapper mapper = new GraphStats.GraphStatsMapper();
    JobConf job = new JobConf(GraphStats.GraphStatsMapper.class);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapTestData test: testCases) {
      mapper.configure(job);

      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<Integer, GraphStatsData>>
        collector =
          new AvroCollectorMock<Pair<Integer, GraphStatsData>>();

      try {
        mapper.map(
            test.input, collector, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertMapperOutput(test, collector);
    }
  }

  private class ReducerTestCase {
    public List<GraphStatsData> inputs;
    public GraphStatsData expectedOutput;
  }

  private ReducerTestCase createReducerTest () {
    ReducerTestCase test = new ReducerTestCase();
    test.inputs = new ArrayList<GraphStatsData>();

    int num = 5;
    GraphStatsData total = new GraphStatsData();

    Random generator = new Random();

    for (int i = 0; i < num; ++i) {
      GraphStatsData stats = new GraphStatsData();
      stats.setCount((long) generator.nextInt(100) + 1);
      stats.setCoverageSum(generator.nextDouble());
      stats.setDegreeSum(generator.nextInt(100) + 1);
      stats.setLengthSum(generator.nextInt(100) + 1);

      test.inputs.add(stats);

      total.setCount(total.getCount() + stats.getCount());
      total.setCoverageSum(total.getCoverageSum() + stats.getCoverageSum());
      total.setDegreeSum(total.getDegreeSum() + stats.getDegreeSum());
      total.setLengthSum(total.getLengthSum() + stats.getLengthSum());
    }
    test.expectedOutput = total;
    return test;
  }

  private void assertReducerTestCase(
      ReducerTestCase test, AvroCollectorMock<GraphStatsData> collector) {
    assertEquals(1, collector.data.size());
    assertEquals(test.expectedOutput, collector.data.get(0));
  }

  @Test
  public void testReducer() {
    ArrayList<ReducerTestCase> testCases = new ArrayList<ReducerTestCase>();
    testCases.add(createReducerTest());

    JobConf job = new JobConf(GraphStatsReducer.class);
    GraphStatsReducer reducer = new GraphStatsReducer();
    reducer.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (ReducerTestCase test: testCases) {
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<GraphStatsData> collector =
        new AvroCollectorMock<GraphStatsData>();

      try {
        reducer.reduce(
            1, test.inputs, collector, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertReducerTestCase(test, collector);
    }
  }
}