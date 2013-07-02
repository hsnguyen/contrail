package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import contrail.graph.GraphUtil;
import contrail.graph.LengthStatsData;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.FileHelper;

public class TestLengthStats {
  @Test
  public void testCombine() {
    Random generator = new Random();
    ArrayList<LengthStatsData> stats = new ArrayList<LengthStatsData>();
    Long length = 100L;
    int n = 2;
    long count = 0;

    LengthStatsData expected = new LengthStatsData();
    expected.setLength(length);

    ArrayList<Double> allCoverage = new ArrayList<Double>();
    ArrayList<Double> allDegree = new ArrayList<Double>();

    for (int i = 0; i < n; ++i) {
      LengthStatsData data = new LengthStatsData();
      data.setLength(length);

      data.setCount(generator.nextInt(100) + 1L);
      count += data.getCount();

      Double[] coverage = {Math.random() * 10, Math.random() * 10};
      allCoverage.addAll(Arrays.asList(coverage));

      data.setCoverageMin(Math.min(coverage[0], coverage[1]));
      data.setCoverageMax(Math.max(coverage[0], coverage[1]));
      data.setCoverageMean(Math.random() * 100);

      expected.setCoverageMean(
          expected.getCoverageMean() +
          data.getCoverageMean() * data.getCount());

      Double[] degree = {Math.random() * 10, Math.random() * 10};
      allDegree.addAll(Arrays.asList(degree));

      data.setDegreeMin(Math.min(degree[0], degree[1]));
      data.setDegreeMax(Math.max(degree[0], degree[1]));
      data.setDegreeMean(Math.random() * 100);

      expected.setDegreeMean(
          expected.getDegreeMean() +
          data.getDegreeMean() * data.getCount());

      stats.add(data);
    }
    Collections.sort(allDegree);
    Collections.sort(allCoverage);

    expected.setCount(count);
    expected.setCoverageMin(allCoverage.get(0));
    expected.setCoverageMax(allCoverage.get(allCoverage.size() - 1));
    expected.setDegreeMin(allDegree.get(0));
    expected.setDegreeMax(allDegree.get(allDegree.size() - 1));

    expected.setCoverageMean(expected.getCoverageMean() / count);
    expected.setDegreeMean(expected.getDegreeMean() / count);

    LengthStatsData actual = LengthStats.combine(
        LengthStats.BASE_NAMES, stats.iterator());

    assertEquals(expected.getLength(), actual.getLength());
    assertEquals(expected.getCount(), actual.getCount());
    assertEquals(expected.getCoverageMean(), actual.getCoverageMean());
    assertEquals(expected.getCoverageMax(), actual.getCoverageMax());
    assertEquals(expected.getCoverageMin(), actual.getCoverageMin());
    assertEquals(expected.getDegreeMean(), actual.getDegreeMean());
    assertEquals(expected.getDegreeMax(), actual.getDegreeMax());
    assertEquals(expected.getDegreeMin(), actual.getDegreeMin());

    assertEquals(expected, actual);
  }

  @Test
  public void testMR() {
    File tempDir = FileHelper.createLocalTempDir();
    String inputDir = FilenameUtils.concat(tempDir.getPath(), "inputpath");
    new File(inputDir).mkdir();

    String outputDir = FilenameUtils.concat(tempDir.getPath(), "outputpath");
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("AAACTTAAACTTGTC", 3);

    GraphUtil.writeGraphToFile(
        new File(FilenameUtils.concat(inputDir, "graph.avro")),
        builder.getAllNodes().values());

    LengthStats stage = new LengthStats();
    stage.setParameter("inputpath", inputDir);
    stage.setParameter("outputpath", outputDir);

    assertTrue(stage.execute());
    System.out.println("Output dir:" + outputDir);

    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = mapper.getJsonFactory();

    ArrayList<JsonNode> nodes = new ArrayList<JsonNode>();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(
          FilenameUtils.concat(outputDir, "part-00000")));

      String line;
      while ((line = reader.readLine()) != null) {
        JsonParser jp = factory.createJsonParser(line);
        JsonNode node = mapper.readTree(jp);

        nodes.add(node);
      }
      reader.close();
    } catch (IOException e) {
      fail(e.getMessage());
    }

    assertEquals(1, nodes.size());
    JsonNode node = nodes.get(0);
    assertEquals(3, node.get("length").getIntValue());
    assertEquals(8, node.get("count").getIntValue());
  }
}
