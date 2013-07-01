package contrail.stages;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Random;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import contrail.graph.LengthStatsData;

public class TestLengthStats {
  @Test
  public void testCombine() {
    Random generator = new Random();
    ArrayList<GenericRecord> stats = new ArrayList<GenericRecord>();
    Long length = 100L;
    int n = 15;
    long count = 0;

    LengthStatsData expected = new LengthStatsData();
    expected.setLength(length);
    for (int i = 0; i < n; ++i) {
      LengthStatsData data = new LengthStatsData();
      data.setLength(length);

      data.setCount(generator.nextInt(100) + 1L);
      count += data.getCount();

      Double[] coverage = {Math.random() * 10, Math.random() * 10};
      data.setCoverageMin(Math.min(coverage[0], coverage[1]));
      data.setCoverageMax(Math.max(coverage[0], coverage[1]));
      data.setCoverageMean(Math.random() * 100);

      expected.setCoverageMean(data.getCoverageMean() * data.getCount());


      Double[] degree = {Math.random() * 10, Math.random() * 10};
      data.setDegreeMin(Math.min(degree[0], degree[1]));
      data.setDegreeMax(Math.max(degree[0], degree[1]));
      data.setDegreeMean(Math.random() * 100);

      expected.setDegreeMean(data.getDegreeMean() * data.getCount());

      stats.add((GenericRecord) data);
    }

    expected.setCoverageMean(expected.getCoverageMean() / count);
    expected.setDegreeMean(expected.getDegreeMean() / count);

    LengthStatsData actual = new LengthStatsData();
    LengthStats.combine(
        LengthStats.BASE_NAMES, stats.iterator(), (GenericRecord) actual);
    assertEquals(expected, actual);
  }
}
