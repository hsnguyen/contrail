package contrail.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.AvroFileContentsIterator;
import contrail.util.CharUtil;
import contrail.util.FileHelper;

public class TestSplitGraph {
  private static final Logger sLogger =
      Logger.getLogger(TestSplitGraph.class);

  @Test
  public void testMain() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("TACTGGATT", 3);
    builder.addKMersForString("AAACCC", 3);

    File temp = FileHelper.createLocalTempDir();
    String graphPath = FilenameUtils.concat(temp.getAbsolutePath(), "graph");

    //Output the nodes in sorted order
    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
    nodes.addAll(builder.getAllNodes().values());

    GraphUtil.writeGraphToPath(
        new Configuration(), new Path(graphPath), nodes);

    // Run it.
    SplitGraph stage = new SplitGraph();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    //stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", graphPath);
    params.put("max_size", 10);
    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());

    stage.setParameters(params);

    assertTrue(stage.execute());

    AvroFileContentsIterator<Pair<CharSequence, List<CharSequence>>> outIterator
      = new AvroFileContentsIterator<Pair<CharSequence, List<CharSequence>>>(
          Arrays.asList(stage.getOutPath().toString()), new Configuration());

    HashMap<String, List<String>> pieces = new HashMap<String, List<String>>();

    HashSet<String> ids = new HashSet<String>();
    for (Pair<CharSequence, List<CharSequence>> pair : outIterator) {
      pieces.put(pair.key().toString(), CharUtil.toStringList(pair.value()));

      // Check if any of these nodes have already been seen.
      HashSet<String> newIds = CharUtil.toStringSet(pair.value());
      newIds.retainAll(ids);
      assertEquals(0, newIds.size());

      ids.addAll(CharUtil.toStringList(pair.value()));
    }
    assertEquals(2, pieces.size());


    System.out.println("Files should be in:" + outputPath.toString());
  }
}
