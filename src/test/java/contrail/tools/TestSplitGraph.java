package contrail.tools;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
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
    String indexPath = FilenameUtils.concat(temp.getAbsolutePath(), "index");

    //Output the nodes in sorted order
    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
    nodes.addAll(builder.getAllNodes().values());

    GraphUtil.writeGraphToIndexedFile(
        new Configuration(), new Path(indexPath), nodes);

    // Run it.
    SplitGraph stage = new SplitGraph();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    //stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", indexPath);
    params.put("max_size", 3);
    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());

    stage.setParameters(params);

    assertTrue(stage.execute());
    System.out.println("Files should be in:" + outputPath.toString());
  }
}
