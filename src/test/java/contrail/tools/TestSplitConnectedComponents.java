package contrail.tools;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.FileHelper;

public class TestSplitConnectedComponents {
  private static final Logger sLogger =
      Logger.getLogger(TestSplitConnectedComponents.class);
  @Test
  public void testMain() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("TACTGGATT", 3);
    builder.addKMersForString("AAACCC", 3);

    File temp = FileHelper.createLocalTempDir();
    String indexPath = FilenameUtils.concat(temp.getAbsolutePath(), "index");

    SortedKeyValueFile.Writer.Options writerOptions =
        new SortedKeyValueFile.Writer.Options();

    GraphNodeData nodeData = new GraphNodeData();
    writerOptions.withConfiguration(new Configuration());
    writerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    writerOptions.withValueSchema(nodeData.getSchema());
    writerOptions.withPath(new Path(indexPath));

    SortedKeyValueFile.Writer<CharSequence, GraphNodeData> writer = null;

    try {
      writer = new SortedKeyValueFile.Writer<CharSequence,GraphNodeData>(
          writerOptions);
    } catch (IOException e) {
      sLogger.fatal("There was a problem creating file:" + indexPath, e);
      fail("failed to create file.");
    }

    // Output the nodes in sorted order
    ArrayList<String> names = new ArrayList();
    names.addAll(builder.getAllNodes().keySet());
    Collections.sort(names);

    for (String name : names) {
      try {
        writer.append(name, builder.getNode(name).getData());
      } catch (IOException e) {
        fail("failed to write node");
      }
    }
    try {
      writer.close();
    } catch (IOException e) {
      fail(e.getMessage());
    }

    // Run it.
    SplitConnectedComponents stage = new SplitConnectedComponents();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    //stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", indexPath);

    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());

    stage.setParameters(params);

    assertTrue(stage.execute());
    System.out.println("Files should be in:" + outputPath.toString());
  }
}
