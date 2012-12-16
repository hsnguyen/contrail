package contrail.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.graph.GraphNodeData;

/**
 * This binary creates an indexed avro file from an avro file containing
 * a graph. This makes it easy to look up nodes in the graph based on
 * the node id.
 *
 * The code assumes the graph is stored in .avro files in the input directory
 * provided by inputpath. It also assumes that to iterate over the nodes in
 * sorted order by nodeId we just sequentially process the nodes in the files
 * after sorting the files by name.
 *
 * Note: This code requies Avro 1.7
 */
public class CreateGraphIndex extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(CreateGraphIndex.class);

  protected Map<String, ParameterDefinition>
  createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Get a list of the graph files.
   */
  private List<String> getGraphFiles() {
    String inputPath = (String) stage_options.get("inputpath");
    FileSystem fs = null;

    ArrayList<String> graphFiles = new ArrayList<String>();
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }
    try {
      Path pathObject = new Path(inputPath);
      for (FileStatus status : fs.listStatus(pathObject)) {
        if (status.isDir()) {
          continue;
        }
        if (!status.getPath().toString().endsWith(".avro")) {
          continue;
        }
        graphFiles.add(status.getPath().toString());
      }
    } catch (IOException e) {
      throw new RuntimeException("Problem moving the files: " + e.getMessage());
    }

    Collections.sort(graphFiles);
    sLogger.info("Matched input files:");
    for (String name : graphFiles) {
      sLogger.info(name);
    }

    return graphFiles;
  }
  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    List<String> graphFiles = getGraphFiles();
    if (graphFiles.size() == 0) {
      sLogger.fatal(
          "No .avro files found in:" + inputPath,
          new RuntimeException("No files matched."));
      System.exit(-1);
    }

    SortedKeyValueFile.Writer.Options writerOptions =
        new SortedKeyValueFile.Writer.Options();

    GraphNodeData nodeData = new GraphNodeData();
    writerOptions.withConfiguration(getConf());
    writerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    writerOptions.withValueSchema(nodeData.getSchema());
    writerOptions.withPath(new Path(outputPath));

    SortedKeyValueFile.Writer<CharSequence, GraphNodeData> writer =
        new SortedKeyValueFile.Writer<CharSequence,GraphNodeData>(
            writerOptions);

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }

    for (String inputFile : graphFiles) {
      FSDataInputStream inStream = fs.open(new Path(inputFile));
      SpecificDatumReader<GraphNodeData> reader =
          new SpecificDatumReader<GraphNodeData>(GraphNodeData.class);

      DataFileStream<GraphNodeData> avroStream =
          new DataFileStream<GraphNodeData>(inStream, reader);

      while(avroStream.hasNext()) {
        avroStream.next(nodeData);
        writer.append(nodeData.getNodeId().toString(), nodeData);
      }
     avroStream.close();
    }

    writer.close();
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CreateGraphIndex(), args);
    System.exit(res);
  }
}
