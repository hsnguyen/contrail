/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.FastQRecord;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This binary creates an indexed avro file from an avro file containing
 * reads sorted by id. This makes it easy to look up reads by id.
 *
 * The code assumes the reads are stored in .avro files in the input directory
 * provided by inputpath.
 *
 * Note: This code requires Avro 1.7
 */
public class CreateReadsIndex extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(CreateReadsIndex.class);

  ArrayList<FSDataInputStream> streams;

  @Override
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
  private List<String> getFiles() {
    String inputPath = (String) stage_options.get("inputpath");
    FileSystem fs = null;

    ArrayList<String> readFiles = new ArrayList<String>();
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
        readFiles.add(status.getPath().toString());
      }
    } catch (IOException e) {
      throw new RuntimeException("Problem moving the files: " + e.getMessage());
    }

    Collections.sort(readFiles);
    sLogger.info("Matched input files:");
    for (String name : readFiles) {
      sLogger.info(name);
    }

    return readFiles;
  }

  /**
   * A utility class which allows us to sort the various streams based
   * on the id of the element. We use this class to do a merge sort of
   * the various input files as we write the output file.
   */
  private static class RecordStream implements Comparable<RecordStream> {
    private final DataFileStream<FastQRecord> stream;
    private FastQRecord next;

    public RecordStream(DataFileStream<FastQRecord> fileStream) {
      stream = fileStream;
      next = null;
      if (stream.hasNext()) {
        next = stream.next();
      }
    }

    public FastQRecord getData() {
      return next;
    }

    public boolean hasNext() {
      return stream.hasNext();
    }

    public FastQRecord next() {
      next = stream.next();
      return next;
    }

    /**
     * Sort the items in ascending order.
     */
    @Override
    public int compareTo (RecordStream other) {
      return this.getData().getId().toString().compareTo(
          other.getData().getId().toString());
    }
  }

  /**
   * Construct a sorted set of streams to read from.
   * @return
   */
  private TreeSet<RecordStream> buildStreamSet(
      FileSystem fs,  List<String> graphFiles) {
    streams = new ArrayList<FSDataInputStream>();
    ArrayList<SpecificDatumReader<FastQRecord>> readers = new
        ArrayList<SpecificDatumReader<FastQRecord>>();
    TreeSet<RecordStream> recordStreams = new TreeSet<RecordStream>();
    for (String inputFile : graphFiles) {
      FSDataInputStream inStream = null;
      try {
        inStream = fs.open(new Path(inputFile));
      } catch (IOException e) {
        sLogger.fatal("Could not open file:" + inputFile, e);
        System.exit(-1);
      }
      SpecificDatumReader<FastQRecord> reader =
          new SpecificDatumReader<FastQRecord>(FastQRecord.class);

      streams.add(inStream);
      readers.add(reader);

      DataFileStream<FastQRecord> avroStream = null;
      try {
        avroStream = new DataFileStream<FastQRecord>(inStream, reader);
      } catch (IOException e) {
        sLogger.fatal("Could not create avro stream.", e);
        System.exit(-1);
      }

      RecordStream stream = new RecordStream(avroStream);

      if (stream.getData() == null) {
        // Stream is empty so continue;
        continue;
      }

      recordStreams.add(stream);
    }
    return recordStreams;
  }

  private void writeSortedGraph(TreeSet<RecordStream> recordStreams) {
    String outputPath = (String) stage_options.get("outputpath");
    SortedKeyValueFile.Writer.Options writerOptions =
        new SortedKeyValueFile.Writer.Options();

    FastQRecord fastQRecord = new FastQRecord();
    writerOptions.withConfiguration(getConf());
    writerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    writerOptions.withValueSchema(fastQRecord.getSchema());
    writerOptions.withPath(new Path(outputPath));

    SortedKeyValueFile.Writer<CharSequence, FastQRecord> writer = null;

    try {
      writer = new SortedKeyValueFile.Writer<CharSequence,FastQRecord>(
          writerOptions);
    } catch (IOException e) {
      sLogger.fatal("There was a problem creating file:" + outputPath, e);
      System.exit(-1);
    }

    int numNodes = 0;
    while (recordStreams.size() > 0) {
      RecordStream stream = recordStreams.pollFirst();
      try {
        writer.append(
            stream.getData().getId().toString(), stream.getData());
      } catch (IOException e) {
        sLogger.fatal("There was a problem writing to file:" + outputPath, e);
        System.exit(-1);
      }
      ++numNodes;
      if (stream.hasNext()) {
        stream.next();
        recordStreams.add(stream);
      }
    }

    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("There was a problem closing file:" + outputPath, e);
      System.exit(-1);
    }
    sLogger.info("Number of nodes written:" + numNodes);
  }

  @Override
  protected void stageMain () {
    String inputPath = (String) stage_options.get("inputpath");
    List<String> recordFiles = getFiles();
    if (recordFiles.size() == 0) {
      sLogger.fatal(
          "No .avro files found in:" + inputPath,
          new RuntimeException("No files matched."));
      System.exit(-1);
    }

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }

    TreeSet<RecordStream> recordStreams  = buildStreamSet(fs, recordFiles);
    writeSortedGraph(recordStreams);

    for (FSDataInputStream stream : streams) {
      try {
        stream.close();
      } catch(IOException e) {
        sLogger.fatal("Couldn't close stream", e);
        System.exit(-1);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CreateReadsIndex(), args);
    System.exit(res);
  }
}
