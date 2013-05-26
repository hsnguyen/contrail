/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import contrail.util.AvroFileContentsIterator;
import contrail.util.CharUtil;
import contrail.util.ContrailLogger;

/**
 * Select a subset of the threadable node groupings for processing.
 *
 * SplitThreadableGraph splits the graph into subgroups consisting of threadable
 * nodes and their neighbors. We need to select a subset of groups such that
 * each node appears in at most one of these groups.
 */
public class SelectThreadableGroups extends NonMRStage{
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      SelectThreadableGroups.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
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
   * The output scheme is a pair containing the id for a subgraph and a list of
   * nodes in the subgraph
   */
  private Schema getSchema() {
    return Pair.getPairSchema(
        Schema.create(Schema.Type.STRING),
        Schema.createArray(Schema.create(Schema.Type.STRING)));
  }

  public Path getOutPath() {
    // Writer for the connected components.
    Path outDir = new Path((String)stage_options.get("outputpath"));
    Path outPath = new Path(FilenameUtils.concat(
        outDir.toString(), "threadable_subgraphs.avro"));
    return outPath;
  }

  /**
   * Create a writer. The output scheme is a pair containing the id for
   * a subgraph and a list of nodes in the subgraph.
   */
  private DataFileWriter<Pair<CharSequence, List<CharSequence>>>
      createWriter() {
    DataFileWriter<Pair<CharSequence, List<CharSequence>>> writer = null;
    Path outDir = new Path((String)stage_options.get("outputpath"));
    try {
      FileSystem fs = outDir.getFileSystem(getConf());

      if (fs.exists(outDir) && !fs.isDirectory(outDir)) {
        sLogger.fatal(
            "outputpath points to an existing file but it should be a " +
            "directory:" + outDir.getName());
        System.exit(-1);
      } else {
        fs.mkdirs(outDir, FsPermission.getDefault());
      }

      FSDataOutputStream outStream = fs.create(getOutPath(), true);

      Schema schema = getSchema();
      DatumWriter<Pair<CharSequence, List<CharSequence>>> datumWriter =
          new SpecificDatumWriter<Pair<CharSequence, List<CharSequence>>>(
              schema);
      writer =
          new DataFileWriter<Pair<CharSequence, List<CharSequence>>>(
              datumWriter);
      writer.create(schema, outStream);

    } catch (IOException exception) {
      sLogger.fatal(
          "There was a problem writing the components to an avro file. " +
           "Exception: " + exception.getMessage(), exception);
    }
    return writer;
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");

    AvroFileContentsIterator<List<CharSequence>> groups =
        AvroFileContentsIterator.fromGlob(getConf(), inputPath);

    if (!groups.hasNext()) {
      sLogger.fatal("Input is empty.", new RuntimeException("No input."));
    }

    // Total number of groups.
    int numGroups = 0;
    // Number of nodes in the groups.
    int numNodesInGroups = 0;

    int component = -1;

    Pair<CharSequence, List<CharSequence>> outPair =
        new Pair<CharSequence, List<CharSequence>>(getSchema());

    DataFileWriter<Pair<CharSequence, List<CharSequence>>> writer =
        createWriter();

    HashSet<String> seenIds = new HashSet<String>();

    for (List<CharSequence> group : groups) {
      ++numGroups;

      boolean alreadySeen = false;
      List<String> thisGroup = CharUtil.toStringList(group);
      for (String id : thisGroup) {
        if (seenIds.contains(id)) {
          alreadySeen = true;
          break;
        }
      }
      seenIds.addAll(thisGroup);
      if (!alreadySeen) {
        ++component;
        numNodesInGroups += group.size();
        outPair.key(String.format("%03d", component));
        outPair.value(group);
        try {
          writer.append(outPair);
        } catch (IOException e) {
          sLogger.fatal("Couldn't write component:", e);
        }
      }
    }

    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close:" + getOutPath().toString(), e);
      System.exit(-1);
    }

    sLogger.info(String.format(
        "Outputted %d of %d groups", component + 1, numGroups));

    sLogger.info(
        String.format(
            "There are %d nodes in %d groups",
            numNodesInGroups, component +1));
  }

  public static void main(String[] args) throws Exception {
    SplitThreadableGraph stage = new SplitThreadableGraph();
    int res = stage.run(args);
    System.exit(res);
  }
}

