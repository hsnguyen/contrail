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
package contrail.tools;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import contrail.graph.GraphBFSIterator;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.IndexedGraph;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This stage uses an indexed graph to split the graph by doing a breadth
 * first search.
 *
 * This code splits the graph into pieces that should be small enough to fit
 * into memory. There is no guarantee that each piece is a connected component
 * because some connected components might be too large to fit in a single
 * piece.
 */
public class SplitGraph extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      SplitGraph.class);
  // Set of the nodeids that have already been visited.
  private HashSet<String> visitedIds;

  // An iterator of all entries in the file.
  private Iterator<AvroKeyValue<CharSequence, GraphNodeData>> kvIterator;

  // The full graph.
  private IndexedGraph graph;

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition max = new ParameterDefinition(
        "max_size", "Maximum number of nodes in a group.", Integer.class,
        10000);
    defs.put(max.getName(), max);
    return Collections.unmodifiableMap(defs);
  }

  private Schema getSchema() {
    return Schema.createArray((new GraphNodeData().getSchema()));
  }

  private Path getOutPath() {
    // Writer for the connected components.
    Path outDir = new Path((String)stage_options.get("outputpath"));
    Path outPath = new Path(FilenameUtils.concat(
        outDir.toString(), "graph.avro"));
    return outPath;
  }

  private DataFileWriter<List<GraphNodeData>> createWriter() {
    DataFileWriter<List<GraphNodeData>> writer = null;
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
      DatumWriter<List<GraphNodeData>> datumWriter =
          new SpecificDatumWriter<List<GraphNodeData>>(schema);
      writer =
          new DataFileWriter<List<GraphNodeData>>(datumWriter);
      writer.create(schema, outStream);

    } catch (IOException exception) {
      fail("There was a problem writing the components to an avro file. " +
           "Exception: " + exception.getMessage());
    }
    return writer;
  }

  @Override
  protected void stageMain() {
    visitedIds = new HashSet<String>();

    graph =  IndexedGraph.buildFromFile(
        (String)stage_options.get("inputpath"), getConf());

    int component = -1;

    DataFileWriter<List<GraphNodeData>> writer = createWriter();

    int maxSize = (Integer) stage_options.get("max_size");

    Iterator<AvroKeyValue<CharSequence, GraphNodeData>> iter = graph.iterator();
    int totalNodes = 0;
    int writtenNodes = 0;
    while (iter.hasNext()) {
      AvroKeyValue<CharSequence, GraphNodeData> pair = iter.next();
      ++totalNodes;
      if (visitedIds.contains(pair.getKey().toString())) {
        continue;
      }

      // Write the connected component for this node.
      ++component;

      sLogger.info(String.format("Writing component: %d", component));

      GraphBFSIterator bfsIterator = new GraphBFSIterator(
          graph, Arrays.asList(pair.getKey().toString()));

      ArrayList<GraphNodeData> nodes = new ArrayList<GraphNodeData>();

      while (nodes.size() < maxSize && bfsIterator.hasNext()) {
        GraphNode node = bfsIterator.next();
        visitedIds.add(node.getNodeId());
        nodes.add(node.clone().getData());
        ++writtenNodes;
      }
      try {
        writer.append(nodes);
      } catch (IOException e) {
        sLogger.fatal("Couldn't write nodes:", e);
      }
    }

    sLogger.info(
        String.format(
            "Ouputted %d nodes in %d components", totalNodes, component));
    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close:" + getOutPath().toString(), e);
      System.exit(-1);
    }

    if (totalNodes != writtenNodes) {
      sLogger.fatal(String.format(
          "Total nodes: %d Number of nodes written:%d", totalNodes,
          writtenNodes));
    }
  }

  public static void main(String[] args) throws Exception {
    SplitGraph stage = new SplitGraph();
    int res = stage.run(args);
    System.exit(res);
  }
}
