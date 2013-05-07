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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.IndexedGraph;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This stage uses an indexed graph to split the graph into connected
 * component. The nodes in each component are written to a different file.
 */
public class SplitConnectedComponents extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      SplitConnectedComponents.class);
  // Set of the nodeids that have already been visited.
  private HashSet<String> visitedIds;

  // An iterator of all entries in the file.
  private Iterator<AvroKeyValue<CharSequence, GraphNodeData>> kvIterator;

  // The full graph.
  private IndexedGraph graph;

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

  protected void stageMain() {
    visitedIds = new HashSet<String>();

    graph =  IndexedGraph.buildFromFile(
        (String)stage_options.get("inputpath"), getConf());

    int component = -1;

    Iterator<AvroKeyValue<CharSequence, GraphNodeData>> iter = graph.iterator();
    while (iter.hasNext()) {
      AvroKeyValue<CharSequence, GraphNodeData> pair = iter.next();
      if (visitedIds.contains(pair.getKey().toString())) {
        continue;
      }

      // Write the connected component for this node.
      ++component;

      // Writer for the connected components.
      Path outDir = new Path((String)stage_options.get("outputpath"));

      DataFileWriter<GraphNodeData> writer = null;
      Path outPath = new Path(FilenameUtils.concat(
          outDir.toString(),
          String.format("component-%03d.avro", component)));
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

        FSDataOutputStream outStream = fs.create(outPath, true);
        Schema schema = (new GraphNodeData()).getSchema();
        DatumWriter<GraphNodeData> datumWriter =
            new SpecificDatumWriter<GraphNodeData>(schema);
        writer =
            new DataFileWriter<GraphNodeData>(datumWriter);
        writer.create(schema, outStream);

      } catch (IOException exception) {
        fail("There was a problem writing the components to an avro file. " +
             "Exception: " + exception.getMessage());
      }

      sLogger.info(String.format("Writing component: %d", component));

      // List of the nodes to process.
      ArrayList<String> unprocessed = new ArrayList<String>();
      unprocessed.add(pair.getKey().toString());

      int numNodes = 0;
      while (unprocessed.size() > 0) {
        String nodeId = unprocessed.remove(unprocessed.size() - 1);

        if (visitedIds.contains(nodeId)) {
          continue;
        }
        visitedIds.add(nodeId);
        ++numNodes;
        GraphNode node = graph.getNode(nodeId);
        try {
          writer.append(node.getData());
        } catch (IOException e) {
          sLogger.fatal("Couldn't write node:" + node.getNodeId(), e);
          System.exit(-1);
        }
        unprocessed.addAll(node.getNeighborIds());
      }

      try {
        writer.close();
      } catch (IOException e) {
        sLogger.fatal("Couldn't close:" + outPath.toString(), e);
        System.exit(-1);
      }

      sLogger.info(String.format(
          "Component:%d \t Number of nodes:%d", component, numNodes));
    }
  }

  public static void main(String[] args) throws Exception {
    SplitConnectedComponents stage = new SplitConnectedComponents();
    int res = stage.run(args);
    System.exit(res);
  }
}
