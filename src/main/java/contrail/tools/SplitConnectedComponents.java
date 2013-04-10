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
import java.util.PriorityQueue;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import contrail.graph.ConnectedComponentData;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.IndexedGraph;

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

    graph = new IndexedGraph(
        (String)stage_options.get("inputpath"), getConf());

    // Writer for the connected components.
    Path outPath = new Path((String)stage_options.get("outputpath"));

    DataFileWriter<ConnectedComponentData> writer = null;
    try {
      FileSystem fs = outPath.getFileSystem(getConf());

      if (fs.exists(outPath) && !fs.isDirectory(outPath)) {
        sLogger.fatal(
            "outputpath points to an existing file but it should be a " +
            "directory.");
      }
      FSDataOutputStream outStream = fs.create(outPath, true);
      Schema schema = (new ConnectedComponentData()).getSchema();
      DatumWriter<ConnectedComponentData> datumWriter =
          new SpecificDatumWriter<ConnectedComponentData>(schema);
      writer =
          new DataFileWriter<ConnectedComponentData>(datumWriter);
      writer.create(schema, outStream);

    } catch (IOException exception) {
      fail("There was a problem writing the components to an avro file. " +
           "Exception: " + exception.getMessage());
    }

    int component = 0;

    for (graph.)
    int numSorted = 0;
    int numUnsorted = 0;

    int dagNodes = 0;
    int nonDagNodes = 0;

    while (kvIterator.hasNext()) {
      // AvroKeyValue uses generic records so we just discard the
      // GraphNodeData and look it back up using IndexGraph to handle the
      // conversion to a specific record. This means we end up doing
      // two lookups for the first record which is inefficient.
      GenericRecord pair = (GenericRecord) kvIterator.next();
      String nodeId = pair.get("key").toString();
      if (visitedIds.contains(nodeId)) {
        continue;
      }

      sLogger.info("Walking component from node:" + nodeId);
      HashMap<String, NodeItem> subgraph = walkComponent(nodeId);

      // Output the connected component.
      sLogger.info("Sorting component from node:" + nodeId);
      ConnectedComponentData component = createComponent(subgraph);
      sLogger.info("Component size:" +  component.getNodes().size());

      if (component.getSorted()) {
        ++numSorted;
        dagNodes += component.getNodes().size();
      } else {
        ++numUnsorted;
        nonDagNodes += component.getNodes().size();
      }

      try {
        writer.append(component);
      } catch (IOException e) {
        sLogger.fatal("Couldn't write connected componet", e);
      }
    }

    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close:" + outPath.toString(), e);
    }

    sLogger.info(String.format(
        "Number sorted components:%d \t total nodes:%d", numSorted, dagNodes));
    sLogger.info(String.format(
        "Number components which aren't dags:%d \t total nodes:%d",
        numUnsorted, nonDagNodes));
  }

  public static void main(String[] args) throws Exception {
    FindConnectedComponents stage = new SplitConnectedComponents();
    int res = stage.run(args);
    System.exit(res);
  }
}
