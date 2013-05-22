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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.log4j.Logger;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeFilesIterator;
import contrail.stages.ResolveThreads.SpanningReads;

/**
 * This stage splits the graph by doing a breadth first search.
 *
 * This code splits the graph into pieces that should be small enough to fit
 * into memory. There is no guarantee that each piece is a connected component
 * because some connected components might be too large to fit in a single
 * piece.
 */
public class SplitThreadableGraph extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      SplitThreadableGraph.class);
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
        outDir.toString(), "graph.avro"));
    return outPath;
  }

  /**
   * Used for storing the minimal information needed for this stage.
   */
  private static class ThreadableNode {
    private final String nodeId;
    private final Set<String> neighborIds;
    private final boolean threadable;

    public ThreadableNode(
        String nodeId, Set<String> neighborIds, boolean threadable) {
      this.nodeId = nodeId;
      this.neighborIds = new HashSet<String>();
      this.neighborIds.addAll(neighborIds);
      this.threadable = threadable;
    }

    public String getNodeId() {
      return nodeId;
    }

    public Set<String> getNeighborIds() {
      return neighborIds;
    }

    public boolean isThreadable() {
      return threadable;
    }
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
      fail("There was a problem writing the components to an avro file. " +
           "Exception: " + exception.getMessage());
    }
    return writer;
  }

  @Override
  protected void stageMain() {
    GraphNodeFilesIterator nodeIterator = GraphNodeFilesIterator.fromGlob(
        getConf(), (String)stage_options.get("inputpath"));
    // The edge graph
    HashMap<String, ThreadableNode> graph =
        new HashMap<String, ThreadableNode>();
    DataFileWriter<Pair<CharSequence, List<CharSequence>>> writer =
        createWriter();

    // The list of nodes which are threadable.
    ArrayList<String> threadableIds = new ArrayList<String>();

    Pair<CharSequence, List<CharSequence>> outPair = new Pair(getSchema());
    outPair.value(new ArrayList<CharSequence>());

    int component = -1;

    int islands = 0;
    // Number of threadable nodes outputted with neighbors.
    int numResolvable = 0;

    // Number of nodes which are neighbors of resolvable.
    int numNeighbors = 0;

    // Load a compressed edge graph into memory.
    int readNodes = 0;
    for (GraphNode node : nodeIterator) {
      ++readNodes;
      if (readNodes % 1000 == 0) {
        sLogger.info(String.format("Read %d nodes.", readNodes));
      }
      if (node.getNeighborIds().size() == 0) {
        // Output this node as its own component since it isn't connected
        // to anyone.
        ++component;
        outPair.key(String.format("%05d", component));
        outPair.value().clear();
        outPair.value().add(node.getNodeId());
        try {
          writer.append(outPair);
        } catch (IOException e) {
          sLogger.fatal("Couldn't write component:", e);
        }
        ++islands;
        continue;
      }

      SpanningReads spanningReads = ResolveThreads.findSpanningReads(node);
      boolean isThreadable = (spanningReads.spanningIds.size() > 0);
      ThreadableNode compressed = new ThreadableNode(
          node.getNodeId(), node.getNeighborIds(), isThreadable);

      if (isThreadable) {
        threadableIds.add(node.getNodeId());
      }
      graph.put(node.getNodeId(), compressed);
    }

    // Iterate over all threadable nodes. For each threadable node,
    // check if its already been outputted. If it hasn't then check if
    // any of its neighbors have been outputted. If no neighbors have been
    // outputted group the threadable node with its neighbors.
    for (String threadableId : threadableIds) {
      if (!graph.containsKey(threadableId)) {
        // Node was already outputted.
        continue;
      }

      ThreadableNode node = graph.remove(threadableId);
      // Check if all of the neighbors are still there.
      boolean hasNeighbors = true;
      for (String neighborId : node.getNeighborIds()) {
        if (!graph.containsKey(neighborId)) {
          hasNeighbors = false;
        }
      }

      if (!hasNeighbors) {
        // Just output this node.
        ++component;
        outPair.key(String.format("%05d", component));
        outPair.value().clear();
        outPair.value().add(node.getNodeId());
        try {
          writer.append(outPair);
        } catch (IOException e) {
          sLogger.fatal("Couldn't write component:", e);
        }
        continue;
      }

      // Output this node with its neighbors so we can resolve threads
      // in a subsequent step.
      for (String neighborId : node.getNeighborIds()) {
        graph.remove(neighborId);
      }

      numNeighbors += node.getNeighborIds().size();
      ++numResolvable;
      ++component;
      outPair.key(String.format("%05d", component));
      outPair.value().clear();
      outPair.value().add(node.getNodeId());
      outPair.value().addAll(node.getNeighborIds());
      try {
        writer.append(outPair);
      } catch (IOException e) {
        sLogger.fatal("Couldn't write component:", e);
      }
    }

    // Output all the remaining nodes in the graph.
    for (String nodeId : graph.keySet()) {
      ++component;
      outPair.key(String.format("%05d", component));
      outPair.value().clear();
      outPair.value().add(nodeId);
      try {
        writer.append(outPair);
        sLogger.info(String.format(
            "Writing component: %d. Size: %d",
            component, outPair.value().size()));

      } catch (IOException e) {
        sLogger.fatal("Couldn't write component:", e);
      }
    }

    int totalNodes = islands + numResolvable + numNeighbors + graph.size();

    sLogger.info(
        String.format(
            "Read %d nodes.", readNodes));
    sLogger.info(
        String.format(
            "Ouputted %d nodes in %d components", totalNodes, component + 1));

    sLogger.info(
        String.format(
            "Total threadable nodes: %d. Resolvable: %d.",
            threadableIds.size(), numResolvable));
    sLogger.info(
        String.format("Number of islands: %d.", islands));
    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close:" + getOutPath().toString(), e);
      System.exit(-1);
    }

    if (component > totalNodes) {
      sLogger.fatal(String.format(
          "Number of components is larger than number of nodes"));
    }
    if (totalNodes != readNodes) {
      sLogger.fatal(String.format(
          "Total nodes: %d doesn't equal Number of nodes read:%d", totalNodes,
          readNodes));
    }
  }

  public static void main(String[] args) throws Exception {
    SplitThreadableGraph stage = new SplitThreadableGraph();
    int res = stage.run(args);
    System.exit(res);
  }
}
