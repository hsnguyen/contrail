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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

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
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This stage splits the graph by doing a breadth first search.
 *
 * This code splits the graph into pieces that should be small enough to fit
 * into memory. There is no guarantee that each piece is a connected component
 * because some connected components might be too large to fit in a single
 * piece.
 */
public class SplitGraph extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      SplitGraph.class);
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

  /**
   * The output scheme is a pair containing the id for a subgraph and a list of
   * nodes in the subgraph
   */

  private Schema getSchema() {
    return Pair.getPairSchema(
        Schema.create(Schema.Type.STRING),
        Schema.createArray(Schema.create(Schema.Type.STRING)));
  }

  private Path getOutPath() {
    // Writer for the connected components.
    Path outDir = new Path((String)stage_options.get("outputpath"));
    Path outPath = new Path(FilenameUtils.concat(
        outDir.toString(), "graph.avro"));
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
      fail("There was a problem writing the components to an avro file. " +
           "Exception: " + exception.getMessage());
    }
    return writer;
  }

  private static class BFSIterator
      implements Iterator<String>, Iterable<String> {

    // Use two sets so we can keep track of the hops.
    // We use sets because we don't want duplicates because duplicates
    // make computing hasNext() difficult. We use a TreeSet for thisHop
    // because we want to process the nodes in sorted order.
    // We use a hashset for nextHop to make testing membership fast.
    private TreeSet<String> thisHop;
    private HashSet<String> nextHop;
    protected String seed;
    private HashSet<String> visited;
    private int hop;
    private HashMap<String, ArrayList<String>> graph;
    public BFSIterator(
        HashMap<String, ArrayList<String>> graph, String seed) {
      thisHop = new TreeSet<String>();
      nextHop = new HashSet<String>();
      visited = new HashSet<String>();
      this.seed = seed;

      thisHop.add(seed);
      hop = 0;

      this.graph = graph;
    }

    @Override
    public Iterator<String> iterator() {
      return new BFSIterator(graph, seed);
    }

    @Override
    public boolean hasNext() {
      return (!thisHop.isEmpty() || !nextHop.isEmpty());
    }

    @Override
    public String next() {
      if (thisHop.isEmpty()) {
        ++hop;
        thisHop.addAll(nextHop);
        nextHop.clear();
      }

      if (thisHop.isEmpty()) {
        throw new NoSuchElementException();
      }

      // Find the first node that we haven't visited already.
      String nodeId = thisHop.pollFirst();
      // Its possible we were slated to visit this node on the next
      // hop in which case we want to remove it.
      nextHop.remove(nodeId);

      visited.add(nodeId);


      for (String neighborId : graph.get(nodeId)) {
        if (!visited.contains(neighborId)) {
          nextHop.add(neighborId);
        }
      }

      return nodeId;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Return the number of hops since the seeds. The seeds correspond to hop 0.
     *
     * @return
     */
    public int getHop() {
      return hop;
    }
  }

  @Override
  protected void stageMain() {
    GraphNodeFilesIterator nodeIterator = GraphNodeFilesIterator.fromGlob(
        getConf(), (String)stage_options.get("inputpath"));
    // The edge graph
    HashMap<String, ArrayList<String>> graph =
        new HashMap<String, ArrayList<String>>();
    DataFileWriter<Pair<CharSequence, List<CharSequence>>> writer =
        createWriter();

    Pair<CharSequence, List<CharSequence>> outPair = new Pair(getSchema());
    outPair.value(new ArrayList<CharSequence>());

    int component = -1;

    int totalNodes = 0;
    int writtenNodes = 0;

    // Load a compressed edge graph into memory.
    int readNodes = 0;
    for (GraphNode node : nodeIterator) {
      ArrayList<String> neighbors = new ArrayList<String>();
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
        continue;
      }

      neighbors.clear();
      neighbors.addAll(node.getNeighborIds());
      graph.put(node.getNodeId(), neighbors);
    }

    int maxSize = (Integer) stage_options.get("max_size");

    Iterator<String> idIterator = graph.keySet().iterator();
    HashSet<String> visitedIds = new HashSet<String>();
    while (idIterator.hasNext()) {
      String nodeId = idIterator.next();
      ++totalNodes;
      if (visitedIds.contains(nodeId)) {
        continue;
      }

      // Write the connected component for this node.
      ++component;
      outPair.key(String.format("%05d", component));
      BFSIterator bfsIterator = new BFSIterator(graph, nodeId);

      outPair.value().clear();
      while (outPair.value().size() < maxSize && bfsIterator.hasNext()) {
        nodeId = bfsIterator.next();
        if (visitedIds.contains(nodeId)) {
          continue;
        }
        visitedIds.add(nodeId);
        outPair.value().add(nodeId);
        ++writtenNodes;
      }
      try {
        writer.append(outPair);
        sLogger.info(String.format(
            "Writing component: %d. Size: %d",
            component, outPair.value().size()));

      } catch (IOException e) {
        sLogger.fatal("Couldn't write component:", e);
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

    if (component > totalNodes) {
      sLogger.fatal(String.format(
          "Number of components is larger than number of nodes"));
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
