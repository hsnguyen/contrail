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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphPath;
import contrail.graph.IndexedGraph;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.stages.QuickMergeUtil;

/**
 * Find all paths between two nodes.
 *
 * This stage uses a sorted and indexed AVRO file to efficiently walk the
 * graph.
 */
public class FindPaths extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(FindPaths.class);
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    defs.remove("outputpath");
    ParameterDefinition start = new ParameterDefinition(
        "start", "The terminal to start at in the format nodeId:Strand " +
        "where strand is F or R.", String.class, null);

    ParameterDefinition end = new ParameterDefinition(
        "end", "The terminal to end at in the format nodeId:Strand " +
        "where strand is F or R.", String.class, null);

    defs.put(start.getName(), start);
    defs.put(end.getName(), end);
    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);

    return Collections.unmodifiableMap(defs);
  }

  protected EdgeTerminal parseTerminal(String value) {
    String[] pieces = value.split(":");
    if (pieces.length != 2) {
      sLogger.fatal(
          "Couldn't parse a terminal from the string:" + value + " The " +
          "value should be in the format nodeId:F or nodeId:R.",
          new RuntimeException("Invalid terminal."));
    }

    pieces[1] = pieces[1].toUpperCase();

    DNAStrand strand = null;
    if (pieces[1].equals("F")) {
      strand = DNAStrand.FORWARD;
    } else if (pieces[1].equals("R")) {
      strand = DNAStrand.REVERSE;
    } else {
      sLogger.fatal(
          "The strand should be F or R",
          new RuntimeException("Invalid terminal."));
    }

    return new EdgeTerminal(pieces[0], strand);
  }

  protected void stageMain() {
    IndexedGraph graph = IndexedGraph.buildFromFile(
        (String)stage_options.get("inputpath"), getConf());

    EdgeTerminal start = parseTerminal((String)stage_options.get("start"));
    EdgeTerminal end = parseTerminal((String)stage_options.get("end"));

    if (start.equals(end)) {
      sLogger.fatal(
          "Start and end terminal can't be the same.",
          new RuntimeException("Bad terminals."));
    }

    // List of all paths that still need to be processed.
    ArrayList<GraphPath> unprocessed = new ArrayList<GraphPath>();

    // Paths which are complete.
    ArrayList<GraphPath> complete = new ArrayList<GraphPath>();

    {
      GraphPath startPath = new GraphPath();
      GraphNodeData nodeData = graph.lookupNode(start.nodeId);
      startPath.add(start, new GraphNode(nodeData));
      unprocessed.add(startPath);
    }

    while (unprocessed.size() > 0) {
      GraphPath path = unprocessed.remove(unprocessed.size() - 1);

      if (path.last().equals(end)) {
        complete.add(path);
        continue;
      }

      GraphNode lastNode = path.lastNode();
      EdgeTerminal lastTerminal = path.last();

      // Construct paths by appending all the outgoing nodes.
      for (EdgeTerminal next : lastNode.getEdgeTerminals(
               lastTerminal.strand, EdgeDirection.OUTGOING)) {
        GraphPath newPath = path.clone();
        newPath.add(next, new GraphNode(graph.lookupNode(next.nodeId)));
        unprocessed.add(newPath);
      }
    }

    int K = (Integer) stage_options.get("K");
    // Print out information
    System.out.println("Path \t coverage \t length");

    ArrayList<GraphNode> mergedNodes = new ArrayList<GraphNode>();
    for (int i = 0; i < complete.size(); ++i) {
      GraphPath path = complete.get(i);
      GraphNode merged = path.merge(K);

      // We print this rather than using a logger because we don't
      // want the logger preamble.
      System.out.println(String.format(
          "%d \t %f \t %d", i, merged.getCoverage(),
          merged.getSequence().size()));

      mergedNodes.add(merged);
    }

    // Compute the edit distances.
    int[][] editDistance = new int[mergedNodes.size()][mergedNodes.size()];
    for (int i = 0; i < mergedNodes.size(); ++i) {
      editDistance[i][i] = 0;
      for (int j = i +1; j < mergedNodes.size(); ++j) {
        int distance = mergedNodes.get(i).getSequence().computeEditDistance(
            mergedNodes.get(j).getSequence());
        editDistance[i][j] = distance;
        editDistance[j][i] = distance;
      }
    }

    System.out.println("Edit Distances:");

    String[] pathNames = new String[mergedNodes.size()];
    for (int i = 0; i < mergedNodes.size(); ++i) {
      pathNames[i] = Integer.toString(i);
    }

    System.out.println("Path:\t" + StringUtils.join(pathNames, "\t"));
    for (int i = 0; i < mergedNodes.size(); ++i) {
      String[] distances = new String[mergedNodes.size()];
      for (int j = 0; j < mergedNodes.size(); ++j) {
        distances[j] = Integer.toString(editDistance[i][j]);
      }
      System.out.println(
          Integer.toString(i) + "\t" + StringUtils.join(distances, "\t"));
    }

  }

  public static void main(String[] args) throws Exception {
    FindPaths stage = new FindPaths();
    int res = stage.run(args);
    System.exit(res);
  }
}
