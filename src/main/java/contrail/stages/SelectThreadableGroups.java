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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
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

    ParameterDefinition maxGroupSize = new ParameterDefinition(
        "max_subgraph_size", "The maximum number of nodes in any of the " +
        "subgraphs.", Integer.class, new Integer(1000));
    defs.put(maxGroupSize.getName(), maxGroupSize);
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

    // TODO(jeremy@lewi.us): make this a parameter.
    // The maximum number of allowed nodes in a group.
    int maxGroupSize = (Integer)stage_options.get("max_subgraph_size");

    // The groupId used to indicate a node isn't assigned a group.
    Integer unassignedGroup = -1;

    // Total number of groups.
    int numGroups = 0;
    int component = -1;

    // Mapping from a nodeId to the id for the group it is currently assigned
    // to.
    HashMap<String, Integer> idToGroup = new HashMap<String, Integer>();

    // Mapping from the id of each group to the nodes in that group.
    HashMap<Integer, List<String>> subGraphs =
        new HashMap<Integer, List<String>>();

    HashSet<Integer> groupsToMerge = new HashSet<Integer>();
    ArrayList<String> unassignedNodes = new ArrayList<String>();

    int numMergeTooLarge = 0;
    for (List<CharSequence> group : groups) {
      ++numGroups;
      groupsToMerge.clear();
      unassignedNodes.clear();

      // TODO(jeremy@lewi.us): The group should already be sorted and not
      // contain duplicates.
      List<String> thisGroup = CharUtil.toStringList(group);
      if (thisGroup.size() != CharUtil.toStringSet(group).size()) {
        HashSet<String> groupSet = CharUtil.toStringSet(group);
        sLogger.fatal(
            "Nodes appear multiple times in the input group:" +
                StringUtils.join(thisGroup,","));
      }
      for (String id : thisGroup) {
        if (id.equals("wh0dQNlBEyvQQgA")) {
          sLogger.info("LEWI NO COMMIT");
        }
        Integer assignedGroup = idToGroup.get(id);
        if (assignedGroup != null && assignedGroup != unassignedGroup) {
          groupsToMerge.add(idToGroup.get(id));
        } else {
          unassignedNodes.add(id);
        }
      }

      Integer groupId = null;
      if (groupsToMerge.size() == 0) {
        // This group is unique.
        ++component;
        groupId = component;
        subGraphs.put(component, thisGroup);

        for (String id : thisGroup) {
          idToGroup.put(id, component);
        }
        continue;
      }

      // Check if merging the groups would produce a group that is too large.
      int newSize = unassignedNodes.size();
      for (Integer mergeId : groupsToMerge) {
        newSize += subGraphs.get(mergeId).size();
      }

      if (newSize > maxGroupSize) {
        ++numMergeTooLarge;
        // Can't merge the nodes.
        for (String id : unassignedNodes) {
          if (id.equals("wh0dQNlBEyvQQgA")) {
            sLogger.info("LEWI NO COMMIT");
          }
          idToGroup.put(id, unassignedGroup);
        }
        continue;
      }

      // Merge all the groups.
      Iterator<Integer> groupIterator = groupsToMerge.iterator();
      groupId = groupIterator.next();

      while (groupIterator.hasNext()) {
        Integer otherGroup = groupIterator.next();
        List<String> otherNodes = subGraphs.remove(otherGroup);

        subGraphs.get(groupId).addAll(otherNodes);

        for (String node : otherNodes) {
          if (node.equals("wh0dQNlBEyvQQgA")) {
            sLogger.info("LEWI NO COMMIT");
          }
          idToGroup.put(node, groupId);
        }
      }

      // Add in the new nodes.
      subGraphs.get(groupId).addAll(unassignedNodes);
      for (String node : unassignedNodes) {
        if (node.equals("wh0dQNlBEyvQQgA")) {
          sLogger.info("LEWI NO COMMIT");
        }
        idToGroup.put(node,  groupId);
      }
    }


    Pair<CharSequence, List<CharSequence>> outPair =
        new Pair<CharSequence, List<CharSequence>>(getSchema());
    DataFileWriter<Pair<CharSequence, List<CharSequence>>> writer =
        createWriter();
    outPair.value(new ArrayList<CharSequence>());

    int graphId = -1;
    // Number of nodes in the subgraphs.
    int numNodesInSubGraphs = 0;
    for (List<String> subGraph : subGraphs.values()) {
      Collections.sort(subGraph);
      // Make sure its unique.
      for (int i = 1; i < subGraph.size(); ++i) {
        if (subGraph.get(i - 1).equals(subGraph.get(i))) {
          sLogger.fatal(
              "Nodes appear multiple times in the group:" +
                  StringUtils.join(subGraph, ","),
              new RuntimeException("Group invalid"));
        }
      }

      ++graphId;
      numNodesInSubGraphs += subGraph.size();
      outPair.key(String.format("%03d", graphId));

      outPair.value().clear();
      outPair.value().addAll(subGraph);
      try {
        writer.append(outPair);
      } catch (IOException e) {
        sLogger.fatal("Couldn't write component:", e);
      }
    }
    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close:" + getOutPath().toString(), e);
      System.exit(-1);
    }

    sLogger.info(
        String.format(
            "Number of input groups: %d", numGroups));
    sLogger.info(String.format(
        "Outputted %d nodes in %d subgraphs", numNodesInSubGraphs,
        graphId + 1));
    sLogger.info(String.format(
        "Number of merges that would have exceeded max group size: %d",
        numMergeTooLarge));
  }

  public static void main(String[] args) throws Exception {
    SelectThreadableGroups stage = new SelectThreadableGroups();
    int res = stage.run(args);
    System.exit(res);
  }
}

