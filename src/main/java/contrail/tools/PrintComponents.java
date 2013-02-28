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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import contrail.graph.ConnectedComponentData;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This class provides information about the connected components in a file.
 */
public class PrintComponents extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      PrintComponents.class);

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    defs.remove("outputpath");

    return Collections.unmodifiableMap(defs);
  }

  int numSorted = 0;
  int numUnsorted = 0;

  int dagNodes = 0;
  int nonDagNodes = 0;

  protected void stageMain() {
    Path inPath = new Path((String) this.stage_options.get("inputpath"));
    try {
      FSDataInputStream inStream = inPath.getFileSystem(getConf()).open(
          inPath);
      SpecificDatumReader<ConnectedComponentData> reader =
          new SpecificDatumReader<ConnectedComponentData>();
      DataFileStream<ConnectedComponentData> fileReader =
          new DataFileStream<ConnectedComponentData>(inStream, reader);

      int num = 0;
      while (fileReader.hasNext()) {
        ConnectedComponentData component = fileReader.next();
        if (component.getSorted()) {
          GraphNode node = new GraphNode(component.getNodes().get(0));
          EdgeTerminal start = null;
          if (node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING) > 0) {
            start = new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD);
          } else {
            start = new EdgeTerminal(node.getNodeId(), DNAStrand.REVERSE);
          }

          node.setData(component.getNodes().get(
              component.getNodes().size() - 1));
          EdgeTerminal end = null;
          if (node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING) > 0) {
            end = new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD);
          } else {
            end = new EdgeTerminal(node.getNodeId(), DNAStrand.REVERSE);
          }

          System.out.println(String.format(
              "Component %d: num nodes: %d start: %s end: %s", num,
              component.getNodes().size(), start, end));

          ++numSorted;
          dagNodes += component.getNodes().size();

        } else {
          System.out.println(String.format(
              "Component %d: num nodes: %d not a tree", num,
              component.getNodes().size()));

          ++numUnsorted;
          nonDagNodes += component.getNodes().size();
        }
        ++num;
      }
    } catch (IOException e) {
      sLogger.fatal("There was a problem reading the connected components", e);
    }

    System.out.println(String.format(
        "Number sorted components:%d \t total nodes:%d", numSorted, dagNodes));
    System.out.println(String.format(
        "Number components which aren't dags:%d \t total nodes:%d",
        numUnsorted, nonDagNodes));
  }

  public static void main(String[] args) throws Exception {
    PrintComponents stage = new PrintComponents();
    int res = stage.run(args);
    System.exit(res);
  }
}
