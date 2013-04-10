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
package contrail.util;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.io.AvroIndexReader;
import contrail.io.IndexedRecords;

/**
 * Provides random access to a graph stored in a sorted/indexed avro file.
 *
 */
public class IndexedGraph implements IndexedRecords<String, GraphNodeData> {
  // TODO(jeremy@lewi.us): Move this into contrail.graph

  private IndexedRecords<String, GraphNodeData> index;

  /**
   * Construct an IndexedGraph using the index passed in.
   * @param index
   */
  public IndexedGraph(IndexedRecords<String, GraphNodeData> index) {
    this.index = index;
  }

  /**
   * Construct an index for the specified file.
   *
   * @param inputPath
   * @param conf
   */
  public static IndexedGraph buildFromFile(
      String inputPath, Configuration conf) {
    AvroIndexReader<String, GraphNodeData> reader = new
        AvroIndexReader<String, GraphNodeData>(
            inputPath, conf, Schema.create(Schema.Type.STRING),
            (new GraphNodeData()).getSchema());
    return new IndexedGraph(reader);
  }

  public GraphNodeData get(String nodeId) {
    return index.get(nodeId);
  }

  public GraphNode getNode(String nodeId) {
    return new GraphNode(get(nodeId));
  }

  /**
   * Read the node from the sorted key value file.
   *
   * Deprecated: Use get().
   * @param reader
   * @param nodeId
   * @return
   */
  @Deprecated
  public GraphNodeData lookupNode(String nodeId) {
    return get(nodeId);
  }
}
