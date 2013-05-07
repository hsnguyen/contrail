/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)

package contrail.tools;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import contrail.graph.GraphBFSIterator;
import contrail.graph.GraphNode;
import contrail.graph.IndexedGraph;
import contrail.stages.ParameterDefinition;

/**
 * Covert a subgraph into a gephi formatted XML file which can then be loaded
 * in gephi.
 *
 * Doc about gexf format:
 * http://gexf.net/1.2draft/gexf-12draft-primer.pdf
 *
 * WARNING: Gephi appears to have problems reading files in "/tmp" so
 * write the file somewhere else.
 *
 * The input must be an indexed avro file to support fast random access lookups.
 *
 * The input/output can be on any filesystem supported by the hadoop file api.
 *
 * TODO(jlewi): We should make color vary depending on the
 * node's length and coverage.
 *
 * TODO(jeremy@lewi.us): We should add an option restrict the graph
 * based on the number of hops and not the total number of nodes.
 */
public class WriteSubGraphAsGephiFile extends WriteGephiFileBase {
  private static final Logger sLogger =
      Logger.getLogger(WriteSubGraphAsGephiFile.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new WriteSubGraphAsGephiFile(), args);
    System.exit(res);
  }

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    Map<String, ParameterDefinition> params =
        new HashMap<String, ParameterDefinition> ();
    params.putAll(super.createParameterDefinitions());
    params.remove("num_hops");

    ParameterDefinition max_size = new ParameterDefinition(
        "max_size", "Max number of nodes to include.",
        Integer.class, null);

    params.put(max_size.getName(), max_size);
    return Collections.unmodifiableMap(params);
  }

  /**
   * An iterator which truncates the graph when a certain size is reached.
   *
   */
  protected class MaxNodesIterator extends GraphBFSIterator {
    private int maxSize;
    private int numReturned = 0;
    public MaxNodesIterator(
        IndexedGraph graph, Collection<String> seeds, int maxSize) {
      super(graph, seeds);
      this.maxSize = maxSize;
    }

    public Iterator<GraphNode> iterator() {
      return new MaxNodesIterator(graph, seeds, maxSize);
    }

    public boolean hasNext() {
      if (numReturned >= maxSize) {
        return false;
      }
      return super.hasNext();
    }

    public GraphNode next() {
      if (numReturned >= maxSize) {
        throw new NoSuchElementException();
      }
      ++numReturned;
     return super.next();
    }
  }

  @Override
  protected void stageMain() {
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - output: " + outputPath);
    writeGraph(outputPath);

    sLogger.info("Wrote: " + outputPath);
  }

  @Override
  protected Iterable<GraphNode> createNodesIterable() {
    String inputPath = (String) stage_options.get("inputpath");
    IndexedGraph graph = IndexedGraph.buildFromFile(inputPath, getConf());

    Integer maxSize = (Integer) stage_options.get("max_size");

    String startNode = (String) stage_options.get("start_node");
    Iterable<GraphNode> nodes = new MaxNodesIterator(
        graph, Arrays.asList(startNode), maxSize);

    return nodes;
  }
}
