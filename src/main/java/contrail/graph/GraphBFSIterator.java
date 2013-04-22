/*
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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator which performs a breadth first search of the graph.
 */
public class GraphBFSIterator
    implements Iterator<GraphNode>, Iterable<GraphNode> {

  private IndexedGraph graph;

  // Use two sets so we can keep track of the hops.
  // We use sets because we don't want duplicates because duplicates
  // make computing hasNext() difficult;
  private HashSet<String> thisHop;
  private HashSet<String> nextHop;
  private ArrayList<String> seeds;
  private HashSet<String> visited;
  private int hop;

  public GraphBFSIterator(IndexedGraph graph, Collection<String> seeds) {
    thisHop = new HashSet<String>();
    nextHop = new HashSet<String>();
    visited = new HashSet<String>();
    this.seeds = new ArrayList<String>();;
    this.seeds.addAll(seeds);
    thisHop.addAll(seeds);
    hop = 0;

    this.graph = graph;
  }

  @Override
  public Iterator<GraphNode> iterator() {
    return new GraphBFSIterator(graph, seeds);
  }

  @Override
  public boolean hasNext() {
    return (!thisHop.isEmpty() || !nextHop.isEmpty());
  }

  @Override
  public GraphNode next() {
    if (thisHop.isEmpty()) {
      ++hop;
      HashSet<String> temp = thisHop;
      thisHop = nextHop;
      nextHop = temp;
    }

    if (thisHop.isEmpty()) {
      throw new NoSuchElementException();
    }

    // Find the first node that we haven't visited already.
    String nodeId = thisHop.iterator().next();

    // Remove nodeId from thisHop and nextHop.
    thisHop.remove(nodeId);

    // Its possible we were slated to visit this node on the next
    // hop in which case we want to remove it.
    nextHop.remove(nodeId);

    visited.add(nodeId);
    GraphNode node = graph.getNode(nodeId);

    for (String neighborId : node.getNeighborIds()) {
      if (!visited.contains(neighborId) && !thisHop.contains(neighborId)) {
        nextHop.add(neighborId);
      }
    }

    return node;
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
