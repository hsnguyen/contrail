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

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import contrail.io.IndexedRecords;

/**
 * Iterator for doing a breadth first search.
 *
 * TODO(jeremy@lewi.us): Figure out some way to share code with
 * GraphBFSIterator. The problem is making GraphNode implement UndirectedNode
 * in a way that is compatible with IndexedGraph.
 */
public class BFSIterator<T> implements Iterator<T>, Iterable<T> {
    // Use two sets so we can keep track of the hops.
    // We use sets because we don't want duplicates because duplicates
    // make computing hasNext() difficult. We use a TreeSet for thisHop
    // because we want to process the nodes in sorted order.
    // We use a hashset for nextHop to make testing membership fast.
    private final TreeSet<String> thisHop;
    private final HashSet<String> nextHop;
    protected String seed;
    private final HashSet<String> visited;
    private int hop;
    private final IndexedRecords<String, T> graph;
    public BFSIterator(
        IndexedRecords<String, T> graph, String seed) {
      thisHop = new TreeSet<String>();
      nextHop = new HashSet<String>();
      visited = new HashSet<String>();
      this.seed = seed;

      thisHop.add(seed);
      hop = 0;

      this.graph = graph;
    }

    @Override
    public Iterator<T> iterator() {
      return new BFSIterator(graph, seed);
    }

    @Override
    public boolean hasNext() {
      return (!thisHop.isEmpty() || !nextHop.isEmpty());
    }

    @Override
    public T next() {
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

      T raw = graph.get(nodeId);
      UndirectedNode node = (UndirectedNode) raw;
      for (String neighborId : node.getNeighborIds()) {
        if (!visited.contains(neighborId)) {
          nextHop.add(neighborId);
        }
      }

      return raw;
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
