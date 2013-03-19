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
package contrail.tools;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.FastQRecord;
import contrail.sequences.Sequence;

/**
 * A class which allows us to look at the information contained in threads.
 */
public class LookAtThreads {

  /**
   * Class for representing the alignment of a read to an edge.
   */
  private class ReadAlignment {
    // The query matches the subsequence [start, end) in the target.
    public int start;
    public int end;
  }

  /**
   * Return all graph paths that are consistent with the given thread.
   * @param graph: The graph indexed by node id.
   * @param head: The terminal for the start of the edge under consideration.
   * @param tail: The other terminal for the edge under consideration.
   * @param thread: The name of the read of the thread to consider.
   * @return
   */
  public List<GraphPath> findThreadPaths(
      IndexedGraph graph, IndexedReads reads, EdgeTerminal head,
      EdgeTerminal tail, String thread, int K) {
    GraphNode startNode = new GraphNode(graph.lookup(head.nodeId));
    GraphNode endNode = new GraphNode(graph.lookup(tail.nodeId));

    // Construct the sequence corresponding to the K+1 subsequence that
    // gave rise to this read.
    Sequence startSequence = DNAUtil.sequenceToDir(
        startNode.getSequence(), head.strand);
    startSequence = startSequence.subSequence(
        startSequence.size() - K, startSequence.size());

    Sequence endSequence = DNAUtil.sequenceToDir(
        endNode.getSequence(), tail.strand);
    startSequence.add(endSequence.subSequence(K, K + 1));


    Sequence edgeSequence = startSequence;

    // Find all alignments of the read to the K + 1 sequence corresponding
    // to the edge.
    FastQRecord read = reads.lookup(thread);

    Pattern edgePattern = Pattern.compile(edgeSequence.toString());

    // We need to consider both strands of the read.
    for (DNAStrand readStrand : DNAStrand.values()) {
      Sequence readSequence = new Sequence(
          read.getRead(), DNAAlphabetFactory.create());

      readSequence = DNAUtil.sequenceToDir(readSequence, readStrand);

      String readSequenceStr = readSequence.toString();


      Matcher matcher = edgePattern.matcher(readSequenceStr);
      while (matcher.find()) {
        ReadAlignment alignment = new ReadAlignment();
        alignment.start = matcher.start();
        alignment.end = matcher.end();
      }
      readSequenceStr.m
    }
  }
}
