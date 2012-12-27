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
package contrail.graph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import contrail.ContrailConfig;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * A class for merging chains of nodes.
 *
 * TODO(jlewi): Replace all uses of NodeMerger with this class.
 */
public class NodeListMerger {
  private static final Logger sLogger = Logger.getLogger(NodeListMerger.class);
  /**
   * Copy edges from old_node to new node.
   * @param new_node: The node to copy to.
   * @param new_strand: Which strand of the node node we are considering.
   * @param old_node: The node to copy from.
   * @param old_strand: Which strand of the old node we are considering.
   * @param direction: Whether to consider  incoming or outgoing edges.
   * @param exclude: List of node ids to exclude from the copy.
   */
  protected void copyEdgesForStrand(
      GraphNode new_node, DNAStrand new_strand, GraphNode old_node,
      DNAStrand old_strand, EdgeDirection direction, HashSet<String> exclude) {

    List<EdgeTerminal> terminals =
      old_node.getEdgeTerminals(old_strand, direction);

    for (Iterator<EdgeTerminal> it = terminals.iterator();
         it.hasNext();) {
      EdgeTerminal terminal = it.next();
      if (exclude.contains(terminal.nodeId)) {
        continue;
      }

      // TODO(jlewi): We need to get the tags.
      List<CharSequence> tags = old_node.getTagsForEdge(old_strand, terminal);

      if (direction == EdgeDirection.INCOMING) {
        new_node.addIncomingEdgeWithTags(
            new_strand, terminal, tags, ContrailConfig.MAXTHREADREADS);
      } else {
        new_node.addOutgoingEdgeWithTags(
            new_strand, terminal, tags, ContrailConfig.MAXTHREADREADS);
      }
    }
  }

  /**
   * Make a copy of the R5Tags.
   * @param tags
   * @return
   */
//  protected static List<R5Tag> copyR5Tags(List<R5Tag> tags) {
//    List<R5Tag> new_list = new ArrayList<R5Tag>();
//    for (R5Tag tag: tags) {
//      R5Tag copy = new R5Tag();
//      copy.setTag(tag.getTag());
//      copy.setStrand(tag.getStrand());
//      copy.setOffset(tag.getOffset());
//      new_list.add(copy);
//    }
//    return new_list;
//  }

  /**
   * Reverse a list of R5 tags in place.
   * @param tags
   * @return
   */
//  protected static void reverseReads(List<R5Tag> tags, int length) {
//    for (R5Tag tag: tags) {
//      R5TagUtil.reverse(tag, length);
//    }
//  }

  /**
   * Datastructure for returning information about the merging of
   * two sequences.
   */
  /**
   * @author jlewi
   *
   */
  protected static class MergeInfo {
    // The canonical representation of the merged sequences.
    public Sequence canonical_merged;

    // merged_strand is the strand the merged sequence came from.
    public DNAStrand merged_strand;

    public int src_size;
    public int dest_size;

    // Whether we need to reverse the read tags.
    public boolean src_reverse;
    public int src_shift;
    public boolean dest_reverse;
    public int dest_shift;
  }

  /**
   * Merge the sequences.
   *
   */
  protected Sequence mergeSequences(
      List<EdgeTerminal> chain, Map<String, GraphNode> nodes,
      int overlap) {
    // Figure out how long the merged sequence is.
    int length = 0;
    for (EdgeTerminal terminal : chain) {
      // Don't count the overlap because it will be counted in the next
      // sequence.
      length += nodes.get(terminal.nodeId).getSequence().size() - overlap;
    }
    length += overlap;

    Sequence mergedSequence = new Sequence(DNAAlphabetFactory.create(), length);

    boolean isFirst = true;

    Sequence lastOverlap = null;
    Sequence currentOverlap = null;

    for (EdgeTerminal terminal : chain) {
      Sequence sequence = nodes.get(terminal.nodeId).getSequence();
      sequence = DNAUtil.sequenceToDir(sequence, terminal.strand);
      currentOverlap = sequence.subSequence(0, overlap);
      if (!isFirst) {
        // Check we overlap with the previous sequence.
        if (!lastOverlap.equals(currentOverlap)) {
          throw new RuntimeException(
              "Can't merge nodes. Sequences don't overlap by: " + overlap + " " +
              "bases.");
        }

        // For all but the first node we truncate the first overlap bases
        // because we only want to add them once.
        sequence = sequence.subSequence(overlap, sequence.size());
      } else {
        isFirst = false;
      }
      mergedSequence.add(sequence);
      lastOverlap = currentOverlap;
    }

    return mergedSequence;
  }

  /**
   * Construct a list of the R5 tags aligned with the merged sequence.
   * @param info
   * @param src_r5tags
   * @param dest_r5tags
   * @return
   */
//  protected static List<R5Tag> alignTags(
//      MergeInfo info, List<R5Tag> src_r5tags, List<R5Tag> dest_r5tags) {
//    // Update the read alignment tags (R5Fields).
//    // Make a copy of src tags.
//    List<R5Tag> src_tags = copyR5Tags(src_r5tags);
//    List<R5Tag> dest_tags = copyR5Tags(dest_r5tags);
//
//    // Reverse the reads if necessary.
//    if (info.src_reverse) {
//      reverseReads(src_tags, info.src_size);
//    }
//
//    if (info.dest_reverse) {
//      reverseReads(dest_tags, info.dest_size);
//    }
//
//    if (info.src_shift > 0) {
//      for (R5Tag tag : src_tags) {
//        tag.setOffset(tag.getOffset() + info.src_shift);
//      }
//    }
//
//    if (info.dest_shift > 0) {
//      for (R5Tag tag : dest_tags) {
//        tag.setOffset(tag.getOffset() + info.dest_shift);
//      }
//    }
//
//    src_tags.addAll(dest_tags);
//    return src_tags;
//  }
//
  /**
   * Compute the coverage for the result of merging two nodes.
   *
   * The resulting coverage is a weighted average of the source and destination
   * coverages. The weights are typically the length measured in # of Kmers
   * spanning the sequence; as opposed to base pairs. Consequently, the length
   * of a sequence is typically len(sequence) - K + 1, where len(sequence)
   * is the number of base pairs.
   * @param src_coverage
   * @param src_coverage_length
   * @param dest_coverage
   * @param dest_coverage_length
   * @return
   */
  protected float computeCoverage(
      Collection<EdgeTerminal> chain, Map<String, GraphNode> nodes,
      int overlap) {
    float coverageSum = 0;
    float weightSum = 0;

    Iterator<EdgeTerminal> itTerminal = chain.iterator();
    while (itTerminal.hasNext()) {
      EdgeTerminal terminal = itTerminal.next();
      GraphNode node = nodes.get(terminal.nodeId);
      float weight = node.getSequence().size() - overlap;
      coverageSum += node.getCoverage() * weight;
      weightSum += weight;
    }
    return coverageSum/weightSum;
  }

  /**
   * Container for the result of merging two nodes.
   */
  public static class MergeResult {
    // The merged node.
    public GraphNode node;

    // Which strand the merged sequence corresponds to.
    public DNAStrand strand;
  }
//
//  /**
//   * Merge two nodes.
//   *
//   * Cycles are currently preserved. So A->B->A will be merged as
//   * A->B->A.
//   *
//   * @param src: The source node
//   * @param dest: The destination node
//   * @param strands: Which strands the edge from src->dest comes from.
//   * @param overlap: The number of bases that should overlap between
//   *   the two sequences.
//   * @param src_coverage_length: The length to associate with the source
//   *   for the purpose of computing the coverage. This is typically the number
//   *   of KMers in the source i.e it is len(src) - K + 1.
//   * @param dest_coverage_length: The length to associate with the destination
//   *   for the purpose of computing the coverage.
//   * @return
//   * @throws RuntimeException if the nodes can't be merged.
//   */
//  public static MergeResult mergeNodes(
//      GraphNode src, GraphNode dest, StrandsForEdge strands, int overlap,
//      int src_coverage_length, int dest_coverage_length) {
//    // To merge two nodes we need to
//    // 1. Form the combined sequences
//    // 2. Update the coverage
//    // 3. Remove outgoing edges from src.
//    // 4. Remove Incoming edges to dest
//    // 5. Add Incoming edges to src
//    // 6. Add outgoing edges from dest
//    Sequence src_sequence = src.getSequence();
//    Sequence dest_sequence = dest.getSequence();
//    MergeInfo merge_info = mergeSequences(
//        src_sequence, dest_sequence, strands, overlap);
//
//    GraphNode new_node = new GraphNode();
//    new_node.setSequence(merge_info.canonical_merged);
//
//    // Preserve the edges we need to preserve the incoming/outgoing
//    // edges corresponding to the strands but we also need to consider
//    // the reverse complement of the strand.
//    // Add the incoming edges.
//    copyEdgesForStrand(
//        new_node, merge_info.merged_strand, src, StrandsUtil.src(strands),
//        EdgeDirection.INCOMING);
//
//    // add the outgoing edges.
//    copyEdgesForStrand(
//        new_node, merge_info.merged_strand, dest, StrandsUtil.dest(strands),
//        EdgeDirection.OUTGOING);
//
//    // Now add the incoming and outgoing edges for the reverse complement.
//    DNAStrand rc_strand = DNAStrandUtil.flip(merge_info.merged_strand);
//    StrandsForEdge rc_edge_strands = StrandsUtil.complement(strands);
//    copyEdgesForStrand(
//        new_node, rc_strand, dest, StrandsUtil.src(rc_edge_strands),
//        EdgeDirection.INCOMING);
//
//    // add the outgoing edges.
//    copyEdgesForStrand(
//        new_node, rc_strand, src, StrandsUtil.dest(rc_edge_strands),
//        EdgeDirection.OUTGOING);
//
//    // Handle a cycle. Suppose we have the graph A->B->A.
//    // and we are merging A and B. So the merged sequence is AB.
//    // Therefore when we move the incoming edges to the chain we get the
//    // edge B->AB which implies RC(AB)->RC(B). The other end of the chain
//    // has the edge RC(A)->RC(AB) which is the edge AB->A. We need to move
//    // these edges so that the result is:
//    // AB->A => AB->AB
//    // RC(AB)->RC(B) => RC(AB)->RC(AB).
//    EdgeTerminal srcTerminal = new EdgeTerminal(
//        src.getNodeId(), StrandsUtil.src(strands));
//
//    if (dest.getEdgeTerminalsSet(
//            StrandsUtil.dest(strands), EdgeDirection.OUTGOING).contains(
//                srcTerminal)) {
//      EdgeTerminal destTerminal = new EdgeTerminal(
//          dest.getNodeId(), StrandsUtil.dest(strands));
//
//      EdgeTerminal newTerminal = new EdgeTerminal(
//        new_node.getNodeId(), merge_info.merged_strand);
//
//      new_node.moveOutgoingEdge(
//          merge_info.merged_strand, srcTerminal, newTerminal);
//
//      new_node.moveOutgoingEdge(
//          DNAStrandUtil.flip(merge_info.merged_strand),
//          destTerminal.flip(), newTerminal.flip());
//    }
//
//    // Align the tags.
//    new_node.getData().setR5Tags(alignTags(
//            merge_info, src.getData().getR5Tags(), dest.getData().getR5Tags()));
//
//    // Compute the coverage.
//    new_node.getData().setCoverage(computeCoverage(
//        src.getCoverage(), src_coverage_length, dest.getCoverage(),
//        dest_coverage_length));
//
//    MergeResult result = new MergeResult();
//    result.node = new_node;
//    result.strand = merge_info.merged_strand;
//    return result;
//  }

  /**
   * Merge a bunch of nodes into a new node.
   * @param newId: The id to assign the merged node.
   * @param chain: A collection of the terminals to merge.
   * @param nodes: A map containing the actual nodes. Should also
   *   contain nodes with edges to the nodes at the ends of
   *   terminals if moving the edges.
   * @param overlap: The number of bases that should overlap between
   *   the two sequences.
   * @return
   * @throws RuntimeException if the nodes can't be merged.
   *
   * The coverage lengths for the source and destination are automatically
   * computed as the number of KMers overlapping by K-1 bases would span
   * the source and destination sequences. K = overlap +1.
   */
  public MergeResult mergeNodes(
      String newId, List<EdgeTerminal> chain, Map<String, GraphNode> nodes,
      int overlap) {
    Sequence mergedSequence = mergeSequences(chain, nodes, overlap);
    float coverage = computeCoverage(chain, nodes, overlap);

    Sequence canonicalSequence = DNAUtil.canonicalseq(mergedSequence);
    DNAStrand mergedStrand = DNAUtil.canonicaldir(mergedSequence);

    GraphNode newNode = new GraphNode();
    newNode.setNodeId(newId);
    newNode.setCoverage(coverage);
    newNode.setSequence(canonicalSequence);

    // List of nodes in the chain.
    HashSet<String> idsInChain = new HashSet<String>();
    for (EdgeTerminal terminal : chain) {
      idsInChain.add(terminal.nodeId);
    }

    EdgeTerminal startTerminal = chain.get(0);
    EdgeTerminal endTerminal = chain.get(chain.size() - 1);
    GraphNode startNode = nodes.get(chain.get(0).nodeId);
    GraphNode endNode = nodes.get(chain.get(chain.size() - 1).nodeId);
    // Preserve the edges we need to preserve the incoming/outgoing
    // edges corresponding to the strands but we also need to consider
    // the reverse complement of the strand.
    // Add the incoming edges. When copying edges to the chain
    // we exclude any edges to nodes in the chain. These edges arise
    // in the case of cycles which are handled separatly.
    copyEdgesForStrand(
        newNode, mergedStrand, startNode, startTerminal.strand,
        EdgeDirection.INCOMING, idsInChain);

    // add the outgoing edges.
    copyEdgesForStrand(
        newNode, mergedStrand, endNode, endTerminal.strand,
        EdgeDirection.OUTGOING, idsInChain);

    // Now add the incoming and outgoing edges for the reverse complement.
    DNAStrand rcStrand = DNAStrandUtil.flip(mergedStrand);

    copyEdgesForStrand(
        newNode, rcStrand, endNode, DNAStrandUtil.flip(endTerminal.strand),
        EdgeDirection.INCOMING, idsInChain);

    // add the outgoing edges.
    copyEdgesForStrand(
        newNode, rcStrand, startNode, DNAStrandUtil.flip(startTerminal.strand),
        EdgeDirection.OUTGOING, idsInChain);

    // TODO(jeremy@lewi.us): We should add options which allow cycles to be
    // broken.
    // Handle a cycle. Suppose we have the graph A->B->A.
    // The merged graph should be AB->AB which implies
    //  RC(AB)->RC(AB).
    if (endNode.getEdgeTerminalsSet(
            endTerminal.strand, EdgeDirection.OUTGOING).contains(
                startTerminal)) {


      // TODO(jlewi): We need to get the tags.
      List<CharSequence> tags = endNode.getTagsForEdge(
          endTerminal.strand, startTerminal);

      EdgeTerminal incomingTerminal = new EdgeTerminal(newId, mergedStrand);
      newNode.addIncomingEdgeWithTags(
            mergedStrand, incomingTerminal, tags,
            ContrailConfig.MAXTHREADREADS);

      // Now add the reverse complement
      EdgeTerminal outgoingTerminal = new EdgeTerminal(
          newId, DNAStrandUtil.flip(mergedStrand));
      newNode.addOutgoingEdgeWithTags(
          DNAStrandUtil.flip(mergedStrand), outgoingTerminal, tags,
          ContrailConfig.MAXTHREADREADS);
    }

    // TODO(jeremy@lewi.us) We need to copy and align all the R5Tags from
    // the nodes being merged to the new node.

    MergeResult result = new MergeResult();
    result.node = newNode;
    result.strand = mergedStrand;
    return result;

  }
}
