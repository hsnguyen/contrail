package contrail.graph;

import java.util.List;

import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

/**
 * Reverse a node so that the node stores the reverse complement of the sequence
 * currently stored.
 *
 * Reversing a node entails the following steps.
 * 1. Replace the stored sequence with the reverse sequence.
 * 2. We flip the source strand for all edges.
 * 3. We all the R5Tags.
 */
public class NodeReverser {
  public GraphNode flip(GraphNode input_node) {
    input = input_node;

    // We start by copying the input node. This way all fields that dont'
    // change (e.g coverage) are preserved.
    output = input.clone();

    reverseSequence();
    reverseEdges();
    reverseReadTags();

    return output;
  }

  /**
   * Reverse the edges in the output node.
   */
  protected void reverseEdges() {
    GraphNodeData node_data = output.getData();
    List<NeighborData> neighbors = node_data.getNeighbors();

    for (NeighborData neighbor: neighbors) {
      for (EdgeData edge_data: neighbor.getEdges()) {
        StrandsForEdge old_strands = edge_data.getStrands();
        DNAStrand src_strand = StrandsUtil.src(old_strands);
        DNAStrand dest_strand = StrandsUtil.dest(old_strands);
        StrandsForEdge new_strands = StrandsUtil.form(src_strand, dest_strand);
        edge_data.setStrands(new_strands);
      }
    }
  }

  /**
   * Reverse the tags that keep track of how this sequence is aligned with
   * actual reads.
   */
  protected void reverseReadTags() {

  }

  /**
   * Set the sequence in the output node to be the reverse complement
   * of the sequence in the input node.
   */
  protected void reverseSequence() {
    Sequence reverse_sequence = DNAUtil.reverseComplement(input.getSequence());
    output.setSequence(reverse_sequence);
  }

  private GraphNode input;
  private GraphNode output;
}
