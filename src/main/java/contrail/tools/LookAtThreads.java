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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.biojava3.alignment.Alignments;
import org.biojava3.alignment.SimpleGapPenalty;
import org.biojava3.alignment.SubstitutionMatrixHelper;
import org.biojava3.alignment.Alignments.PairwiseSequenceAlignerType;
import org.biojava3.alignment.template.SequencePair;
import org.biojava3.alignment.template.SubstitutionMatrix;
import org.biojava3.core.sequence.DNASequence;
import org.biojava3.core.sequence.compound.DNACompoundSet;
import org.biojava3.core.sequence.compound.NucleotideCompound;
import org.biojava3.core.sequence.location.template.Point;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphPath;
import contrail.graph.IndexedGraph;
import contrail.io.IndexedRecords;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.FastQRecord;
import contrail.sequences.Sequence;
import contrail.util.ListUtil;

/**
 * A class which allows us to look at the information contained in threads.
 */
public class LookAtThreads {

  protected SimpleGapPenalty gapP;
  protected SubstitutionMatrix<NucleotideCompound> matrix;
  public LookAtThreads() {
    // Setting the gap penalties is tricky because we only want to allow
    // insertions at the start/end because one sequence might extend past
    // the other. However, there shouldn't be insertions/deletions in the
    // regions where the reads overlap, only mismatch because of sequencing
    // errors. If set the open and extension penalty to larger than the mismatch
    // penalties (which are 4), then the aligner will penalize non overlap
    // For example we might get the alignment
    // ACTC
    // TCGA
    // When what we want is
    // ACTC
    // --TCGA
    //
    // However if make the insertion penalty zero we might get
    // ACTCGA
    // AC--GA
    // When really there is no good match because insertions shouldn't
    // really be allowed inside the read.
    gapP = new SimpleGapPenalty();
    gapP.setOpenPenalty((short)0);
    gapP.setExtensionPenalty((short)0);

    matrix = SubstitutionMatrixHelper.getNuc4_4();
  }

  protected static class Coordinates {
    // TODO(jlewi): Which strand and sequence do the coordinates refer to.
    public int start;
    public int end;

    // Which strand of the sequence is aligned to the aligned sequence.
    public DNAStrand strand;
  }

  /**
   * Class for representing the alignment of a read to an edge.
   */
  protected static class ReadAlignment {
    // TODO(jlewi): Should we just use the classes in biojava for representing
    // alignments.
    public String readId;

    // Aligned length is the length of the aligned sequence.
    // e.g suppose we align
    // -ACTCG
    // TACT--
    // Then the aligned length is 6

    // The coordinates of the read on the aligned sequence
    public Coordinates coordinates;

    public ReadAlignment() {
      coordinates = new Coordinates();
    }
  }

  /**
   * Trace the path in the graph given by a read.
   *
   * @param graph: The graph
   * @param terminal: The terminal the read aligns to
   * @param terminalMark: The position in the node that aligns to
   *   read_mark
   * @param read: The dna sequence
   * @param readMark: The position in the read that aligns to terminal_mark
   * @return
   */
//  protected GraphPath traceRead(
//      // TODO(jeremy@lewi.us): Add a unittest.
//      IndexedGraph graph, EdgeTerminal terminal, int terminalMark,
//      Sequence read, int readMark, int K) {
//    GraphNode node = new GraphNode(graph.lookup(terminal.nodeId));
//
//    GraphPath path = new GraphPath();
//    path.add(terminal, node);
//
//    int readPos = readMark;
//    EdgeDirection direction = EdgeDirection.OUTGOING;
//    while (readPos < read.size()) {
//      // Select from the read the Kmer that should match the outgoing terminal
//      // for the current terminal.
//      Sequence readTarget = read.subSequence(readMark + 1, readMark + 1 + K);
//
//      // Iterate over all edges until we find the edge that matches.
//      for (EdgeTerminal nextTerminal : node.getEdgeTerminals(
//              terminal.strand, direction)) {
//        GraphNode nextNode = graph.lookupNode(nextTerminal.nodeId);
//        Sequence nextSequence = DNAUtil.sequenceToDir(
//            nextNode.getSequence(), nextTerminal.strand);
//
//        // Check if the first K characters of the next node align
//        // with this read.
//        if (!nextSequence.subSequence(0, K).equals(readTarget) {
//          continue;
//        }
//
//      }
//      // If this code is reached then there wasn't an edge that matched.
//      // This could happen if the read had an error in it.
//
//    }
//  }

  /**
   * Find all paths starting at start.
   *
   * The path stops when the graph ends, we hit a cycle or the length exceeds
   * maxLength.
   *
   * @param graph
   * @param start
   * @param direction
   * @param maxLength
   * @return
   */
  protected List<GraphPath> findAllOutgoingPaths(
      IndexedRecords<String, GraphNodeData>  graph, EdgeTerminal start,
      int stopLength, int K) {
    // List of all paths that still need to be processed.
    ArrayList<GraphPath> unprocessed = new ArrayList<GraphPath>();

    // Paths which are complete.
    ArrayList<contrail.graph.GraphPath> complete = new ArrayList<GraphPath>();

    {
      GraphPath startPath = new GraphPath();
      GraphNodeData nodeData = graph.get(start.nodeId);
      startPath.add(start, new GraphNode(nodeData));
      unprocessed.add(startPath);
    }

    while (unprocessed.size() > 0) {
      GraphPath path = unprocessed.remove(unprocessed.size() - 1);

      if (path.length(K) > stopLength) {
        complete.add(path);
        continue;
      }

      GraphNode lastNode;
      EdgeTerminal lastTerminal;

      lastNode = path.lastNode();
      lastTerminal = path.last();


      // If the graph ends add this path.
      if (lastNode.getEdgeTerminals(
          lastTerminal.strand, EdgeDirection.OUTGOING).size() == 0) {
        complete.add(path);
      }

      boolean hasCycle = false;

      // Construct paths by appending all the incoming/outgoing nodes.
      for (EdgeTerminal next : lastNode.getEdgeTerminals(
               lastTerminal.strand, EdgeDirection.OUTGOING)) {
        if (path.contains(next)) {
          // The next terminal is already in the path so it would create
          // a cycle.
          hasCycle = true;
        }
        GraphPath newPath = path.clone();

        newPath.add(next, new GraphNode(graph.get(next.nodeId)));

        unprocessed.add(newPath);
      }

      // TODO(jeremy@lewi.us): If we hit a cycle what should we do?
      if (hasCycle) {
        complete.add(path);
      }
    }

    return complete;
  }

  /**
   * Find all paths starting at start.
   *
   * The path stops when the graph ends, we hit a cycle or the length exceeds
   * maxLength.
   *
   * @param graph
   * @param start
   * @param direction
   * @param maxLength
   * @return
   */
  protected List<GraphPath> findAllPaths(
      IndexedRecords<String, GraphNodeData> graph, EdgeTerminal start,
      EdgeDirection direction, int stopLength, int K) {
    if (direction == EdgeDirection.OUTGOING) {
      return findAllOutgoingPaths(graph, start, stopLength, K);
    } else {
      // To find incoming edges we flip the start terminal, find the
      // outgoing paths from the flipped path, and then reverse the path.
      List<GraphPath> flippedPaths =
          findAllOutgoingPaths(graph, start.flip(), stopLength, K);


      ArrayList<GraphPath> paths = new ArrayList<GraphPath>();
      for (GraphPath path: flippedPaths) {
        paths.add(path.flip());
      }
      return paths;
    }
  }

  /**
   * Find all paths through the node.
   * @param graph
   * @param start
   * @param stopLength: Stop path in each direction when length exceeds this
   *   value.
   * @param K
   * @return
   */
  public List<GraphPath> findPathsThroughTerminal(
      IndexedGraph graph, EdgeTerminal start,
      int stopLength, int K) {
    GraphNode node =  graph.getNode(start.nodeId);
    List<GraphPath> outPaths = findAllPaths(
        graph, new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD),
        EdgeDirection.OUTGOING, stopLength, K);

    List<GraphPath> inPaths = findAllPaths(
        graph, new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD),
        EdgeDirection.INCOMING, stopLength, K);

    // We now need to join each input path to each output path to get all
    // paths.
    List<GraphPath> allPaths = new ArrayList<GraphPath>();

    for (GraphPath in : inPaths) {
      for (GraphPath out : outPaths) {
        GraphPath path = new GraphPath();
        path.addPath(in);

        // We don't want to add the first node in out because that would
        // cause the node to appear twice.
        path.addPath(out.subPath(1, out.numTerminals()));
        allPaths.add(path);
      }
    }
    return allPaths;
  }

  /**
   * Class to keep track of the alignment of reads to a graph path.
   */
  protected static class PathProfile {
    public GraphPath path;
    public List<ReadAlignment> readAlignments;

    public PathProfile() {
      pathCoordinates = new Coordinates();
      readAlignments = new ArrayList<ReadAlignment>();
    }

    // The alignment of the path to the aligned sequence.
    //public ReadAlignment pathAlignment;

    // TODO(jeremy@lewi.us): Do we really want to treat the path as special?
    // As opposed to just treating it like another read aligned with the other
    // reads?
    public int alignedLength;

    // The alignment of the path to the aligned sequence.
    public Coordinates pathCoordinates;
  }


  protected static class AlignResult {
    public int overlap;
    public int numErrors;
    SequencePair<DNASequence, NucleotideCompound> pair;
  }

  protected AlignResult align(DNASequence query, DNASequence target) {
    AlignResult result = new AlignResult();

    result.pair = Alignments.getPairwiseAlignment(query, target,
        PairwiseSequenceAlignerType.GLOBAL, gapP, matrix);

    Point targetStart = result.pair.getTarget().getStart();
    Point targetEnd = result.pair.getTarget().getEnd();
    Point queryStart = result.pair.getQuery().getStart();
    Point queryEnd = result.pair.getQuery().getEnd();

    result.overlap =
        Math.min(targetEnd.getPosition(), queryEnd.getPosition()) -
        Math.max(targetStart.getPosition(), queryStart.getPosition()) + 1;
    result.numErrors = result.overlap - result.pair.getNumIdenticals();
    return result;
  }

  /**
   * This function performs a pairwise alignment of each read to the sequence.
   *
   * The result is 1 profile for each pairwise alignment.
   *
   * @param sequence
   * @param reads
   * @param readIds
   * @param maxErrorRate
   * @return
   */
  protected ArrayList<PathProfile> alignReadsPairWise(
      Sequence sequence, IndexedRecords<String, FastQRecord> reads,
      List<String> readIds, float maxErrorRate) {
    ArrayList<PathProfile> profiles = new ArrayList<PathProfile>();
    DNASequence targetFWD = new DNASequence(sequence.toString(),
        DNACompoundSet.getDNACompoundSet());

    DNASequence targetRC = new DNASequence(
        DNAUtil.reverseComplement(sequence).toString(),
        DNACompoundSet.getDNACompoundSet());

    // TODO(jlewi): If we wanted to use BioJava's multiple sequence alignment
    // rather than doing pairwise alignments then we would need to know
    // which strand of the reads to align to the query path.
    for (String readId : readIds) {
      FastQRecord record = reads.get(readId);

      DNASequence query = new DNASequence(record.getRead().toString(),
          DNACompoundSet.getDNACompoundSet());

      // Compute global sequence alignments of the read to the forward
      // and reverse strands of the sequence.
      AlignResult alignFWD = align(query, targetFWD);
      AlignResult alignRC = align(query, targetRC);

      // Select the alignment which is optimal
      DNAStrand strand = null;
      AlignResult align = null;

      if (alignFWD.numErrors < alignRC.numErrors) {
        align = alignFWD;
        strand = DNAStrand.FORWARD;
      } else {
        align = alignRC;
        strand = DNAStrand.REVERSE;
      }

      // Get the coordinates of the alignment.
      // BioJava uses 1 based indexing with the range being [start, end]
      // we convert this to 0 based indexing of the form [start, end) to
      // be consistent with how java represents sequences.
      Point targetStart = align.pair.getTarget().getStart();
      Point targetEnd = align.pair.getTarget().getEnd();
      Point queryStart = align.pair.getQuery().getStart();
      Point queryEnd = align.pair.getQuery().getEnd();

      // If we aligned to the reverse strand then we need to flip the positions
      if (strand == DNAStrand.REVERSE) {
        Point temp;
        temp = targetEnd;
        targetEnd = targetStart.reverse(align.pair.getLength());
        targetStart = temp.reverse(align.pair.getLength());

        temp = queryEnd;
        queryEnd = queryStart.reverse(align.pair.getLength());
        queryStart = temp.reverse(align.pair.getLength());
      }

      // We only consider this a a valid alignment if the number of errors
      // is less than the error rate.
      if (align.numErrors >= maxErrorRate * align.overlap) {
        continue;
      }

      PathProfile profile = new PathProfile();

      // The length of the aligned sequences will be the maximum value
      // of the end position of the two sequences.
      profile.alignedLength = align.pair.getLength();

      // We standardize by always making sure the reference sequence
      // is the forward strand of the aligned sequence.
      profile.pathCoordinates.strand = DNAStrand.FORWARD;

      ReadAlignment alignment = new ReadAlignment();
      profile.readAlignments.add(alignment);
      alignment.readId = readId;

      profile.pathCoordinates.start = targetStart.getPosition() - 1;
      profile.pathCoordinates.end = targetEnd.getPosition();

      alignment.coordinates.start = queryStart.getPosition() -1;
      alignment.coordinates.end = queryEnd.getPosition();
      alignment.coordinates.strand = strand;

      profiles.add(profile);
    }
    return profiles;
  }

  protected PathProfile alignReadsToSequence(
      Sequence sequence, IndexedRecords<String, FastQRecord> reads,
      List<String> readIds, float maxErrorRate) {
    ArrayList<PathProfile> profiles = alignReadsPairWise(
        sequence, reads, readIds, maxErrorRate);

    // TODO(jlewi): Combine the profiles into a single profile.
    // We align the pairwise alignments by using the reference/graph path
    // sequence.
    // maxStart is the maximum amount of sequence before the start of the
    // reference sequence.
    int maxStart = 0;

    // maxTail is the maximum amount of sequence extending past the
    // reference sequence.
    int maxTail = 0;
    PathProfile profile = new PathProfile();
    profile.alignedLength = 0;
    for (PathProfile pairProfile : profiles) {
      maxStart = Math.max(maxStart, pairProfile.pathCoordinates.start);

      // To compute the tail
      int tail = pairProfile.readAlignments.get(0).coordinates.end -
          pairProfile.pathCoordinates.end;
      maxTail = Math.max(maxTail, tail);
    }

    profile.alignedLength = maxStart + sequence.size() + maxTail;

    profile.pathCoordinates.start = maxStart;
    profile.pathCoordinates.end = maxStart + sequence.size();

    // Adjust the position of each of the alignments.
    for (PathProfile pairProfile : profiles) {
      ReadAlignment paired = pairProfile.readAlignments.get(0);
      ReadAlignment adjusted = new ReadAlignment();
      adjusted.readId = paired.readId;

      int shift = maxStart - pairProfile.pathCoordinates.start;
      adjusted.coordinates.start = paired.coordinates.start + shift;
      adjusted.coordinates.end = paired.coordinates.end + shift;
      adjusted.coordinates.strand = paired.coordinates.strand;
      profile.readAlignments.add(adjusted);
    }
    // Now we need to add all the alignments
    return profile;
  }

  /**
   * Return all graph paths that are consistent with the reads through
   * the node.
   * @param graph: The graph indexed by node id.
   * @param head: The terminal for the start of the edge under consideration.
   * @param tail: The other terminal for the edge under consideration.
   * @param thread: The name of the read of the thread to consider.
   * @return
   */
  public List<GraphPath> findThreadPaths(
      IndexedGraph graph, IndexedRecords<String, FastQRecord> reads,
      String nodeId, int K) {
    GraphNode node = new GraphNode(graph.get(nodeId));

    // Find all threads associated with edges from this node.
    HashSet<String> threads = new HashSet<String>();
    for (DNAStrand strand : DNAStrand.values()) {
      for (EdgeTerminal terminal : node.getEdgeTerminalsSet(
              strand, EdgeDirection.OUTGOING)) {
        ListUtil.addAllAsStrings(
            threads, node.getTagsForEdge(strand, terminal));
      }
    }

    // Get the maximum length of the reads.
    int maxThreadLength = 0;
    for (String thread : threads) {
      FastQRecord read = reads.get(thread);

      maxThreadLength = Math.max(maxThreadLength, read.getRead().length());
    }

    List<GraphPath> allPaths = findPathsThroughTerminal(
        graph, new EdgeTerminal(nodeId, DNAStrand.FORWARD), maxThreadLength,
        K);

    return allPaths;
    // Construct the sequence corresponding to the K+1 subsequence that
    // gave rise to this read.
//    Sequence startSequence = DNAUtil.sequenceToDir(
//        startNode.getSequence(), head.strand);
//    startSequence = startSequence.subSequence(
//        startSequence.size() - K, startSequence.size());
//
//    Sequence endSequence = DNAUtil.sequenceToDir(
//        endNode.getSequence(), tail.strand);
//    startSequence.add(endSequence.subSequence(K, K + 1));
//
//
//    Sequence edgeSequence = startSequence;

//    // Find all alignments of the read to the K + 1 sequence corresponding
//    // to the edge.
//    FastQRecord read = reads.lookup(thread);
//
//    Pattern edgePattern = Pattern.compile(edgeSequence.toString());
//
//    // We need to consider both strands of the read.
//    for (DNAStrand readStrand : DNAStrand.values()) {
//      Sequence readSequence = new Sequence(
//          read.getRead(), DNAAlphabetFactory.create());
//
//      readSequence = DNAUtil.sequenceToDir(readSequence, readStrand);
//
//      String readSequenceStr = readSequence.toString();
//
//      Matcher matcher = edgePattern.matcher(readSequenceStr);
//      while (matcher.find()) {
//        ReadAlignment alignment = new ReadAlignment();
//        alignment.start = matcher.start();
//        alignment.end = matcher.end();
//
//        // Now we want to follow the sequence through the outgoing edges.
//        GraphPath outTerminals = traceRead(
//            graph, head, readSequence, alignment.start);
//      }
//      readSequenceStr.m
//    }
  }

  /**
   * This function returns information about which reads if any span a
   * node.
   *
   * @return:
   */
  public Set<String> findSpanningReads(GraphNode node) {
    // Get a list of reads for incoming edges.
    HashSet<String> inReads = new HashSet<String>();
    HashSet<String> outReads = new HashSet<String>();

    // Get outgoing edges for the forward strand.
    for (EdgeTerminal terminal : node.getEdgeTerminalsSet(
        DNAStrand.FORWARD, EdgeDirection.OUTGOING)) {
      ListUtil.addAllAsStrings(
          outReads, node.getTagsForEdge(DNAStrand.FORWARD, terminal));
    }

    // To get reads for incoming edges we look at outgoing edges for the
    // reverse strand. getTagsForEdge assumes the edge is an outgoing edge.
    for (EdgeTerminal terminal : node.getEdgeTerminalsSet(
        DNAStrand.REVERSE, EdgeDirection.OUTGOING)) {
      ListUtil.addAllAsStrings(
          inReads, node.getTagsForEdge(DNAStrand.REVERSE, terminal));
    }

    // Compute the intersection.
    inReads.retainAll(outReads);

    return inReads;
  }  
}
