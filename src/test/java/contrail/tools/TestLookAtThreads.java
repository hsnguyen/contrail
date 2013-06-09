package contrail.tools;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphPath;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.graph.IndexedGraph;
import contrail.io.IndexedRecordsInMemory;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.FastQRecord;
import contrail.sequences.Sequence;
import contrail.tools.LookAtThreads;

import org.biojava3.alignment.template.AlignedSequence;
import org.biojava3.core.sequence.DNASequence;
import org.biojava3.core.sequence.compound.NucleotideCompound;
public class TestLookAtThreads extends LookAtThreads{
  /**
   * Test that find all paths works
   */
  @Test
  public void testFindAllPaths() {
    // We construct the graph {A,B}->Z->{D,E}
    // This graph has 4 paths.
    // A->Z->D
    // A->Z->E
    // B->Z->D
    // B->Z->E

    int K = 4;
    GraphNode headA = GraphTestUtil.createNode("A", "ACTG");
    GraphNode headB = GraphTestUtil.createNode("B", "CCTG");
    GraphNode middle = GraphTestUtil.createNode("Z", "CTGA");
    GraphNode tailD = GraphTestUtil.createNode("D", "TGAA");
    GraphNode tailE = GraphTestUtil.createNode("E", "TGAT");

    GraphUtil.addBidirectionalEdge(
        headA, DNAStrand.FORWARD,  middle, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        headB, DNAStrand.FORWARD,  middle, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        middle, DNAStrand.FORWARD,  tailD, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        middle, DNAStrand.FORWARD, tailE, DNAStrand.FORWARD);

    IndexedRecordsInMemory<String, GraphNodeData> graph =
        new IndexedRecordsInMemory<String, GraphNodeData>();
    graph.put(headA.getNodeId(), headA.getData());
    graph.put(headB.getNodeId(), headB.getData());
    graph.put(middle.getNodeId(), middle.getData());
    graph.put(tailD.getNodeId(), tailD.getData());
    graph.put(tailE.getNodeId(), tailE.getData());

    IndexedGraph indexedGraph = new IndexedGraph(graph);

    int stopLength = 100;
    List<GraphPath> paths = findPathsThroughTerminal(
        indexedGraph, new EdgeTerminal(middle.getNodeId(), DNAStrand.FORWARD),
        stopLength, K);

    HashSet<GraphPath> expected = new HashSet<GraphPath>();
    HashSet<GraphPath> actual = new HashSet<GraphPath>();

    for (GraphNode head : new GraphNode[] {headA, headB}) {
      for (GraphNode tail : new GraphNode[] {tailD, tailE}) {
        GraphPath path = new GraphPath();
        path.add(new EdgeTerminal(head.getNodeId(), DNAStrand.FORWARD), head);
        path.add(
            new EdgeTerminal(middle.getNodeId(), DNAStrand.FORWARD), middle);
        path.add(new EdgeTerminal(tail.getNodeId(), DNAStrand.FORWARD), tail);
        expected.add(path);
      }
    }

    actual.addAll(paths);
    assertEquals(expected, actual);
  }

  private void printAlignment(String name, AlignedSequence<DNASequence, NucleotideCompound> seq ) {
    String out = String.format(
        "%s: start: %d",
        name, seq.getStart().getPosition());
    System.out.println(out);
    System.out.printf("%s: end:%d \n", name, seq.getEnd().getPosition());
  }

  @Test
  public void testAlignReadsPairWise() {
    Sequence sequence = new Sequence("ACTCG", DNAAlphabetFactory.create());

    IndexedRecordsInMemory<String, FastQRecord> reads =
        new IndexedRecordsInMemory<String, FastQRecord>();

    FastQRecord read = new FastQRecord();
    read.setId("read");
    read.setRead("TCGT");
    reads.put(read.getId().toString(), read);

    List<String> readIds = Arrays.asList(read.getId().toString());

    float maxErrorRate = .1F;
    List<PathProfile> profiles = alignReadsPairWise(
        sequence, reads, readIds, maxErrorRate);

    assertEquals(1, profiles.size());

    PathProfile profile = profiles.get(0);
    assertEquals(6, profile.alignedLength);
    assertEquals(0, profile.pathCoordinates.start);
    assertEquals(5, profile.pathCoordinates.end);

    assertEquals(1, profile.readAlignments.size());
    ReadAlignment alignment = profile.readAlignments.get(0);
    assertEquals(DNAStrand.FORWARD, alignment.coordinates.strand);
    assertEquals(2, alignment.coordinates.start);
    assertEquals(6, alignment.coordinates.end);
  }

  @Test
  public void testAlignReadsPairWiseRC() {
    // Test that pairwise alignment works when the reverse complement
    // of the target is optimally aligned.
    Sequence sequence = new Sequence("ACTCG", DNAAlphabetFactory.create());

    IndexedRecordsInMemory<String, FastQRecord> reads =
        new IndexedRecordsInMemory<String, FastQRecord>();

    FastQRecord read = new FastQRecord();
    read.setId("read");
    // RC(TCGT) = ACGA
    read.setRead("ACGA");
    reads.put(read.getId().toString(), read);

    List<String> readIds = Arrays.asList(read.getId().toString());

    float maxErrorRate = .1F;
    List<PathProfile> profiles = alignReadsPairWise(
        sequence, reads, readIds, maxErrorRate);

    assertEquals(1, profiles.size());

    PathProfile profile = profiles.get(0);
    assertEquals(6, profile.alignedLength);
    assertEquals(0, profile.pathCoordinates.start);
    assertEquals(5, profile.pathCoordinates.end);

    assertEquals(1, profile.readAlignments.size());
    ReadAlignment alignment = profile.readAlignments.get(0);
    assertEquals(DNAStrand.REVERSE, alignment.coordinates.strand);
    assertEquals(2, alignment.coordinates.start);
    assertEquals(6, alignment.coordinates.end);
  }

  @Test
  public void testAlignReadsToSequence() {
    // Test that pairwise alignment works when the reverse complement
    // of the target is optimally aligned.
    Sequence sequence = new Sequence("ACTCG", DNAAlphabetFactory.create());

    IndexedRecordsInMemory<String, FastQRecord> reads =
        new IndexedRecordsInMemory<String, FastQRecord>();

    FastQRecord head = new FastQRecord();
    head.setId("head");
    head.setRead("CACT");
    reads.put(head.getId().toString(), head);

    FastQRecord tail = new FastQRecord();
    tail.setId("tail");
    tail.setRead("TCGTT");
    reads.put(tail.getId().toString(), tail);

    List<String> readIds = Arrays.asList(
        head.getId().toString(), tail.getId().toString());

    float maxErrorRate = .1F;

    PathProfile profile =  alignReadsToSequence(
        sequence, reads, readIds, maxErrorRate);

    assertEquals(2, profile.readAlignments.size());
    assertEquals(8, profile.alignedLength);

    assertEquals(1, profile.pathCoordinates.start);
    assertEquals(6, profile.pathCoordinates.end);

    ReadAlignment alignment = profile.readAlignments.get(0);
    assertEquals(head.getId().toString(), alignment.readId);
    assertEquals(0, alignment.coordinates.start);
    assertEquals(4, alignment.coordinates.end);
    assertEquals(DNAStrand.FORWARD, alignment.coordinates.strand);

    alignment = profile.readAlignments.get(1);
    assertEquals(tail.getId().toString(), alignment.readId);
    assertEquals(3, alignment.coordinates.start);
    assertEquals(8, alignment.coordinates.end);
    assertEquals(DNAStrand.FORWARD, alignment.coordinates.strand);
  }


//  @Test
//  public void testAlignment() {
//    // When where query is longer target position is position in query
//    String querySeq = "ACTCG";
//    String targetSeq = "TCGT";
//
//    DNASequence target = new DNASequence(targetSeq,
//        AmbiguityDNACompoundSet.getDNACompoundSet());
//    DNASequence query = new DNASequence(querySeq,
//        AmbiguityDNACompoundSet.getDNACompoundSet());
//
//    // Nuc 4_4 assigns a penalty of -4 to any pair of non identical bases
//    // and a bonus of 5 to a match.
//    SubstitutionMatrix<NucleotideCompound> matrix = SubstitutionMatrixHelper.getNuc4_4();
//
//    // We give very high penalties to insertion and deletion because
//    // we don't want to consider insertions and deletions.
//    SimpleGapPenalty gapP = new SimpleGapPenalty();
//    gapP.setOpenPenalty((short)5);
//    gapP.setExtensionPenalty((short)5);
//
//    SequencePair<DNASequence, NucleotideCompound> psa =
//        Alignments.getPairwiseAlignment(query, target,
//            PairwiseSequenceAlignerType.GLOBAL, gapP, matrix);
//
//    printAlignment("Query", psa.getQuery());
//    printAlignment("Target", psa.getTarget());
//
//
//    //psa.
//    System.out.println("Num identicals:" + Integer.toString(psa.getNumIdenticals()));
//    System.out.println(psa);
//  }
//
//
//  @Test
//  public void testMultiAlignment() {
//    List<DNASequence> sequences = new ArrayList<DNASequence>();
//    sequences.add(new DNASequence("ACTCGG",
//        AmbiguityDNACompoundSet.getDNACompoundSet()));
//    sequences.add(new DNASequence("CTCGGT",
//        AmbiguityDNACompoundSet.getDNACompoundSet()));
//    sequences.add(new DNASequence("TCGGTA",
//        AmbiguityDNACompoundSet.getDNACompoundSet()));
//    sequences.add(new DNASequence("CCGAGT",
//        AmbiguityDNACompoundSet.getDNACompoundSet()));
//    sequences.add(new DNASequence("CGAGTT",
//        AmbiguityDNACompoundSet.getDNACompoundSet()));
//    Profile<DNASequence, NucleotideCompound> profile =
//        Alignments.getMultipleSequenceAlignment(sequences);
//    System.out.printf("Clustalw:%n%s%n", profile);
//    ConcurrencyTools.shutdown();
//
//  }

  @Test
  public void testFindSpanningReads() {
    GraphNode node = GraphTestUtil.createNode("node", "ACGGT");

    int maxThreads = 100;
    // Add two incoming edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in2", DNAStrand.FORWARD),
        Arrays.asList("read2"), maxThreads);

    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);

    Set<String> spanning = findSpanningReads(node);

    HashSet<String> expected = new HashSet<String>();
    expected.add("read1");
    assertEquals(expected, spanning);
  }
}
