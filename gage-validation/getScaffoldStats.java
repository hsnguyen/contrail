import java.io.BufferedReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class getScaffoldStats {
  private static final int GAP_FUDGE_FACTOR = 1000;
  private static final int MIN_SKIP_SIZE = 200;
  private static final char GAP_CHAR = 'N';
  private static final char GAP_CHAR_LOWER = 'n';
  private static final NumberFormat nf = new DecimalFormat("############.#");

  private static final String[] extensions = {"final", "fa", "fasta", "scafSeq"};
  private int genomeSize = 0;
  private HashMap<String, Scaffold> scfs = new HashMap<String, Scaffold>();

  // Hash map from the ID of a scaffold to the longest region in that
  // scaffold that aligns to the reference.
  private HashMap<String, Utils.Pair> sizes = new HashMap<String, Utils.Pair>();

  /**
   * Class to represent a scaffold.
   */
  private static class Scaffold {
    // gaps is a hash map of gap name to gap length.
    // The name is <SCAFFOLD_NAME>_i where i is the index of the gap.
    private HashMap<String, Integer> gaps = new HashMap<String, Integer>();
    // lengths[i] = Length of the i'th contig.
    // contigs[i] = Name of the i'th contig. This will be <SCAFFOLD_NAME>_i
    //   where <SCAFFOLD_NAME> is the id of the scaffold from which the contig
    //   came. The contigs result from splitting the scaffold at the gaps.
    // tiles[i] = Is a tuple (start, end) of the start and end coordinates of
    //   the i'th contig.
    private HashMap<String, Integer> lengths = new HashMap<String, Integer>();

    private ArrayList<String> contigs = new ArrayList<String>();
    private HashMap<String, Utils.Pair> tiles = new HashMap<String, Utils.Pair>();
  }

  /**
   * Compute the scaffold statistics.
   *
   * For each scaffold (query sequence) we find the longest sub region
   * from the query sequence that aligns to the reference genome. The
   * coordinates of the region are stored in sizes.
   *
   * @param coords: The path to the .coords file produced by the mummer
   *   program show-coords.
   *   http://mummer.sourceforge.net/manual/#coords
   *   The expected columns are:
   *   [S1] [E1] [S2] [E2] [LEN 1] [LEN 2] [% IDY] [LEN R] [LEN Q] [COV R] [COV Q] [TAGS]
   * @throws Exception
   */
  public getScaffoldStats(String coords) throws Exception {
    BufferedReader bf = Utils.getFile(coords, "coords");

    String line = null;
    StringBuffer fastaSeq = new StringBuffer();
    String header = "";

    // lastStart and last keep track of the start and end coordinates in
    // the query sequence of the last alignment.
    int last = 0;
    int lastStart = 0;
    String currID = "";
    int currStart = 0;

    // The columns containing various pieces of data.
    final int REFERENCE_START = 0; // Start of the region in the reference.
    final int REFERENCE_END = 1; // End of the region in the reference.
    final int QUERY_LENGTH = 8; // Length of the region in the query reference.

    while ((line = bf.readLine()) != null) {
      String[] split = line.trim().split("\\s+");

      try {
        // Start of the region in the reference sequence.
        int start = Integer.parseInt(split[REFERENCE_START]);
        String queryID =  split[split.length-1];

        // We only consider this alignment if:
        // 1) It is from a different scaffold (query sequence).
        // or
        // 2) There is a gap of size at least M between the end of the previous
        //    alignment from this scaffold and the start of this alignment.
        if (start - last > 0.1 * Integer.parseInt(split[QUERY_LENGTH]) ||
            !currID.equalsIgnoreCase(queryID)) {
          // We are looking at a different scaffold from the previous one.
          if (!currID.equals("")) {
            Utils.Pair pair = new Utils.Pair(currStart, last);
            // We keep track of the longest region in each scaffold that aligns
            // to the reference.
            if (sizes.get(currID) == null ||
                sizes.get(currID).size() < pair.size()) {
              sizes.put(currID, pair);
            }
          }
          currID = queryID;
          currStart = start;
        }

        int end = Integer.parseInt(split[REFERENCE_END]);

        if (start >= lastStart && end <= last) { continue; }
        if (Math.abs(last - start) >
            0.1 * Integer.parseInt(split[QUERY_LENGTH])) {
          currStart = start;
        }
        last = end;
        lastStart = start;
      } catch (Exception e) {
        // The first lines of the .coords file are a header. We skip these
        // lines by relying on Integer.parseInt to throw an exception since
        // the first record in the line won't contain an integer.
        System.err.println("Skipping line " + line);
      }
    }
    Utils.Pair pair = new Utils.Pair(currStart, last);
    if (sizes.get(currID) == null || sizes.get(currID).size() < pair.size()) {
      sizes.put(currID, pair);
    }
    for (String s : sizes.keySet()) {
      System.err.println("Size of " + s + " is " + (sizes.get(s).second - sizes.get(s).first));
    }
  }

  /**
   * Turn each FASTA record into a Scaffold object.
   *
   * @param header
   * @param fastaSeq
   */
  private void storeScaffold(String header, String fastaSeq) {
    Scaffold scf = new Scaffold();

    // The index is used to number the gaps sequentially using zero based
    // indexing.
    int index = 0;
    // offset is the start of the last contig.
    int offset = 0;
    String name = null;
    int gapSize = 0;
    for (int i = 0; i < fastaSeq.length(); i++) {
      if (fastaSeq.charAt(i) == GAP_CHAR || fastaSeq.charAt(i) == GAP_CHAR_LOWER) {
        gapSize++;
        if (scf.contigs.size() <= index) {
          //System.err.println("String contig " + name + " and index is " + index + " and offset is " + offset + " and i is " + i);
          // Since we hit a gap character, store the most recent contig.
          // i: Start of the gap.
          // offset: Start of the contig.
          int length = i - offset;
          Utils.Pair p = new Utils.Pair(offset, i);
          scf.contigs.add(name);
          scf.lengths.put(name, length);
          scf.tiles.put(name, p);
        }
      } else {
        if (gapSize != 0) {
          //System.err.println("Adding gap " + name + " of size " + gapSize);
          scf.gaps.put(name, gapSize);
          index++;
          offset = i;
        }
        gapSize = 0;
        name = header+"_"+index;
      }
    }
    if (scf.contigs.size() <= index) {
      //System.err.println("String contig " + name + " and index is " + index + " and offset is " + offset + " and i is " + fastaSeq.length());
      int length = fastaSeq.length() - offset;
      Utils.Pair p = new Utils.Pair(offset, fastaSeq.length());
      scf.contigs.add(name);
      scf.lengths.put(name, length);
      scf.tiles.put(name, p);
    }

    System.err.println("Storing scaffold " + header);
    scfs.put(header, scf);

    for (String s : scf.contigs) {
      System.err.println(
          "Scf has contig " + s  + " of length " + scf.lengths.get(s) +
          " and gap after it is " + scf.gaps.get(s) + " and tile is " +
          scf.tiles.get(s));
    }
  }

  /**
   * Parses a fastaFile into Scaffold records.
   *
   * @param inputFile: This should be the fasta file for the scaffolds. The
   *   scaffolds can and should contain gaps.
   * @throws Exception
   */
  public void processFasta(String inputFile) throws Exception {
    BufferedReader bf = Utils.getFile(inputFile, extensions);

    String line = null;
    StringBuffer fastaSeq = new StringBuffer();
    String header = "";

    while ((line = bf.readLine()) != null) {
      if (line.startsWith(">")) {
        if (fastaSeq.length() != 0) storeScaffold(header, fastaSeq.toString());
        header = line.split("\\s+")[0].substring(1);
        fastaSeq = new StringBuffer();
      }
      else {
        fastaSeq.append(line);
      }
    }

    if (fastaSeq.length() != 0) { storeScaffold(header, fastaSeq.toString());
    }
    bf.close();
  }

  /**
   *
   * @param tileFile: The tile file produced by the mummer program show-tiling.
   *   See http://mummer.sourceforge.net/manual/#tiling
   * @throws Exception
   */
  public void checkAgainstTiling(String tileFile) throws Exception {
    BufferedReader bf = Utils.getFile(tileFile, "tiling");

    String line = null;
    StringBuffer fastaSeq = new StringBuffer();
    String header = "";

    int count = 0;
    int gaps = 0;
    int scfGaps = 0;
    String currScf = null;
    char currOri = ' ';
    String currName = null;
    int lastIndex = 0;
    int lastOffset = 0;
    int lastEnd = 0;
    int start = 0;
    int totalLength = 0;
    int numIndels = 0;
    int numInversion = 0;
    int numTranslocation = 0;
    ArrayList<Integer> lengths = new ArrayList<Integer>();

    int inCurrScf = 0;
    String startScf = null;
    int startOffset = 0;
    char startOri = ' ';
    int startIndex = -1;
    int startGaps = 0;
    int numBadGaps = 0;
    int numGaps = 0;
    int badGapSize = 0;

    HashMap<String, String> lastChr = new HashMap<String, String>();
    HashMap<String, Character> lastOri = new HashMap<String, Character>();
    HashMap<String, Integer> lastPos = new HashMap<String, Integer>();
    HashMap<String, Integer> lastStart = new HashMap<String, Integer>();

    while ((line = bf.readLine()) != null) {
      if (line.startsWith(">")) {
        // close last segment
        if (count > 0) {
          if (currName.equalsIgnoreCase(startScf) && currOri == startOri && Math.abs(lastIndex - startOffset) == 1) {
            System.err.println("Looped a circle " +  lastIndex + " " + startOffset + " "+  startScf + " "  +  currName);
            if (inCurrScf == 1) { numIndels--; } // we looped a circle
            System.err.println("Looped a circle replacing length " + startIndex);
            lengths.add(totalLength + scfGaps + startGaps + lengths.get(startIndex));
            lengths.remove(startIndex);
          } else {
            lengths.add(totalLength + scfGaps);
          }
        }
        System.err.println("Starting a new range in " + currScf + " on scf " + currName + "  done with one from " + (lastEnd) + " to " + start);
        currScf = line.replaceAll(">", "");
        currOri = ' ';
        currName = null;
        count = start = gaps = scfGaps = totalLength = 0;
        startScf = null;
        startOffset = 0;
        startGaps = 0;
        inCurrScf = 0;
        lastPos.clear();
        lastStart.clear();
        continue;
      }

      String[] splitLine = line.trim().split("\\s+");
      String scfName = splitLine[splitLine.length - 1];
      char ori = (splitLine[splitLine.length - 2].equalsIgnoreCase("+") ? '+' : '-');
      // We assume the sequences are the result of splitting the scaffolds
      // at the gaps using SplitFastaByLaetter. This will assign each ungapped
      // subsequence an id of <ID>_# where ID was the original id and # is
      // a counter which starts at 0 for each scaffold.
      int underscore = scfName.trim().lastIndexOf("_");
      if (underscore == -1) {
        // TODO(jeremy@lewi.us): Its possible if a scaffold didn't have any
        // gaps then its name wouldn't have an underscore; in which case
        // we probably don't want to treat that as an error.
        System.err.println("Error unexpected name! " + scfName);
        System.exit(1);
      }
      String scfID = scfName.trim().substring(0, underscore);
      Integer index = Integer.parseInt(scfName.trim().substring(underscore+1));
      System.err.println("Parsing " + index + " and scf " + scfID);

      // The length of this contig.
      Integer length = Integer.parseInt(splitLine[3]);

      // offset is the start in the reference of this alignment.
      Integer offset = Integer.parseInt(splitLine[0]);

      // Get the data for the original scaffold which had gaps.
      Scaffold scf = scfs.get(scfID);
      String name = scfID + "_" + index;
      System.err.println("Processing " + line);
      if (!scf.lengths.get(name).equals(length)) {
        System.err.println("Error for scf " + scfName + " length " + length + " doesnt match " + scf.lengths.get(name));
        System.exit(1);
      }
      if (scf.contigs.size() == 1) {
        // Since this scaffold conists of a single contig just add the length
        // of the contig to lengths.
        lengths.add(length);
        continue;
      }

      if (startScf == null) {
        startScf = currName;
        startOri = currOri;
        startOffset = lastIndex;
      }

      if (currOri == ' ' && currName == null) {
        currOri = ori;
        currName = scfID;
        start = offset;
        count = gaps = scfGaps = lastOffset = totalLength = 0;
      } else {
        System.err.println(
            "Processing scf " + currName + " with ori " + currOri +
            " and offset " + lastOffset + " and info on this is " + scfID +
            " and ori " + ori + " and offset " + offset + " and math is " +
            Math.abs(index-lastIndex) + " and gaps is " + gaps +
            " and scf is " + scfGaps);
        boolean badSkip = false;
        int totalSize = 0;
        int totalOtherSize = 0;
        if (Math.abs(index - lastIndex) > 1 && currName.equalsIgnoreCase(scfID)) {
          System.err.println("Processing skip in " + scfID + " from " + index + " to " + lastIndex);
          for (int myIndex = Math.min(index, lastIndex)+1; myIndex < Math.max(index, lastIndex); myIndex++) {
            if (scfs.get(scfID).lengths.get(scfID + "_" + myIndex) > MIN_SKIP_SIZE) {
              badSkip = true;
            }
            totalSize += scfs.get(scfID).lengths.get(scfID + "_" + myIndex);
            totalOtherSize = (sizes.get(scfID + "_" + myIndex) == null ? -1 : totalOtherSize + sizes.get(scfID + "_" + myIndex).size());
          }

          int dist = offset - lastPos.get(currName); //lastEnd;
          if (Math.abs(dist - totalSize) < GAP_FUDGE_FACTOR || (totalOtherSize > 0 && Math.abs(dist - totalOtherSize) < GAP_FUDGE_FACTOR)) {
            badSkip = false;
          } else {
            badSkip = true;
          }
          System.err.println("The skip avove had dist " + dist + " versus " + totalSize + " which is OK? " + badSkip);
        }

        int gapDifference = -1;
        int otherDiff = Integer.MAX_VALUE;
        if (lastPos.get(scfID) != null) {
          int dist = offset - lastPos.get(scfID);
          Integer lastGap = null;
          if (index > lastIndex) {
            lastGap = scfs.get(currName).gaps.get(currName + "_" + lastIndex);
          } else {
            lastGap = scfs.get(currName).gaps.get(currName + "_" + index);
          }
          gapDifference = (lastGap == null ? -1 : Math.abs(dist - lastGap));
          System.err.println("The sizes is " + scfID + " and sizes " + sizes.get(scfID + "_" + lastIndex));
          Utils.Pair trueSize = sizes.get(scfID + "_" + lastIndex);
          Utils.Pair secondTrueSize = sizes.get(scfID + "_" + index);
          int otherDist = (trueSize == null || secondTrueSize == null ? -1 : Math.abs(secondTrueSize.first - (int)trueSize.second));
          otherDiff = (otherDist == -1 || lastGap == null ? Integer.MAX_VALUE : Math.abs(otherDist - lastGap));

          System.err.println("Between " + currName + "_" + lastIndex + " and " + index +" expected gap " + dist + " and real gap is " + lastGap + " and estimate gap is " + gapDifference + " and other gap is " + otherDist + " " + otherDiff);
        }

        if (badSkip || currOri != ori || !currName.equalsIgnoreCase(scfID)) {
          if ((currOri != ori && currName.equalsIgnoreCase(scfID)) || (lastOri.get(scfID) != null && lastOri.get(scfID) != ori && lastChr.get(scfID).equalsIgnoreCase(currScf))) {
            System.err.println("Inversion found in scf " + scfID);
            numInversion++;
          } else if (lastChr.get(scfID) != null && !(lastChr.get(scfID).equalsIgnoreCase(currScf))) {
            System.err.println("Translocation found in scf " + scfID);
            numTranslocation++;
          }
          else if (Math.abs(index - lastIndex) > 1 && currName.equalsIgnoreCase(scfID)) {
            System.err.println("Insertion in scf " + scfID);
            numIndels++;
            inCurrScf++;
          }
          if (count > 0) {
            if (startIndex < 0) { startIndex = lengths.size(); }
            Integer tmpGap = scfs.get(scfID).gaps.get(scfID + "_" + index);
            startGaps = (tmpGap == null ? 0 : tmpGap);
            lengths.add(totalLength + scfGaps);
            //lengths.add(lastEnd-start+1);
          }
          System.err.println("Starting a new range in " + currScf + " on scf " + currName + "  done with one from " + (lastEnd) + " to " + start);
          currOri = ori;
          currName = scfID;
          start = offset;
          gaps = scfGaps = lastOffset = totalLength = 0;
        }
        else {
          if (gapDifference >= 0 && currName.equalsIgnoreCase(scfID)) {
            if (gapDifference > GAP_FUDGE_FACTOR && otherDiff > GAP_FUDGE_FACTOR) numBadGaps++;
            badGapSize+=Math.min(otherDiff, gapDifference);
            numGaps++;
            System.err.println("Difference is " + Math.min(otherDiff, gapDifference));
          }
        }
      }
      if (scfs.get(currName) != null && index < scfs.get(currName).contigs.size()-1 && index > 0) {
        scfGaps += scfs.get(currName).gaps.get(currName + "_" + index);
      }
      lastIndex = index;
      gaps += (lastOffset != 0 ? offset - lastEnd : gaps);
      lastOffset = offset;
      lastEnd = offset + length;
      totalLength += length;
      lastOri.put(scfID, ori);
      lastPos.put(scfID, lastEnd);
      lastStart.put(scfID, lastOffset);
      lastOri.put(scfID, ori);
      lastChr.put(scfID, currScf);
      System.err.println("After processing " + scfID + "_" + index + " the length is " + totalLength + " and gaps is " + scfGaps);
      count++;
    }

    if (count != 0) {
      if (startScf.equalsIgnoreCase(currName) && currOri == startOri && Math.abs(lastIndex - startOffset) == 1)
        //if (currName.equalsIgnoreCase(startScf) && currOri == startOri && Math.abs(lastIndex - startOffset) == 1)
      {
        // we looped a circle
        System.err.println("Looped a circle replacing length " + startIndex);
        lengths.add(totalLength + scfGaps + lengths.get(startIndex));
        lengths.remove(startIndex);
      } else {
        lengths.add(totalLength + scfGaps);
      }
    }
    System.err.println("Final Processing scf " + currName + " with ori " + currOri + " and offset " + lastIndex + " and info on this is " + " and gaps is " + gaps + " and scf is " + scfGaps + " length " + lastEnd + " to " + start);

    bf.close();

    // now we can compute all the stats.
    System.out.println("Num inDels: " + numIndels);
    System.out.println("Num Inversions: " + numInversion);
    System.out.println("Num Translocation: " + numTranslocation);
    System.out.println("Num BadGaps: " + numBadGaps);
    System.out.println("Average bad gap size: " + (double)badGapSize / numGaps);

    Collections.sort(lengths);
    Collections.reverse(lengths);

    int sum = 0;
    System.out.println("Longest Scaffold: " + lengths.get(0));
    for (int i = 0; i < lengths.size(); i++) {
      System.err.println("At index " + i + " I have length " + lengths.get(i));
      sum += lengths.get(i);

      if (sum / (double)genomeSize >= 0.5) {
        int n50Count = i + 1;
        System.out.println(
            "N50 Length: " + lengths.get(i) + " Count: " + n50Count);
        break;
      }
    }
  }

  public static void printUsage() {
    System.err.println(
        "This program sizes a fasta or fastq file. Multiple fasta files can " +
        "be supplied by using a comma-separated list.");
    System.err.println(
        "Example usage: getScaffoldStats <FASTA FILES> <TILING> <GENOME SIZE>" +
        " <COORDS FILE>");
    System.err.println(
        "Example usage: getScaffoldStats fasta1.fasta,fasta2.fasta");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) { printUsage(); System.exit(1);}

    getScaffoldStats f = new getScaffoldStats(args[3]);
    f.genomeSize = Integer.parseInt(args[2]);
    f.processFasta(args[0]);

    f.checkAgainstTiling(args[1]);
  }
}
