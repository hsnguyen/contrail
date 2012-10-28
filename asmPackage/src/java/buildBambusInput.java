import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class buildBambusInput {
  private static class MappingInfo {
    public String readID = null;

    // position on the read
    int start = 0;
    int end = 0;

    // position on the contig
    int contigStart = 0;
    int contigEnd = 0;
  }

  private static final int SUB_LEN = 25;
  private static final int NUM_READS_PER_CTG = 200;
  private static final int NUM_CTGS = 200000;

  private static void readBowtieResults(String fileName, String prefix, HashMap<String, ArrayList<MappingInfo>> map) throws Exception {
    prefix = prefix.replaceAll("X.*", "");
    System.err.println("For file " + fileName + " prefix is " + prefix);
    BufferedReader bf = Utils.getFile(fileName, "bout");
    if (bf != null) {
      String line = null;
      int counter = 0;
      while ((line = bf.readLine()) != null) {
        if (counter % 1000000 == 0) {
          System.err.println("Read " + counter + " mapping records from " + fileName);
        }
        String[] splitLine = line.trim().split("\\s+");
        MappingInfo m = new MappingInfo();

        int position = 1;
        // skip crud
        while (!splitLine[position].equalsIgnoreCase("+") && !splitLine[position].equalsIgnoreCase("-")) {
          position++;
        }

        String contigID = splitLine[position+1];
        Boolean isFwd = null;
        if (splitLine[position].contains("-")) {
          isFwd = false;
        } else if (splitLine[position].contains("+")) {
          isFwd = true;
        }

        m.readID = prefix + splitLine[0].replaceAll("/", "_");
        m.start = 1;
        m.end = SUB_LEN;
        m.contigStart = Integer.parseInt(splitLine[position+2]);
        if (isFwd) {
          m.contigEnd = m.contigStart + splitLine[position+3].length() - 1;
        } else {
          m.contigEnd = m.contigStart;
          m.contigStart = m.contigEnd + splitLine[position+3].length() - 1;
        }

        if (map.get(contigID) == null) {
          map.put(contigID, new ArrayList<MappingInfo>(NUM_READS_PER_CTG));
        }
        map.get(contigID).add(m);
        counter++;
      }
      bf.close();
    }
  }

  private static void outputContigRecord(PrintStream out, String contigID, String sequence, ArrayList<MappingInfo> reads) {
    out.println("##" + contigID + " " + (reads == null ? 0 : reads.size()) + " 0 bases 00000000 checksum.");
    out.print(sequence);
    if (reads != null) {
      for (MappingInfo m : reads) {
        out.println("#" + m.readID + "(" + Math.min(m.contigEnd, m.contigStart) + ") " + (m.contigEnd >= m.contigStart ? "[]" : "[RC]") + " " + (m.end - m.start + 1) + " bases, 00000000 checksum. {" + " " + (m.contigEnd >= m.contigStart ? m.start + " " + m.end : m.end + " " + m.start) + "} <" + (m.contigEnd >= m.contigStart ? (m.contigStart+1) + " " + (m.contigEnd+1) : (m.contigEnd+1) + " " + (m.contigStart+1)) + ">");
      }
    }
  }
  private static boolean containsPrefix(HashSet<String> prefix, String name, String postfix) {
    boolean contains = false;

    for (String s : prefix) {
      System.err.println("Checking for " + s.replaceAll("X", ".") + "." + postfix);
      if (name.matches(s.replaceAll("X", ".") + "." + postfix)) {
        contains = true;
        break;
      }
    }
    return contains;
  }

  public static void main(String[] args) throws Exception {
    String resultDir = System.getProperty("user.dir") + "/";
    if (args.length < 3) {
      System.err.println("Please provide an asm and read directory");
      System.exit(1);
    }

    String execPath = buildBambusInput.class.getClassLoader().getResource(buildBambusInput.class.getName().replace('.', File.separatorChar) + ".class").getPath();
    String perlCommand = new File(execPath).getParent() + File.separatorChar + "get_singles.pl";

    String asmDir = args[0];
    String readDir = args[1];
    String suffix = args[2];
    String libFile = args[3];
    HashMap<String, Utils.Pair> libSizes = new HashMap<String, Utils.Pair>();
    BufferedReader libSizeFile = Utils.getFile(libFile, "libSize");
    String libLine = null;
    while ((libLine = libSizeFile.readLine()) != null) {
      String[] splitLine = libLine.trim().split("\\s+");
      libSizes.put(splitLine[0], new Utils.Pair(Integer.parseInt(splitLine[1]), Integer.parseInt(splitLine[2])));
    }
    libSizeFile.close();

    String outPrefix = null;
    File dir = new File(asmDir);
    if (!dir.isDirectory()) {
      System.err.println("Error, asm directory " + asmDir + " is not a directory");
      System.exit(1);
    }

    for (File fs : dir.listFiles()) {
      if (fs.getName().contains(suffix)) {
        outPrefix = fs.getName().replaceAll("suffix", "");
      }
    }

    dir = new File(readDir);
    if (!dir.isDirectory()) {
      System.err.println("Error, read directory " + readDir + " is not a directory");
      System.exit(1);
    }

    File[] files = dir.listFiles();
    HashSet<String> prefixes = new HashSet<String>();

    for (File fs : files) {
      if (fs.getName().contains("SRR")) { continue; }
      if (fs.getName().matches(".*_[12]\\..*fastq.*")) {
        prefixes.add(fs.getName().replaceAll("\\.fastq", "").replaceAll("\\.bz2", "").replaceAll("1\\.", "X.").replaceAll("2\\.", "X.").replaceAll("1$", "X").replaceAll("2$", "X"));
      }
    }

    System.err.println("Prefixes for files I will read are " + prefixes);
    PrintStream out = new PrintStream(new File(resultDir + outPrefix + ".fasta"));
    PrintStream libOut = new PrintStream(new File(resultDir + outPrefix + ".library"));
    // index of mates, assuming files are in the same pairing order
    HashMap<String, HashMap<String, ArrayList<String>>> mates = new HashMap<String, HashMap<String, ArrayList<String>>>();
    for (File fs : files) {
      // first trim to 25bp
      if (containsPrefix(prefixes, fs.getName(), "fastq") || containsPrefix(prefixes, fs.getName(), "fastq")) {
        String myPrefix = fs.getName().replaceAll("1\\.", "X").replaceAll("2\\.", "X").replaceAll("X.*", "");
        System.err.println("Processing file " + fs.getName() + " prefix " + myPrefix + " FOR FASTA OUTPUT");
        if (mates.get(myPrefix) == null) {
          mates.put(myPrefix, new HashMap<String, ArrayList<String>>());
          mates.get(myPrefix).put("left", new ArrayList<String>());
          mates.get(myPrefix).put("right", new ArrayList<String>());
        }
        BufferedReader bf = Utils.getFile(fs.getAbsolutePath(), "fastq");
        if (bf != null) {
          String line = null;
          int counter = 0;

          while ((line = bf.readLine()) != null) {
            StringBuffer b = new StringBuffer();
            if (counter % 4 == 0) {
              String name = line.replaceAll("@", ""+myPrefix).replaceAll("/", "_");
              out.println(">" + name);
              String[] split = name.trim().split("\\s+");
              if (fs.getName().matches(".*1\\..*")) {
                mates.get(myPrefix).get("left").add(split[0]);
              } else if (fs.getName().matches(".*2\\..*")) {
                mates.get(myPrefix).get("right").add(split[0]);
              }
            } else if ((counter - 1) % 4 == 0) {
              out.println(line.substring(0, SUB_LEN));
            }
            if (counter % 1000000 == 0) {
              System.err.println("Processed " + counter + " reads");
              out.flush();
            }
            counter++;
          }
          bf.close();
        }
      }
    }
    out.close();

    for (String lib : mates.keySet()) {
      HashMap<String, ArrayList<String>> libMates = mates.get(lib);
      String libName = lib.replaceAll("_", "");
      if (libSizes.get(libName) == null) {
        System.err.println("No library sizes defined for library:" + libName);
        String knownLibraries = "";
        for (String library : libSizes.keySet()) {
          knownLibraries += library + ",";
        }
        // Strip the last column.
        knownLibraries = knownLibraries.substring(
            0, knownLibraries.length() - 1);
        System.err.println("Known libraries are: " + knownLibraries);
        System.exit(1);
      }
      libOut.println("library " + libName + " " + libSizes.get(libName).first + " " + (int)libSizes.get(libName).second);
      ArrayList<String> left = libMates.get("left");
      ArrayList<String> right = libMates.get("right");
      for (int whichMate = 0; whichMate < left.size(); whichMate++) {
        libOut.println(left.get(whichMate) + " " + right.get(whichMate) + " " + libName);
      }
    }
    libOut.close();
    System.err.println("Library file built");

    // run the bowtie aligner
    System.err.println("Launching bowtie aligner: " + perlCommand + " -reads " + readDir + " -assembly " + asmDir + " -suffix " + suffix + " --threads 2");
    Process p = Runtime.getRuntime().exec("perl " + perlCommand + " -reads " + readDir + " -assembly " + asmDir + " -suffix " + suffix + " --threads 2");
    p.waitFor();
    System.err.println("Bowtie finished");
    HashMap<String, ArrayList<MappingInfo>> map = new HashMap<String, ArrayList<MappingInfo>>(NUM_CTGS);
    for (String prefix : prefixes) {
      String first = resultDir + prefix.replaceAll("X", "1") + ".bout";
      String second  = resultDir + prefix.replaceAll("X", "2") + ".bout";
      if (!new File(first).exists()) {
        first = first + ".bz2";
        second = second + ".bz2";
        if (!new File(first).exists()) {
          System.err.println("Cannot find bowtie output, expected " + resultDir + prefix + ".1.bout[.bz2]");
          System.exit(1);
        }
      }
      readBowtieResults(first, prefix, map);
      readBowtieResults(second, prefix, map);
    }

    // finally run through all the contig files and build the TIGR .contig file
    dir = new File(asmDir);
    if (!dir.isDirectory()) {
      System.err.println("Error, read directory " + asmDir + " is not a directory");
      System.exit(1);
    }

    File contigFasta = null;
    for (File f: dir.listFiles()) {
      if (f.getName().endsWith(suffix)) {
        contigFasta = f;
        break;
      }
    }

    out = new PrintStream(new File(resultDir + outPrefix + ".contig"));
    BufferedReader bf = new BufferedReader(new FileReader(contigFasta));
    String line = null;
    String contigID = null;
    StringBuffer contigSequence = null;
    int counter = 0;
    while ((line = bf.readLine()) != null) {
      String[] splitLine = line.trim().split("\\s+");
      if (splitLine[0].startsWith(">")) {
        if (contigID != null) {
          if (counter % 10000 == 0) {
            System.err.println("Processed in " + counter + " contig records");
          }
          counter++;

          outputContigRecord(out, contigID, contigSequence.toString(), map.get(contigID));
        }
        contigID = splitLine[0].replaceAll(">", "");
        contigSequence = new StringBuffer();
      } else {
        contigSequence.append(line + "\n");
      }
    }

    if (contigID != null) {
      buildBambusInput.outputContigRecord(out, contigID, contigSequence.toString(), map.get(contigID));
    }

    bf.close();
    out.close();
  }
}
