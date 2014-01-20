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
// Author: Jeremy Lewi (jeremy@lewi.us), Serge Koren(sergekoren@gmail.com)
package contrail.scaffolding;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.sequences.FastaRecord;
import contrail.sequences.ReadIdUtil;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.FileHelper;

/**
 * This class constructs the input needed to run Bambus for scaffolding.
 *
 * The input is:
 *   1. The original reads
 *   2. The assembled contigs
 *   3. The alignments of the reads to the contigs produced by bowtie.
 *   4. A libSize file listing each library and the min/max insert size.
 *
 * The output is:
 *   1. A single fasta file containing all the original reads.
 *   2. A library file which lists the ids of each mate pair in each library.
 *   3. A tigr file containing the contigs and information about how the reads
 *      align to the contigs.
 */
public class BuildBambusInput extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(BuildBambusInput.class);
  /**
   * This class stores a pair of files containing mate pairs.
   */
  protected static class MateFilePair implements Comparable {
    public String leftFile;
    public String rightFile;
    public String libraryName;

    public MateFilePair(
        String libraryName, String leftFile, String rightFile) {
      this.libraryName = libraryName;
      this.leftFile = leftFile;
      this.rightFile = rightFile;
    }

    @Override
    public int compareTo(Object o) {
      if (!(o instanceof MateFilePair)) {
        throw new RuntimeException("Can only compare to other MateFilePair");
      }

      MateFilePair other = (MateFilePair) o;
      return this.libraryName.compareTo(other.libraryName);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MateFilePair)) {
        throw new RuntimeException("o must be an instance of MateFilePair");
      }

      MateFilePair other = (MateFilePair) o;
      if (!leftFile.equals(other.leftFile)) {
        return false;
      }
      if (!rightFile.equals(other.rightFile)) {
        return false;
      }
      if (!libraryName.equals(other.libraryName)) {
        return false;
      }
      return true;
    }
  }

  /**
   * Store the minimum and maximum size for inserts in a library.
   */
  private static class LibrarySize {
    final public int minimum;
    final public int maximum;
    private final int libSize;

    public LibrarySize(int first, int second) {
      minimum = Math.min(first, second);
      maximum = Math.max(first, second);
      libSize = maximum - minimum + 1;
    }

    public int size() {
      return libSize;
    }
  }

  // libSizes stores the sizes for each read library. The key is the
  // prefix of the FastQ files for that library. The value is a pair
  // which stores the lower and upper bound for the library size.
  private HashMap<String, LibrarySize> libSizes;

  /**
   * Parse the library file and extract the library sizes.
   */
  protected void parseLibSizes(String libFile) {
    try {
      libSizes = new HashMap<String, LibrarySize>();
      BufferedReader libSizeFile =
          new BufferedReader(new FileReader(new File(libFile)));
      String libLine = null;
      while ((libLine = libSizeFile.readLine()) != null) {
        String[] splitLine = libLine.trim().split("\\s+");
        libSizes.put(
            splitLine[0],
            new LibrarySize(
                Integer.parseInt(splitLine[1]),
                Integer.parseInt(splitLine[2])));
      }
      libSizeFile.close();
    } catch (Exception e) {
      sLogger.fatal("Could not parse the library file.", e);
      System.exit(-1);
    }
  }

  /**
   * Given a set of read files, group them into mate pairs.
   *
   * @param readFiles
   */
  protected ArrayList<MateFilePair> buildMatePairs(
      Collection<String> readFiles) {
    HashMap<String, ArrayList<String>> libraryFiles =
        new HashMap<String, ArrayList<String>>();

    ArrayList<MateFilePair> matePairs = new ArrayList<MateFilePair>();
    // Group the reads into mate pairs. Each mate pair belongs to a library.
    // The library name is given by the prefix of the filename.
    for (String filePath : readFiles) {
      String name = FilenameUtils.getBaseName(filePath);
      // We expect the filename to be something like "libraryName_1.fastq"
      // or "libraryName_2.fastq".
      if (!name.matches((".*_[012]"))) {
        sLogger.fatal(
            "File: " + filePath + " doesn't match the patern .*_[012] so we " +
            "couldn't determine the library name",
            new RuntimeException());
      }
      // We want to strip off the trailing _[12]
      String libraryName = name.substring(0, name.length() - 2);

      if (!libraryFiles.containsKey(libraryName)) {
        libraryFiles.put(libraryName, new ArrayList<String>());
      }
      libraryFiles.get(libraryName).add(filePath);
    }

    for (String libraryName : libraryFiles.keySet()) {
      ArrayList<String> files = libraryFiles.get(libraryName);
      if (files.size() != 2) {
        String message =
            "There was a problem grouping the reads into mate pairs. Each " +
            "library (filename prefix) should match two files. But for " +
            "library:" + libraryName + " the number of matching files was:" +
            files.size() + ".";
        if (files.size() > 0) {
          message =
              "The files that matched were: " + StringUtils.join(files, ",");
        }
        sLogger.fatal(message, new RuntimeException(message));
      }
      Collections.sort(files);
      MateFilePair pair = new MateFilePair(
          libraryName, files.get(0), files.get(1));
      matePairs.add(pair);

      sLogger.info("Found mate pairs for library:" + libraryName);
    }
    return matePairs;
  }

  /**
   * For each pair of mates write an entry to the library
   * file. We also shorten the reads and write them to the fasta file.
   * We need to put all the reads into one file because toAmos_new expects that.
   *
   * The reads are truncated because BOWTIE is a short read aligner and only
   * works with short reads. Therefore, Bambus needs to use the shortened reads
   * otherwise the alignment coordinates reported by Bambus won't be consistent.
   *
   * The code assumes that the reads in two mate pair files are already
   * aligned. i.e The i'th record in frag_1.fastq is the mate pair for
   * the i'th record in frag_2.fastq
   *
   * The reads are written to fastaOutputFile.
   *
   * @param matePairs: A collection of file pairs representing mate pair
   *    libraries.
   * @param fastaOutputFile: The file to write the shortened reads to.
   * @param libraryOutputFile: The file to write the library information to.
   */
  protected void createFastaAndLibraryFiles(
      Collection<MateFilePair> matePairs, File fastaOutputFile,
      File libraryOutputFile) {
    LibraryFileWriter libWriter = null;
    int readLength = (Integer) stage_options.get("readLength");
    try {
      libWriter = new LibraryFileWriter(libraryOutputFile);
    } catch (IOException e) {
      sLogger.fatal("Could not create library file: " + libraryOutputFile, e);
    }

    PrintStream fastaStream = null;

    try {
      fastaStream = new PrintStream(fastaOutputFile);
    } catch(IOException e) {
      sLogger.fatal(String.format(
          "Could not open %s for writing.", fastaOutputFile), e);
      System.exit(-1);
    }

    for (MateFilePair matePair : matePairs) {
      libWriter.writeLibrary(
          matePair.libraryName, getLibSize(matePair.libraryName));

      FastQFileReader leftReader = new FastQFileReader(matePair.leftFile);
      FastQFileReader rightReader = new FastQFileReader(matePair.rightFile);

      int counter = 0;

      FastaRecord fasta = new FastaRecord();

      while (leftReader.hasNext() && rightReader.hasNext()) {
        FastQRecord left = leftReader.next();
        FastQRecord right = rightReader.next();

        String leftId = left.getId().toString();
        String rightId = right.getId().toString();
        if (!ReadIdUtil.isMatePair(leftId, rightId)) {
          sLogger.fatal(String.format(
              "Expecting a mate pair but the read ids: %s, %s do not form " +
                  "a valid mate pair.", leftId, rightId));
          System.exit(-1);
        }

        // TODO(jeremy@lewi.us): The original code added the library name
        // as a prefix to the read id and then replaced "/" with "_".
        // I think manipulating the readId's is risky because we need to
        // be consistent. So we don't prepend the library name.
        // However, some programs e.g bowtie cut the "/" off and set a
        // a special code. So to be consistent we use the function
        // safeReadId to convert readId's to a version that can be safely
        // used everywhere.
        left.setId(Utils.safeReadId(left.getId().toString()));
        right.setId(Utils.safeReadId(right.getId().toString()));

        libWriter.writeMateIds(
            left.getId().toString(), right.getId().toString(),
            matePair.libraryName);

        for (FastQRecord fastq : new FastQRecord[] {left, right}) {
          fasta.setId(fastq.getId());

          // Truncate the read because bowtie can only handle short reads.
          fasta.setRead(fastq.getRead().subSequence(0, readLength));

          FastUtil.writeFastARecord(fastaStream, fasta);
        }

        ++counter;
        if (counter % 1000000 == 0) {
          sLogger.info("Processed " + counter + " reads");
          fastaStream.flush();
        }
        counter++;
      }

      if (leftReader.hasNext() != rightReader.hasNext()) {
        sLogger.fatal(String.format(
            "The mait pair files %s and %s don't have the same number of " +
                "reads this indicates the reads aren't properly paired as mate " +
                "pairs.", matePair.leftFile, matePair.rightFile));
      }
      leftReader.close();
      rightReader.close();
    }

    libWriter.close();
  }

  /**
   * Writer for the library file.
   *
   * The library file lists each mate pair in each library.
   *
   * For more info on the bambus format see:
   * http://www.cs.jhu.edu/~genomics/Bambus/Manual.html#matesfile
   */
  private static class LibraryFileWriter {
    private PrintStream outStream;
    private File libraryFile;

    // TODO(jeremy@lewi.us): Do we really want to throw an exception in the
    // constructor?
    public LibraryFileWriter(File file) throws IOException {
      libraryFile = file;
      outStream = new PrintStream(libraryFile);
    }

    /**
     * Write the name of a library and its min and max insert size.
     */
    public void writeLibrary(String name, LibrarySize libSize) {
      String libName = name.replaceAll("_", "");

      outStream.println(
          "library " + libName + " " + libSize.minimum + " " + libSize.maximum);
    }

    /**
     * Write the ids of a mate pair.
     */
    public void writeMateIds(String leftId, String rightId, String libName) {
      // If the left read name starts with "l", "p" or "#" we have a problem
      // because  the binary toAmos_new in the amos package reserves uses these
      // characters to identify special types of rows in the file.
      if (leftId.startsWith("l") || leftId.startsWith("p") ||
          leftId.startsWith("#")) {
        sLogger.fatal(
            "The read named:" + leftId + " will cause problems with the " +
            "amos binary toAmos_new. The amos binary attributes special " +
            "meaning to rows in the library file starting with 'p', 'l' or " +
            "'#' so if the id for a read starts with any of those " +
            "characters it will mess up amos.",
            new RuntimeException("Invalid read name"));
        System.exit(-1);
      }
      outStream.println(leftId + " " + rightId + " " + libName);
    }

    public void close() {
      outStream.close();
    }
  }

  /**
   * Return the size of the library.
   */
  private LibrarySize getLibSize(String lib) {
    String libName = lib.replaceAll("_", "");
    LibrarySize result = libSizes.get(libName);
    if (result == null) {
      String knownLibraries = "";
      for (String library : libSizes.keySet()) {
        knownLibraries += library + ",";
      }
      // Strip the last column.
      knownLibraries = knownLibraries.substring(
          0, knownLibraries.length() - 1);
      sLogger.fatal(
          "No library sizes are defined for libray:" + libName + " . Known " +
          "libraries are: " + knownLibraries,
          new RuntimeException("No library sizes for libray:" + libName));
    }
    return result;
  }

  /**
   * Create a tigr file from the original contigs and converted bowtie outputs.
   * @param bowtieAvroPath: Path containing the bowtie mappings in avro format.
   */
  private void createTigrFile(String bowtieAvroPath) {
    String graphPath = (String) stage_options.get("graph_glob");
    // Convert the data to a tigr file.
    TigrCreator tigrCreator = new TigrCreator();
    tigrCreator.initializeAsChild(this);

    String hdfsPath = (String)stage_options.get("hdfs_path");

    tigrCreator.setParameter(
        "inputpath", StringUtils.join(
            new String[]{bowtieAvroPath, graphPath}, ","));

    String outputPath = FilenameUtils.concat(hdfsPath, "tigr");
    tigrCreator.setParameter("outputpath", outputPath);

    if (!tigrCreator.execute()) {
      sLogger.fatal(
          "Failed to create TIGR file.",
          new RuntimeException("TigrCreator failed."));
      System.exit(-1);

    }

    // Copy tigr file to local filesystem.
    ArrayList<Path> tigrOutputs = new ArrayList<Path>();

    FileSystem fs;
    try{
      fs = FileSystem.get(this.getConf());
      for (FileStatus status : fs.listStatus(new Path(outputPath))) {
        if (status.getPath().getName().startsWith("part")) {
          tigrOutputs.add(status.getPath());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }
    if (tigrOutputs.size() != 1) {
      sLogger.fatal(String.format(
          "TigrConverter should have produced a single output file. However " +
          "%d files were found that matched 'part*' in directory %s.",
          tigrOutputs.size(), outputPath),
          new RuntimeException("Improper output."));
      System.exit(-1);
    }

    File contigOutputFile = getContigOutputFile();

    // TODO(jlewi): How can we verify that the copy completes successfully.
    try {
      fs.copyToLocalFile(
          tigrOutputs.get(0), new Path(contigOutputFile.getPath()));
    } catch (IOException e) {
      sLogger.fatal(String.format(
          "Failed to copy %s to %s", tigrOutputs.get(0).toString(),
          contigOutputFile), e);
      System.exit(-1);
    }
  }

  /**
   * Align the contigs to the reads.
   *
   * @param args
   * @throws Exception
   */
  @Override
  protected void stageMain() {
    String libFile = (String) this.stage_options.get("libsize");
    parseLibSizes(libFile);

    String globs = (String) this.stage_options.get("reads_glob");
    ArrayList<String> readFiles = FileHelper.matchListOfGlobs(globs);

    if (readFiles.isEmpty()) {
      sLogger.fatal(
          "No read files matched:"  +
          (String) this.stage_options.get("reads_glob"),
          new RuntimeException("Missing inputs."));
      System.exit(-1);
    }

    sLogger.info("Files containing reads to align are:");
    for (String file : readFiles) {
      sLogger.info("read file:" + file);
    }
    ArrayList<MateFilePair> matePairs = buildMatePairs(readFiles);
    String resultDir = (String) stage_options.get("outputpath");

    File resultDirFile = new File(resultDir);
    if (!resultDirFile.exists()) {
      sLogger.info("Creating output directory:" + resultDir);

      if (!resultDirFile.mkdirs()) {
        sLogger.fatal("Could not create directory:" + resultDir);
        System.exit(-1);
      }
    }

    File fastaOutputFile = getFastaOutputFile();
    File libraryOutputFile = getLibraryOutputFile();
    File contigOutputFile = getContigOutputFile();

    sLogger.info("Outputs will be written to:");
    sLogger.info("Fasta file: " + fastaOutputFile.getName());
    sLogger.info("Library file: " + libraryOutputFile.getName());
    sLogger.info("Contig Aligned file: " + contigOutputFile.getName());

    createFastaAndLibraryFiles(matePairs, fastaOutputFile, libraryOutputFile);

    String bowtieAvroPath = (String) stage_options.get("bowtie_alignments");
    createTigrFile(bowtieAvroPath);
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    definitions.putAll(super.createParameterDefinitions());

    ParameterDefinition bowtieAlignments =
        new ParameterDefinition(
            "bowtie_alignments",
            "The hdfs path to the avro files containing the alignments " +
            "produced by bowtie of the reads to the contigs.",
            String.class, null);

    ParameterDefinition readsGlob =
        new ParameterDefinition(
            "reads_glob", "A glob expression matching the path to the fastq " +
            "files containg the reads to align to the reference genome. " +
            "Should be a local file system.",
            String.class, null);

    // Currently these need to be on the local filesystem.
    ParameterDefinition contigsGlob =
        new ParameterDefinition(
            "reference_glob", "A glob expression matching the path to the " +
            "fasta files containg the reference genome. Should be on the " +
            "local filesystem.",
            String.class, null);

    ParameterDefinition readLength =
        new ParameterDefinition(
            "read_length",
            "How short to make the reads. The value needs to be consistent " +
            "with the value used in AlignReadsWithBowtie. Bowtie requires " +
            "short reads. Bambus needs to use the same read lengths as those " +
            "used by bowtie because otherwise there could be issues with " +
            "contig distances because read start/end coordinates for the " +
            "alignments aren't consistent.",
            Integer.class, 25);

    ParameterDefinition libsizePath =
        new ParameterDefinition(
            "libsize", "The path to the file containing the sizes for each " +
            "library",
            String.class, null);

    ParameterDefinition outputPath =
        new ParameterDefinition(
            "outputpath", "The directory to write the outputs which are " +
            "the files to pass to bambus for scaffolding.",
            String.class, null);

    ParameterDefinition outputPrefix =
        new ParameterDefinition(
            "outprefix", "The prefix for the output files defaults to " +
            "(bambus_input).",
            String.class, "bambus_input");

    ParameterDefinition hdfsPath =
        new ParameterDefinition(
            "hdfs_path", "The path on the hadoop filesystem to use as a " +
            "working directory.", String.class, null);

    ParameterDefinition graphPath =
        new ParameterDefinition(
            "graph_glob", "The glob on the hadoop filesystem to the avro " +
            "files containing the GraphNodeData records representing the " +
            "graph.", String.class, null);

    for (ParameterDefinition def:
      new ParameterDefinition[] {
        bowtieAlignments, readsGlob, contigsGlob, libsizePath,
        outputPath, outputPrefix, hdfsPath, graphPath}) {
      definitions.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(definitions);
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> invalid = super.validateParameters();

    invalid.addAll(this.checkParameterIsNonEmptyString(Arrays.asList(
        "outputpath", "outprefix", "hdfs_path")));

    invalid.addAll(this.checkParameterIsExistingLocalFile(Arrays.asList(
        "libsize")));

    invalid.addAll(this.checkParameterMatchesLocalFiles(Arrays.asList(
        "reference_glob", "reads_glob")));

    invalid.addAll(this.checkParameterMatchesFiles(Arrays.asList(
        "graph_glob")));

    return invalid;
  }

  /**
   * Returns the name of the output file containing the shortened fasta reads.
   * @return
   */
  public File getFastaOutputFile() {
    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");
    return new File(resultDir, outPrefix + ".fasta");
  }

  /**
   * Returns the name of the output file containing the contigs in tigr format.
   * @return
   */
  public File getContigOutputFile() {
    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");
    return new File(resultDir, outPrefix + ".contig");
  }

  /**
   * Returns the name of the output file containing library.
   * @return
   */
  public File getLibraryOutputFile() {
    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");
    return new File(resultDir, outPrefix + ".library");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new BuildBambusInput(), args);
    System.exit(res);
  }
}
