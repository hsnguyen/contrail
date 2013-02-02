package contrail.pipelines;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.correct.BuildBitVector;
import contrail.correct.ConvertKMerCountsToText;
import contrail.correct.CutOffCalculation;
import contrail.correct.FastQToAvro;
import contrail.correct.InvokeFlash;
import contrail.correct.InvokeQuake;
import contrail.correct.JoinReads;
import contrail.correct.KmerCounter;
import contrail.correct.RekeyReads;
import contrail.stages.ContrailParameters;
import contrail.stages.NotImplementedException;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;


// TODO(jeremy@lewi.us): Why is bitvectorpath listed as an argument
// when you do "--help=true", shouldn't it get set automatically? It is
// used by BuildBitVector stage. We should exclude that option from
// the options for the CorrectionPipelineRunner stage because it should be
// derived automatically from the other parameters.
//
// TODO(jeremy@lewi.us): How should the user indicate that they don't
// want to run flash? One option would be to allow the empty string for
// flash_input. However, that makes it difficult to distinguish the user
// forgot to set the path or if they don't want to use flash. I think a
// better option is to add a boolean option to disable flash.
//
// TODO(jeremy@lewi.us): The code currently assumes that the quake paths
// and the flash paths are disjoint. Otherwise we would input some reads
// to quake twice and that would be bad. We should add a check to verify this
// and fail if this assumption is violated.
//
// TODO(jeremy@lewi.us): Quake has a mode which supports mate pairs; which
// I think just has to do with treating both pairs of a read the "same".
// Currently we don't use that mode and treat all reads as non-mate pairs
// for the purpose of quake.
public class CorrectionPipelineRunner extends Stage{
  private static final Logger sLogger = Logger.getLogger(CorrectionPipelineRunner.class);

  public CorrectionPipelineRunner() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }

  protected void setDefaultParameters() {
    // This function is intended to be overloaded in subclasses which
    // customize the parameters for different datasets.
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    // We add all the options for the stages we depend on.
    Stage[] substages =
      {
        new RekeyReads(), new JoinReads(), new InvokeFlash(), new KmerCounter(), new ConvertKMerCountsToText(), new CutOffCalculation(),
        new BuildBitVector(), new InvokeQuake()
      };

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    // Remove parameters inherited from the substages which get set
    // automatically or are derived from other parameters.
    definitions.remove("cutoff");
    definitions.remove("bitvectorpath");

    //The outputpath is a name of a directory in which all the outputs are placed
    ParameterDefinition flashInputPath = new ParameterDefinition(
        "flash_input", "The path to the fastq files to run flash on. This " +
        "can be a glob expression.",
        String.class, null);

    ParameterDefinition quakeNonMateInputPath = new ParameterDefinition(
        "no_flash_input",
        "The path to the fastq files which should be included in " +
        "quake but which we don't run flash on. This can be a glob expression.",
        String.class, null);
//    ParameterDefinition quakeMateInputPath = new ParameterDefinition(
//        "quake_input_mate", "The inputpath of matepairs on which quake is to be run", String.class, new String(""));

    definitions.put(flashInputPath.getName(), flashInputPath);
    definitions.put(quakeNonMateInputPath.getName(), quakeNonMateInputPath);

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * Join the FastQRecords corresponding to mate pairs.
   *
   * @param flashInputAvroPath
   * @return
   */
  private RunningJob runJoinMatePairs(String inputPath, String outputPath) {
    sLogger.info("Join mate pairs.");
    JoinReads stage = new JoinReads();
    HashMap<String, Object> parameters =new HashMap<String, Object>();
    parameters.put("inputpath", inputPath);
    parameters.put("outputpath", outputPath);
    stage.setParameters(parameters);
    return runStage(stage);
  }

  /**
   *
   * @param inputGlobs: Collection of input globs.
   */
  private void runQuake(Collection<String> inputGlobs) {
    String outputPath = (String) stage_options.get("outputpath");
    String quakeOutputPath = FilenameUtils.concat(outputPath, "quake");
    String noFlashInputAvroPath =FilenameUtils.concat(
        quakeOutputPath, FastQToAvro.class.getSimpleName());
      // Kmer counting stage
      sLogger.info("Running KmerCounter");
      KmerCounter kmerCounter = new KmerCounter();

      //stageInput = stageInput + quakeNonMateInput + "," + quakeMateInput;

      // We add the flash output directory only if the user has run flash
      //if(flashInvoked){
       // stageInput = stageInput + "," + flashOutputPath;
      //}
      //kmerCounterOutputPath = runStage(stage, stageInput, outputDirectory, "");

      String kmerInputPaths = StringUtils.join(inputGlobs, ",");
      HashMap<String, Object> counterOptions = new HashMap<String, Object>();
      counterOptions.put("inputpath", kmerInputPaths);

      kmerCounter.setParameters(counterOptions);

      throw new NotImplementedException("Need to update the remaining code");
      //Convert KmerCounts to Non Avro stage
//      sLogger.info("Running ConvertKMerCountsToText");
//      stage = new ConvertKMerCountsToText();
//      stageInput = new Path(kmerCounterOutputPath, "part-00000.avro").toString();
//      nonAvroKmerCountOutput = runStage(stage, stageInput, outputDirectory, "");
//
//      // Cutoff calculation stage
//      sLogger.info("Running CutOffCalculation");
//      CutOffCalculation cutoffObj = new CutOffCalculation();
//      stageInput = new Path(nonAvroKmerCountOutput, "part-00000").toString();
//      runStage(cutoffObj, stageInput, outputDirectory,  "");
//
//      //Bitvector construction
//      sLogger.info("Running BuildBitVector");
//      cutoff = cutoffObj.getCutoff();
//      stage_options.put("cutoff", cutoff);
//      stage = new BuildBitVector();
//      stageInput = kmerCounterOutputPath;
//      bitVectorDirectory = runStage(stage, stageInput, outputDirectory, "");
//      bitVectorPath = new Path(bitVectorDirectory, "bitvector").toString();
//      stage_options.put("bitvectorpath", bitVectorPath);
//
//      //Invoke Quake on Non Mate Pairs
//      // TODO(dnettem) - for now all quake inputs dumped together.
//      if(quakeNonMateInput.trim().length() !=0 ){
//        sLogger.info("Running Quake for Non Mate Pairs");
//        stage = new InvokeQuake();
//        stageInput = quakeNonMateInput;
//        if(flashInvoked){
//          stageInput = stageInput + "," +  flashOutputPath + "," + quakeMateInput;
//        }
//        runStage(stage, stageInput, outputDirectory, "non_mate_");
//      }
//      else{
//        sLogger.info("Skipping running Quake for non mate pairs");
//      }
  }

  private void runFlash() {
    String outputPath = (String) stage_options.get("outputpath");
    // The output will be organized into subdirectories for flash and quake
    // respectively.
    String flashOutputPath = FilenameUtils.concat(outputPath, "flash");

    // Convert the flash and no flash fastq files to avro


    String flashInputAvroPath = FilenameUtils.concat(
        flashOutputPath, FastQToAvro.class.getSimpleName());

    {
      FastQToAvro flashInputToAvro = new FastQToAvro();
      HashMap<String, Object> options = new HashMap<String, Object>();
      options.put("inputpath", stage_options.get("flash_input"));
      options.put("outputpath", flashInputAvroPath);
      flashInputToAvro.setParameters(options);
      runStage(flashInputToAvro);
    }


    //TODO(dnettem) - Make this more generic. Currently this is quick and dirty, since
    // we know for sure that Flash will run.


    //String[] required_args = {"flash_input", "flash_binary","flash_notcombined","splitSize"};

    String rawFlashInput = (String) stage_options.get("flash_input");
    String flashBinary = (String) stage_options.get("flash_binary");
    String notCombinedFlash = (String) stage_options.get("flash_notcombined");



    String quakeNonMateInput = (String) stage_options.get("quake_input_non_mate");
    String quakeMateInput = (String) stage_options.get("quake_input_mate");
    String quakeBinary = (String) stage_options.get("quake_binary");

    // Join for Flash.
    String flashJoinedPath = FilenameUtils.concat(
        flashOutputPath, JoinReads.class.getSimpleName());
    runJoinMatePairs(flashInputAvroPath, flashJoinedPath);

    // Invoke Flash
    // TODO(jeremy@lewi.us): We should add a boolean option to skip flash.
    String flashOutput = FilenameUtils.concat(
        flashOutputPath, InvokeFlash.class.getSimpleName());
    {
      sLogger.info("Running Flash");
      InvokeFlash invokeFlash = new InvokeFlash();

      Map<String, Object> parameters =
          ContrailParameters.extractParameters(
              stage_options, invokeFlash.getParameterDefinitions().values());
      parameters.put("inputpath", flashJoinedPath);
      parameters.put("outputpath", flashOutput);
      invokeFlash.setParameters(parameters);
      runStage(invokeFlash);
    }
  }

  /**
   * This method runs the entire pipeline
   * @throws Exception
   */
  private void runCorrectionPipeline() {
    runFlash();
    //runQuake();
  }

  /**
   * Runs a particular stage and returns the path containing the output
   * The output path is OutputDirectory/prefix+className
   * @param stage
   * @param inputPath
   * @param flashBinary
   * @param outputDirectory
   * @return
   * @throws Exception
   */
  private String runStage(
      Stage stage, String inputPath, String outputDirectory, String prefix) {

    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            stage.getParameterDefinitions().values());
    stage.setConf(getConf());
    stageOptions.put("inputpath", inputPath);
    String outputPath = new Path(outputDirectory, (prefix + stage.getClass().getName()) ).toString();
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);
    RunningJob job = null;
    try {
      job = stage.runJob();
      if (!job.isSuccessful()) {
        sLogger.fatal(
            String.format(
                "Stage %s had a problem", stage.getClass().getName()),
            new RuntimeException("Stage failed"));
        System.exit(-1);
      }
    } catch (Exception e) {
      sLogger.fatal(
          "Stage: " + stage.getClass().getSimpleName() + " failed.", e);
      System.exit(-1);
    }

    return outputPath;
  }

  /**
   * Runs a particular stage.
   *
   * This is a simple wrapper for handling stage failures.
   *
   * @param stage
   * @param inputPath
   * @param flashBinary
   * @param outputDirectory
   * @return
   * @throws Exception
   */
  private RunningJob runStage(Stage stage) {
    RunningJob job = null;
    try {
      job = stage.runJob();
      if (!job.isSuccessful()) {
        sLogger.fatal(
            String.format(
                "Stage %s had a problem", stage.getClass().getName()),
            new RuntimeException("Stage failed"));
        System.exit(-1);
      }
    } catch (Exception e) {
      sLogger.fatal(
          "Stage: " + stage.getClass().getSimpleName() + " failed.", e);
      System.exit(-1);
    }
    return job;
  }

  @Override
  public RunningJob runJob() throws Exception {
    // Check for missing arguments.
    String[] required_args = {
        "flash_binary", "no_flash_input", "flash_input", "outputpath"};
    checkHasParametersOrDie(required_args);

    Configuration baseConf = getConf();
    JobConf conf = null;
    if (baseConf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }

    initializeJobConfiguration(conf);
    if (stage_options.containsKey("writeconfig")) {
      throw new NotImplementedException(
          "Support for writeconfig isn't implemented yet for " +
          "AssembleContigs");
    } else {
      long starttime = System.currentTimeMillis();
      runCorrectionPipeline();
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);
      sLogger.info("Runtime: " + diff + " s");
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CorrectionPipelineRunner(), args);
    System.exit(res);
  }
}
