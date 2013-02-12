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
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

/**
 * Stage with hooks for customizing different parts of an MR execution.
 *
 * This is an experimental class. The goal of this class is to try to
 * address some of the pain points of using Stage. Currently the stage
 * class requires the subclass writers to manually do repetitive tasks
 * such as initialize the hadoop configuration, validate parameters etc...
 *
 * The point of this class is to handle all the common processing but
 * provide hooks at various points in stage execution which can be overloaded
 * to allow the subclass to customize functionality at different points.
 */
public class MRStage extends Stage {
  private static final Logger sLogger = Logger.getLogger(MRStage.class);
  protected RunningJob job;

  public MRStage() {
    job = null;
    infoWriter = null;
  }

  /**
   * Subclasses should override this hook and use it to configure the job.
   *
   * For example, the subclass should set the input/output format for the
   * configuration.
   */
  protected void setupConfHook() {
    // Do nothing by default.
  }

  /**
   * Execute the stage.
   *
   * @return: True on success false otherwise.
   */
  final public boolean execute() {
    // TODO(jeremy@lewi.us): We should check the required arguments are set
    // and invoke a hook to validate the parameters.

    // Initialize the hadoop configuration.
    if (getConf() == null) {
      setConf(new JobConf(this.getClass()));
    } else {
      setConf(new JobConf(getConf(), this.getClass()));
    }
    JobConf conf = (JobConf) getConf();
    initializeJobConfiguration(conf);

    setupConfHook();
    logParameters();
    if (stage_options.containsKey("writeconfig")) {
      writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already.
      // TODO(jlewi): We should add an option to disable this so we don't
      // accidentally override data.
      Path outPath = FileOutputFormat.getOutputPath(conf);
      try {
        FileSystem outFs = outPath.getFileSystem(conf);
        if (outFs.exists(outPath)) {
          // TODO(jlewi): We should only delete an existing directory
          // if explicitly told to do so.
          sLogger.info("Deleting output path: " + outPath.toString() + " "
              + "because it already exists.");
          outFs.delete(outPath, true);
        }
      } catch (IOException e) {
        sLogger.fatal(
            "There was a problem checking if the directory:" +
            outPath.toString() + " exists and deleting it if it does.", e);
        System.exit(-1);
      }
      try {
        // Write the stageinfo if a writer is specified.
        if (infoWriter != null) {
          infoWriter.writeStage(getStageInfo(null));
        }
        job = JobClient.runJob(conf);

        if (infoWriter != null) {
          infoWriter.overWriteLastStage(getStageInfo(null));
        }
        return job.isSuccessful();
      } catch (IOException e) {
        sLogger.fatal(
            "There was a problem running the mr job.", e);
        System.exit(-1);
      }
    }

    return true;
  }
}
