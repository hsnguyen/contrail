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
package contrail.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.FastQRecord;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * A simple mapreduce job which sorts reads by id.
 * This program is useful if you want to build an avro sorted key value
 * file for the graph.
 *
 * Currently this only works with FastQRecords but we could easily extend
 * this.
 */
public class SortReads extends MRStage {
  private static final Logger sLogger = Logger.getLogger(SortReads.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
      new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition parameter :
         ContrailParameters.getInputOutputPathOptions()) {
      defs.put(parameter.getName(), parameter);
    }

    // TODO(jeremy@lewi.us): This option is a temporary workaround for
    // the following issue:
    // http://code.google.com/p/contrail-bio/issues/detail?id=18
    ParameterDefinition ignoreDuplicates = new ParameterDefinition(
        "ignore_duplicates",
        "Warning for debugging only. If a read appears multiple times we " +
        "ignore it rather than throwing an exception", Boolean.class, false);

    defs.put(ignoreDuplicates.getName(), ignoreDuplicates);

    return Collections.unmodifiableMap(defs);
  }

  public static class SortMapper extends
    AvroMapper<FastQRecord, Pair<CharSequence, FastQRecord>> {
    private Pair<CharSequence, FastQRecord> pair;
    public void configure(JobConf job) {
      pair = new Pair<CharSequence, FastQRecord>("", new FastQRecord());
    }

    @Override
    public void map(FastQRecord read,
        AvroCollector<Pair<CharSequence, FastQRecord>> collector,
        Reporter reporter)
            throws IOException {
      pair.set(read.getId(), read);
      collector.collect(pair);
    }
  }

  public static class SortReducer extends
      AvroReducer<CharSequence, FastQRecord, FastQRecord> {
    private boolean ignoreDuplicates;

    public void configure(JobConf job) {
      SortReads stage = new SortReads();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      ignoreDuplicates =
          (Boolean)(definitions.get("ignore_duplicates")).parseJobConf(job);
    }

    @Override
    public void reduce(CharSequence readId, Iterable<FastQRecord> iterable,
        AvroCollector<FastQRecord> collector, Reporter reporter)
            throws IOException {
      Iterator<FastQRecord> iterator = iterable.iterator();
      if (!iterator.hasNext()) {
        sLogger.fatal(
            "No read for id:" + readId, new RuntimeException("No read."));
      }
      collector.collect(iterator.next());
      if (iterator.hasNext()) {
        if (ignoreDuplicates) {
          sLogger.warn("Multiple reads for id:" + readId);
        } else {
          sLogger.fatal(
              "Multiple reads for id:" + readId,
              new RuntimeException("multiple reads."));
          throw new RuntimeException("Multiple read.");
        }
      }
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) (getConf());

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroJob.setInputSchema(conf, new FastQRecord().getSchema());

    Pair<CharSequence, FastQRecord> pair =
        new Pair<CharSequence, FastQRecord>("", new FastQRecord());
    AvroJob.setMapOutputSchema(conf, pair.getSchema());
    AvroJob.setOutputSchema(conf, pair.value().getSchema());

    AvroJob.setMapperClass(conf, SortMapper.class);
    AvroJob.setReducerClass(conf, SortReducer.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortReads(), args);
    System.exit(res);
  }
}

