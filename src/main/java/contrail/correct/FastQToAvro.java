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
// Author: Deepak Nettem (deepaknettem@gmail.com)

package contrail.correct;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.io.FastQInputFormat;
import contrail.io.FastQText;
import contrail.sequences.FastQRecord;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * MapReduce job to convert a FastQ File to Avro.
 * Uses FastQInputFormat.
 *
 * TODO(deepaknettem): This code needs a unittest.
 */
public class FastQToAvro extends Stage {
  final Logger sLogger = Logger.getLogger(FastQToAvro.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String,
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def :
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public static class FastQToAvroMapper extends MapReduceBase
      implements Mapper<LongWritable, FastQText,
                        AvroWrapper<FastQRecord>, NullWritable> {
    private final FastQRecord read = new FastQRecord();
    private final AvroWrapper<FastQRecord> out_wrapper = new AvroWrapper<FastQRecord>(read);

    @Override
    public void map(LongWritable line, FastQText record,
        OutputCollector<AvroWrapper<FastQRecord>, NullWritable> output, Reporter reporter)
            throws IOException {

      read.id = record.getId();
      read.read = record.getDna();
      read.qvalue = record.getQValue();

      output.collect(out_wrapper, NullWritable.get());
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    JobConf conf = new JobConf(FastQToAvro.class);

    conf.setJobName("FastQToAvro");
    String inputPath, outputPath;
    conf.setJobName("Rekey Data");
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    inputPath = (String) stage_options.get("inputpath");
    outputPath = (String) stage_options.get("outputpath");

    //Sets the parameters in JobConf
    initializeJobConfiguration(conf);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    AvroJob.setOutputSchema(conf,new FastQRecord().getSchema());

    // Input
    conf.setMapperClass(FastQToAvroMapper.class);
    conf.setInputFormat(FastQInputFormat.class);

    if (stage_options.containsKey("splitSize")) {
      Long splitSize = (Long) stage_options.get("splitSize");
      conf.setLong("FastQInputFormat.splitSize", splitSize);
    }

    // Map Only Job
    conf.setNumReduceTasks(0);

    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);
    }

    long starttime = System.currentTimeMillis();
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) ((endtime - starttime) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return result;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FastQToAvro(), args);
    System.exit(res);
  }
}

