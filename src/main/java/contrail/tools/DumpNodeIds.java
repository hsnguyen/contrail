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
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * An MR job to dump the ids associated with nodes to a text file along
 * with a count for each node.
 *
 * This is primarily useful for debugging; i.e to identify nodes which
 * appear twice.
 */
public class DumpNodeIds extends MRStage {
  private static final Logger sLogger = Logger.getLogger(DumpNodeIds.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String,
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def : ContrailParameters
        .getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  private static class DumpMapper extends
      AvroMapper<Object, Pair<CharSequence, Integer>> {
    private Pair<CharSequence, Integer> outPair;

    @Override
    public void configure(JobConf job) {
      outPair = new Pair<CharSequence, Integer>("", 0);
    }

    /**
     * Mapper to do the conversion.
     */
    @Override
    public void map(
        Object record,
        AvroCollector<Pair<CharSequence, Integer>> collector,
        Reporter reporter) throws IOException {
      if (record instanceof GraphNodeData) {
        GraphNodeData data = (GraphNodeData) record;
        outPair.key(data.getNodeId().toString());
        outPair.value(1);
        collector.collect(outPair);
      } else if (record instanceof List<?>) {
        for (Object item : (List<Object>) record) {
          if (item instanceof GraphNodeData) {
            GraphNodeData data = (GraphNodeData) item;
            outPair.key(data.getNodeId().toString());
            outPair.value(1);
            collector.collect(outPair);
          } else {
            sLogger.fatal(
                "Item isn't valid type:" + item.getClass().getCanonicalName());
          }
        }
      } else {
        sLogger.fatal(
            "Record isn't valid type:" + record.getClass().getCanonicalName());
      }
   }
  }

  private static class DumpReducer implements Reducer<
      AvroKey<CharSequence>, AvroValue<Integer>, Text, IntWritable> {
    private Text nodeId;
    private IntWritable countWritable;
    @Override
    public void configure(JobConf job) {
      // TODO Auto-generated method stub
      nodeId = new Text();
      countWritable = new IntWritable();
    }

    @Override
    public void reduce(AvroKey<CharSequence> key,
        Iterator<AvroValue<Integer>> values,
        OutputCollector<Text, IntWritable> collector, Reporter reporter)
        throws IOException {
      nodeId.set(key.datum().toString());
      int count = 0;
      while (values.hasNext()) {
        AvroValue<Integer> v = values.next();
        count += v.datum();
      }
      countWritable.set(count);
      collector.collect(nodeId, countWritable);
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(new GraphNodeData().getSchema());
    schemas.add(Schema.createArray(new GraphNodeData().getSchema()));
    AvroJob.setInputSchema(conf, Schema.createUnion(schemas));

    Pair<CharSequence, Integer> pair = new Pair<CharSequence, Integer>("", 0);
    AvroJob.setMapOutputSchema(conf,  pair.getSchema());

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroInputFormat<Object> inputFormat = new AvroInputFormat<Object>();
    conf.setInputFormat(inputFormat.getClass());
    conf.setOutputFormat(TextOutputFormat.class);

    AvroJob.setMapperClass(conf, DumpMapper.class);
    conf.setReducerClass(DumpReducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new DumpNodeIds(), args);
    System.exit(res);
  }
}
