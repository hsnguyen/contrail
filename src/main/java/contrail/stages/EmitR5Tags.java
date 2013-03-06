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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
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

import contrail.QuickMarkMessage;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.R5Tag;
import contrail.sequences.KMerReadTag;
import contrail.sequences.StrandsForEdge;
import contrail.stages.QuickMarkAvro.QuickMarkMapper;
import contrail.stages.QuickMarkAvro.QuickMarkReducer;

/**
 * This stage emits the R5Tags in each node keyed by the read id.
 *
 * This can then be used to align the reads to the contigs.
 */
public class EmitR5Tags extends MRStage {
  private static final Logger sLogger = Logger.getLogger(EmitR5Tags.class);

  public static class EmitR5TagsMapper extends
    AvroMapper<GraphNodeData, R5TagNodeIdPair>> {
      private R5TagNodeIdPair outPair;

      public void configure(JobConf job) {
        outPair = new R5TagNodeIdPair();
      }

      public void map(GraphNodeData nodeData,
          AvroCollector<Pair<CharSequence, R5Tag>> collector,
          Reporter reporter) throws IOException {
        for (R5Tag tag : nodeData.getR5Tags()) {
          // Output key is the id of the read.
          outPair.setR5tag(tag);
          outPair.setNodeId(nodeData.getNodeId());
          collector.collect(outPair);
        }
      }
  }
//
//  public static class QuickMarkReducer extends
//      AvroReducer<CharSequence, QuickMarkMessage, GraphNodeData> {
//    GraphNode node = null;
//
//    public void configure(JobConf job) {
//      node= new GraphNode();
//    }
//
//    @Override
//    public void reduce(CharSequence  nodeid, Iterable<QuickMarkMessage> iterable,
//        AvroCollector<GraphNodeData> collector, Reporter reporter) throws IOException  {
//      boolean compresspair = false;
//
//      Iterator<QuickMarkMessage> iter = iterable.iterator();
//      int sawnode = 0;
//
//      while(iter.hasNext()) {
//        QuickMarkMessage msg = iter.next();
//
//        if(msg.getNode() != null)   {
//          node.setData(msg.getNode());
//          node= node.clone();
//          sawnode++;
//        }
//        if (msg.getSendToCompressor() == true)  {
//          // This must be a message of compression i.e. whose CompressibleStrands were not NONE
//          compresspair = true;
//        }
//      }
//
//      if (sawnode != 1) {
//        throw new IOException(String.format(
//            "ERROR: There should be exactly 1 node for nodeid:%s but there " +
//            "were %d nodes for this id.", nodeid.toString(), sawnode));
//      }
//
//      if (compresspair)     {
//        KMerReadTag readtag = new KMerReadTag("compress", 0);
//        //when QuickMerge is run all nodes that need to be compressed or are connected to compressed nodes will be sent to the same reducer
//        node.setMertag(readtag);
//        reporter.incrCounter(
//            GraphCounters.quick_mark_nodes_send_to_compressor.group,
//            GraphCounters.quick_mark_nodes_send_to_compressor.tag, 1);
//      }
//      else  {
//        KMerReadTag readtag = new KMerReadTag(node.getNodeId(), node.getNodeId().hashCode());
//        node.setMertag(readtag);
//      }
//      collector.collect(node.getData());
//    }
//  }
//
//  /**
//   * Get the parameters used by this stage.
//   */
//  protected Map<String, ParameterDefinition> createParameterDefinitions() {
//      HashMap<String, ParameterDefinition> defs =
//        new HashMap<String, ParameterDefinition>();
//
//    defs.putAll(super.createParameterDefinitions());
//
//    for (ParameterDefinition def:
//      ContrailParameters.getInputOutputPathOptions()) {
//      defs.put(def.getName(), def);
//    }
//    return Collections.unmodifiableMap(defs);
//  }
//
//  @Override
//  protected void setupConfHook() {
//    JobConf conf = (JobConf) getConf();
//
//    String inputPath = (String) stage_options.get("inputpath");
//    String outputPath = (String) stage_options.get("outputpath");
//
//    FileInputFormat.addInputPath(conf, new Path(inputPath));
//    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//    Pair<CharSequence, QuickMarkMessage> map_output =
//        new Pair<CharSequence, QuickMarkMessage>("", new QuickMarkMessage());
//
//    CompressibleNodeData compressible_node = new CompressibleNodeData();
//    AvroJob.setInputSchema(conf, compressible_node.getSchema());
//
//    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
//    AvroJob.setOutputSchema(conf, QuickMarkAvro.REDUCE_OUT_SCHEMA);
//
//    AvroJob.setMapperClass(conf, QuickMarkMapper.class);
//    AvroJob.setReducerClass(conf, QuickMarkReducer.class);
//  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new EmitR%Tags(), args);
    System.exit(res);
  }
}
