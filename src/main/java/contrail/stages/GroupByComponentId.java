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
package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.util.ContrailLogger;

/**
 * Group nodes according to some id.
 */
public class GroupByComponentId extends MRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(GroupByComponentId.class);

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

  public static class Mapper extends AvroMapper <Object, Pair<CharSequence, Object>> {
    private Pair<CharSequence, Object> outPair;

    @Override
    public void configure(JobConf job) {
      ArrayList<Schema> schemas = new ArrayList<Schema>();
      schemas.add(Schema.create(Schema.Type.STRING));
      schemas.add(new GraphNodeData().getSchema());
      Schema union = Schema.createUnion(schemas);
      outPair = new Pair<CharSequence, Object>(
          Schema.create(Schema.Type.STRING), union);
    }

    @Override
    public void map(
        Object record,
        AvroCollector<Pair<CharSequence, Object>> collector, Reporter reporter)
            throws IOException {
      if (record instanceof GraphNodeData) {
        outPair.key(((GraphNodeData) record).getNodeId());
        outPair.value(record);
      } else {
        Pair<CharSequence, List<CharSequence>> component =
            (Pair<CharSequence, List<CharSequence>>) record;
        for (CharSequence nodeId : component.value()) {
          outPair.key(nodeId);
          outPair.value(component.key());
          collector.collect(outPair);
        }
      }
    }
  }

  public static class Reducer extends
      AvroReducer<CharSequence, Object, Pair<CharSequence, GraphNodeData>> {
    private Pair<CharSequence, GraphNodeData> outPair;
    private GraphNode node;

    @Override
    public void configure(JobConf job) {
      outPair = new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
      node = new GraphNode();
    }

    @Override
    public void reduce(
        CharSequence id, Iterable<Object> records,
        AvroCollector<Pair<CharSequence, GraphNodeData>> collector,
        Reporter reporter) throws IOException {

      outPair.key(null);
      outPair.value(null);

      int numRecords = 0;
      for (Object record : records) {
        ++numRecords;
        if (record instanceof CharSequence) {
          outPair.key(record.toString());
        } else {
          node.setData((GraphNodeData)record);
          outPair.value(node.clone().getData());
        }
      }

      if (numRecords > 2) {
        // There should be at most 2 records for each key.
        reporter.incrCounter("contrail", "error-more-than-2-records", 1);
      }

      if (outPair.key() != null && outPair.value() != null) {
        collector.collect(outPair);
      } else if (outPair.key() == null) {
        reporter.incrCounter("contrail", "error-missing-component-id", 1);
        sLogger.fatal(
            String.format(
                "Node %s wasn't assigned to a component.",
                outPair.value().getNodeId()),
                new RuntimeException("Missing component"));
      } else if (outPair.value() == null) {
        reporter.incrCounter("contrail", "error-missing-node-data", 1);
        sLogger.fatal(
            String.format("No GraphNodeData for node %s", id),
             new RuntimeException("Missing node data."));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new GroupByComponentId(), args);
    System.exit(res);
  }
}
