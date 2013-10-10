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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphNodeData;
import contrail.stages.CompressibleNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * Base class for selecting nodes.
 * Subclasses are used to determine the actual filtering criterion.
 */
public class FilterNodes extends MRStage {
  private static final Logger sLogger =
      Logger.getLogger(SelectNodes.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition filter = new ParameterDefinition(
        "filter",
        "The Name of the mapper class to use as the filter for selecting " +
        "nodes. This should be the full classpath.",
        String.class,
        null);
    defs.put(filter.getName(), filter);
    return Collections.unmodifiableMap(defs);
  }

  private FilterBase newFilterBase() {
    FilterBase base = null;
    String filter = (String) stage_options.get("filter");
    try {
      base =
          Class.forName(filter).asSubclass(FilterBase.class).newInstance();
    } catch (ClassNotFoundException e) {
      sLogger.fatal("Could not find class:" + filter);
    } catch (InstantiationException e) {
      sLogger.fatal("Could not instantiate class: " + filter, e);
    } catch (IllegalAccessException e) {
      sLogger.fatal("Could not instantiate class: " + filter, e);
    }

    return base;
  }

  @Override
  protected void startExecuteHook() {
    // Determine the list of required parameters by looking at the
    // filter.
    FilterBase base = newFilterBase();
    addParameterDefinitions(base.getParameterDefinitions());
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) (getConf());

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    GraphNodeData nodeData = new GraphNodeData();
    CompressibleNodeData compressibleData = new CompressibleNodeData();

    // We need to create a schema representing the union of GraphNodeData
    // and CompressibleNodeData.
    schemas.add(nodeData.getSchema());
    schemas.add(compressibleData.getSchema());
    Schema unionSchema = Schema.createUnion(schemas);

    AvroJob.setInputSchema(conf, unionSchema);

    AvroJob.setMapOutputSchema(conf, nodeData.getSchema());
    AvroJob.setOutputSchema(conf, nodeData.getSchema());

    FilterBase base = newFilterBase();
    AvroJob.setMapperClass(conf, base.filterClass());

    // Reducer only.
    conf.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortGraph(), args);
    System.exit(res);
  }
}
