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
package contrail.crunch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import contrail.graph.EdgeDirection;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.MinMaxDegree;
import contrail.graph.MinMaxDegreeStats;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.tools.PrettyPrint;

/**
 * A crunch pipeline which computes the degree statistics.
 * For each node we compute (max degree, min degree). We count how many nodes occur
 * with (max degree, min degree).
 */
public class DegreeStats extends CrunchStage {
	/**
	 *  creates the custom definitions that we need for this phase
	 */
	@Override
	protected Map<String, ParameterDefinition> createParameterDefinitions() {
		HashMap<String, ParameterDefinition> defs =
				new HashMap<String, ParameterDefinition>();
		defs.putAll(super.createParameterDefinitions());

		for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
			defs.put(def.getName(), def);
		}
		return Collections.unmodifiableMap(defs);
	}

	/**
	 * Compute the min and max degree for each node.
	 */
	public static class MinMaxDegreeDo extends DoFn<GraphNodeData, MinMaxDegree> {
		@Override
		public void process(GraphNodeData nodeData, Emitter<MinMaxDegree> emitter) {
			GraphNode node = new GraphNode(nodeData);

			MinMaxDegree degree = new MinMaxDegree();
			degree.setMax(Math.max(node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING), 
					node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING)));
			degree.setMin(Math.min(node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING), 
					node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING)));
			emitter.emit(degree);
		}
	}
	
  /**
   * Convert a pair to a record.
   */
  public static class CountConverter 
      extends DoFn<Pair<MinMaxDegree, Long>, MinMaxDegreeStats> {
    @Override
    public void process(
        Pair<MinMaxDegree, Long> pair, 
        Emitter<MinMaxDegreeStats> emitter) {      
      MinMaxDegreeStats stats = new MinMaxDegreeStats();
      stats.setMax(pair.first().getMax());
      stats.setMin(pair.first().getMin());
      stats.setCount(pair.second());
      emitter.emit(stats);
    }
  }	

	@Override
	protected void stageMain() {
	  String inputPath = (String) stage_options.get("inputpath");
	  String outputPath = (String) stage_options.get("outputpath");
	  
	  Source<GraphNodeData> source = From.avroFile(
	      inputPath, Avros.specifics(GraphNodeData.class));

	  // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(DegreeStats.class, getConf());

    PCollection<GraphNodeData> nodes = pipeline.read(source);
    PTable<MinMaxDegree, Long> counts = nodes.parallelDo(
            new MinMaxDegreeDo(), Avros.specifics(MinMaxDegree.class))
              .count();
    PCollection<MinMaxDegreeStats> stats = counts.parallelDo(
        new CountConverter(), Avros.specifics(MinMaxDegreeStats.class));
    
    stats.write(To.avroFile(outputPath));
    
    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();
	}
	

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DegreeStats(), args);
    System.exit(res);
  }
}
