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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.graph.EdgeDirection;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.scaffolding.BowtieDoFns;
import contrail.scaffolding.BowtieMapping;
import contrail.sequences.DNAStrand;
import contrail.stages.CrunchStage;
import contrail.stages.ParameterDefinition;

/**
 * Join the contigs with the alignments produced by bowtie and compute basic
 * stats such as how many reads aligned to a contig.
 */
public class BowtieAlignmentStats extends CrunchStage {
  /**
   * Creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition bowtieAlignments =
        new ParameterDefinition(
            "bowtie_alignments",
            "The hdfs path to the avro files containing the alignments " +
            "produced by bowtie of the reads to the contigs.",
            String.class, null);

    ParameterDefinition graphPath =
        new ParameterDefinition(
            "graph_glob", "The glob on the hadoop filesystem to the avro " +
            "files containing the GraphNodeData records representing the " +
            "graph.", String.class, null);

    ParameterDefinition outputPath =
        new ParameterDefinition(
            "outputpath", "The directory to write the outputs which are " +
            "the files to pass to bambus for scaffolding.",
            String.class, null);

    for (ParameterDefinition def: Arrays.asList(
            bowtieAlignments, graphPath, outputPath)) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Compute stats about the read alignments to the contigs.
   */
  public static class ComputeStatsDo
      extends DoFn<
          Pair<String,
               Pair<Collection<GraphNodeData>, Collection<BowtieMapping>>>,
          ContigReadStats> {
    @Override
    public void process(
        Pair<String,
            Pair<Collection<GraphNodeData>, Collection<BowtieMapping>>> grouped,
        Emitter<ContigReadStats> emitter) {
      if (grouped.second().first().size() == 0) {
        this.increment("contrail-errors", "error-no-node");
        return;
      }
      if (grouped.second().first().size() > 1) {
        this.increment("contrail-errors", "error-multiple-nodes");
      }
      GraphNodeData nodeData = grouped.second().first().iterator().next();
      GraphNode node = new GraphNode(nodeData);
      ContigReadStats stats = new ContigReadStats();
      stats.setContigId(node.getNodeId().toString());
      stats.setLength(node.getSequence().size());
      stats.setCoverage(node.getCoverage());
      int inDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING);
      int outDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING);
      stats.setMaxDegree(Math.max(inDegree, outDegree));
      stats.setMinDegree(Math.min(inDegree, outDegree));

      stats.setNumAlignedReads(grouped.second().second().size());

      emitter.emit(stats);
    }
  }

  @Override
  protected void stageMain() {
    String bowtiePath = (String) stage_options.get("bowtie_alignments");
    String graphGlob = (String) stage_options.get("graph_glob");
    String outputPath = (String) stage_options.get("outputpath");

    Source<BowtieMapping> mappingSource = From.avroFile(
        bowtiePath, Avros.specifics(BowtieMapping.class));

    Source<GraphNodeData> nodeSource = From.avroFile(
        graphGlob, Avros.specifics(GraphNodeData.class));

    deleteExistingPath(new Path(outputPath));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(BowtieAlignmentStats.class, getConf());

    PCollection<BowtieMapping> mappings = pipeline.read(mappingSource);
    PCollection<GraphNodeData> nodes = pipeline.read(nodeSource);

    PTable<String, BowtieMapping> mappingsTable = mappings.parallelDo(
        new BowtieDoFns.KeyByContigId(), Avros.tableOf(
            Avros.strings(),
            Avros.specifics(BowtieMapping.class)));

    PTable<String, GraphNodeData> nodesTable = nodes.parallelDo(
        new GraphNodeDoFns.KeyByNodeId(), Avros.tableOf(
            Avros.strings(),
            Avros.specifics(GraphNodeData.class)));

    int numReducers = getConf().getInt("mapred.reduce.tasks", 10);
    PTable<String, Pair<Collection<GraphNodeData>, Collection<BowtieMapping>>>
      joined = Cogroup.cogroup(numReducers, nodesTable, mappingsTable);

    PCollection<ContigReadStats> stats = joined.parallelDo(
        new ComputeStatsDo(), Avros.specifics(ContigReadStats.class));
    stats.write(To.avroFile(outputPath));

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new BowtieAlignmentStats(), args);
    System.exit(res);
  }
}
