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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.crunch.BowtieDoFns.BuildMatePairMappings;
import contrail.crunch.MatePairGraph.KeyByMateIdDo;
import contrail.graph.GraphUtil;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.MatePairMappings;
import contrail.stages.ParameterDefinition;

/**
 * Compute records describing the edges between contigs formed by mate pairs.
 */
public class MatePairEdges extends CrunchStage {
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

    ParameterDefinition outputPath =
        new ParameterDefinition(
            "outputpath", "The directory to write the outputs which are " +
            "the files to pass to bambus for scaffolding.",
            String.class, null);

    for (ParameterDefinition def: Arrays.asList(bowtieAlignments, outputPath)) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Form the ContigEdges.
   */
  public static class BuildContigEdges
      extends DoFn<MatePairMappings, ContigEdge> {
    @Override
    public void process(
        MatePairMappings mappings, Emitter<ContigEdge> emitter) {
      for (BowtieMapping left : mappings.getLeftMappings()) {
        for (BowtieMapping right : mappings.getRightMappings()) {
          ContigEdge edge = new ContigEdge();
          BowtieMapping major = null;
          BowtieMapping minor = null;
          CharSequence majorId = GraphUtil.computeMajorID(
              left.getContigId(), right.getContigId());

          if (left.getContigId().equals(majorId.toString())) {
            major = left;
            minor = right;
          } else {
            major = right;
            minor = left;
          }

          edge.setMajorContigId(major.getContigId().toString());
          edge.setMinorContigId(minor.getContigId().toString());
          edge.setNumLinks(1);
          emitter.emit(edge);
        }
      }
    }
  }


  /**
   * Key edges by major minor id so we can combine them.
   */
  public static class KeyByContigIds
      extends DoFn<ContigEdge, Pair<String, ContigEdge>> {
    @Override
    public void process(
        ContigEdge edge, Emitter<Pair<String, ContigEdge>> emitter) {
      String key =
          edge.getMajorContigId().toString() + "-"  + edge.getMinorContigId();
      emitter.emit(new Pair<String, ContigEdge>(key, edge));
    }
  }

  /**
   * Combine edges between the same contigs.
   */
  public static class CombineEdges
      extends DoFn<Pair<String, Iterable<ContigEdge>>, ContigEdge> {
    @Override
    public void process(
        Pair<String, Iterable<ContigEdge>> pair, Emitter<ContigEdge> emitter) {
      ContigEdge output = new ContigEdge();

      Iterator<ContigEdge> iterator = pair.second().iterator();
      ContigEdge edge = iterator.next();
      output.setMajorContigId(edge.getMajorContigId());
      output.setMinorContigId(edge.getMinorContigId());
      output.setNumLinks(edge.getNumLinks());

      while (iterator.hasNext()) {
        edge = iterator.next();
        output.setNumLinks(output.getNumLinks() + edge.getNumLinks());
      }
      emitter.emit(output);
    }
  }

  @Override
  protected void stageMain() {
    String bowtiePath = (String) stage_options.get("bowtie_alignments");
    String outputPath = (String) stage_options.get("outputpath");

    Source<BowtieMapping> mappingSource = From.avroFile(
        bowtiePath, Avros.specifics(BowtieMapping.class));

    deleteExistingPath(new Path(outputPath));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(MatePairEdges.class, getConf());

    PCollection<BowtieMapping> mappings = pipeline.read(mappingSource);

    PGroupedTable<String, BowtieMapping> alignments = mappings.parallelDo(
        new KeyByMateIdDo(), Avros.tableOf(
            Avros.strings(),
                Avros.specifics(BowtieMapping.class))).groupByKey();

    PCollection<MatePairMappings>  pairedMappings = alignments.parallelDo(
        new BuildMatePairMappings(), Avros.specifics(MatePairMappings.class));

    PCollection<ContigEdge> edges = pairedMappings.parallelDo(
        new BuildContigEdges(), Avros.specifics(ContigEdge.class));

    PGroupedTable<String, ContigEdge> groupedEdges = edges.parallelDo(
        new KeyByContigIds(), Avros.tableOf(
            Avros.strings(), Avros.specifics(ContigEdge.class))).groupByKey();

    PCollection<ContigEdge> edgeStats = groupedEdges.parallelDo(
        new CombineEdges(), Avros.specifics(ContigEdge.class));

    edgeStats.write(To.avroFile(outputPath));

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new MatePairEdges(), args);
    System.exit(res);
  }
}
