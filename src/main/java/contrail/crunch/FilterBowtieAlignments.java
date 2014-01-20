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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
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
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.MatePairMappings;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;

/**
 * Remove all bowtie mappings if:
 * 1) Both reads align to the same mate pair.
 * 2) Only of the reads in the pair aligns to a contig.
 */
public class FilterBowtieAlignments extends CrunchStage {
  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
         ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Filter the mate pairs.
   */
  public static class FilterMatePairMappings
    extends DoFn<MatePairMappings, MatePairMappings> {
    @Override
    public void process(
        MatePairMappings input,
        Emitter<MatePairMappings> emitter) {

      if (input.getLeftMappings().size() == 0 ||
          input.getRightMappings().size() == 0) {
        this.increment("Contrail-FilterMatePairMappings", "single-reads");
        // Filter it out because both reads align to the same contig.
        return;
      }

      // Check if its a cycle.
      HashSet<String> contigIds = new HashSet<String>();
      for (int i = 0; i < 2; ++i) {
        List<BowtieMapping> mappings = input.getLeftMappings();
        if (i == 1) {
          mappings = input.getRightMappings();
        }
        for (BowtieMapping m : mappings) {
          contigIds.add(m.getContigId().toString());
        }
      }

      if (contigIds.size() == 1) {
        this.increment("Contrail-FilterMatePairMappings", "cycle");
        return;
      }

      emitter.emit(input);
    }
  }

  /**
   * Extract the bowtie mappings.
   */
  public static class ExtractMappings
      extends DoFn<MatePairMappings, BowtieMapping> {
    @Override
    public void process(
        MatePairMappings input,
        Emitter<BowtieMapping> emitter) {

      for (int i = 0; i < 2; ++i) {
        List<BowtieMapping> mappings = input.getLeftMappings();
        if (i == 1) {
          mappings = input.getRightMappings();
        }
        for (BowtieMapping m : mappings) {
          emitter.emit(m);
        }
      }
    }
  }

  public static PCollection<BowtieMapping> buildFilterPipeline(
      PCollection<BowtieMapping> inputMappings) {

    PGroupedTable<String, BowtieMapping> alignments = inputMappings.parallelDo(
        new KeyByMateIdDo(), Avros.tableOf(
            Avros.strings(),
                Avros.specifics(BowtieMapping.class))).groupByKey();

    PCollection<MatePairMappings>  pairedMappings = alignments.parallelDo(
        new BuildMatePairMappings(), Avros.specifics(MatePairMappings.class));

    PCollection<MatePairMappings> filtered = pairedMappings.parallelDo(
        new FilterMatePairMappings(), Avros.specifics(MatePairMappings.class));

    PCollection<BowtieMapping> extracted = filtered.parallelDo(
        new ExtractMappings(), Avros.specifics(BowtieMapping.class));

    return extracted;
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    Source<BowtieMapping> source = From.avroFile(
        inputPath, Avros.specifics(BowtieMapping.class));

    deleteExistingPath(new Path(outputPath));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(MatePairGraph.class, getConf());

    PCollection<BowtieMapping> raw = pipeline.read(source);

    PCollection<BowtieMapping> outputs = buildFilterPipeline(raw);

    outputs.write(To.avroFile(outputPath));

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new FilterBowtieAlignments(), args);
    System.exit(res);
  }
}
