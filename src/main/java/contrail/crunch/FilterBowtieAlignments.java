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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificData;
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

import contrail.crunch.MatePairGraph.KeyByMateIdDo;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.MatePairMappings;
import contrail.sequences.ReadIdUtil;
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
   * Build a record conting all the alignments for a given mate pair.
   *
   * Input is keyed by the id for a mate pair. The values are the alignments
   * of the reads associated with that mate pair to contigs.
   *
   */
  public static class BuildMatePairMappings
      extends DoFn<Pair<String, Iterable<BowtieMapping>>,
                   MatePairMappings> {
    @Override
    public void process(
        Pair<String, Iterable<BowtieMapping>> pair,
        Emitter<MatePairMappings> emitter) {
      MatePairMappings mateData = new MatePairMappings();
      mateData.setMateId(pair.first());
      mateData.setLeftMappings(new ArrayList<BowtieMapping>());
      mateData.setRightMappings(new ArrayList<BowtieMapping>());

      this.increment("Contrail", "mates");

      // Keep track of the mappings associated with each suffix.
      HashMap<String, ArrayList<BowtieMapping>> suffixContigs =
          new HashMap<String, ArrayList<BowtieMapping>>();

      for (BowtieMapping mapping : pair.second()) {
        String suffix =
            ReadIdUtil.getMatePairSuffix(mapping.getReadId().toString());

        if (!suffixContigs.containsKey(suffix)) {
          suffixContigs.put(suffix, new ArrayList<BowtieMapping>());
        }

       suffixContigs.get(suffix).add(SpecificData.get().deepCopy(
           mapping.getSchema(), mapping));
      }

      // Get the links.
      if (suffixContigs.size() > 2) {
        // Error.
        this.increment("Contrail-errors", "too-many-mate-pair-suffixes");
        return;
      }
      if (suffixContigs.size() == 1) {
        // Error.
        this.increment("Contrail-errors", "single-reads");
      }

      Iterator<ArrayList<BowtieMapping>> iter =
          suffixContigs.values().iterator();

      if (iter.hasNext()) {
        mateData.setLeftMappings(iter.next());
      }

      if (iter.hasNext()) {
        mateData.setRightMappings(iter.next()) ;
      } else {
        mateData.setRightMappings(new ArrayList<BowtieMapping>());
      }
      emitter.emit(mateData);
    }
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
