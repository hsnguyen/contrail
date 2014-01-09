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
import org.apache.log4j.Logger;

import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.MatePairMappings;
import contrail.sequences.ReadIdUtil;
import contrail.sequences.SRRReadIdUtil;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;

/**
 * A crunch pipeline for grouping bowtie alignments by read.
 */
public class GroupBowtieAlignments extends CrunchStage {
  private static final Logger sLogger = Logger.getLogger(
      GroupBowtieAlignments.class);

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
   * Key the bowtie mappings by the read id.
   */
  public static class KeyByMateIdDo extends
      DoFn<BowtieMapping, Pair<String, BowtieMapping>> {
    @Override
    public void process(
        BowtieMapping mapping,
        Emitter<Pair<String, BowtieMapping>> emitter) {
      String mateId = ReadIdUtil.getMateId(mapping.getReadId().toString());
      emitter.emit(new Pair<String, BowtieMapping>(mateId, mapping));
    }
  }

  /**
   * Convert a sequence of alignments into a MatePairMappings object.
   */
  public static class GroupMappings
      extends DoFn<Pair<String, Iterable<BowtieMapping>>, MatePairMappings> {
    @Override
    public void process(
        Pair<String, Iterable<BowtieMapping>> pair,
        Emitter<MatePairMappings> emitter) {
      MatePairMappings mateData = new MatePairMappings();
      mateData.setMateId(pair.first());
      mateData.setMappings(new ArrayList<BowtieMapping>());

      this.getContext().getCounter("Contrail", "mates").increment(1);

      for (BowtieMapping mapping : pair.second()) {
        mateData.getMappings().add(
            SpecificData.get().deepCopy(mapping.getSchema(), mapping));
      }

      // Get the library.
      // TODO(jeremy@lewi.us): We probably don't want to hard code the
      // library id parser. We should take the class name of the parse
      // as a flag.
      mateData.setLibraryId(SRRReadIdUtil.getLibraryId(pair.first()));

      if (mateData.getMappings().size() == 2) {
        // Check if they align to the same contig.
        BowtieMapping left = mateData.getMappings().get(0);
        BowtieMapping right = mateData.getMappings().get(1);

        if (left.getContigId().toString().equals(
            right.getContigId().toString())) {
          // Increment a counter to keep track of number of mate pairs for
          // which both ends align to the same contig.
          this.increment(
              "Contrail", "mates-same-contig");

          // Order the alignments based on where they align to the contig.
          {
            int leftMin = Math.min(left.getContigStart(), left.getContigEnd());
            int rightMin = Math.min(
                right.getContigStart(), right.getContigEnd());
            if (rightMin < leftMin) {
              Collections.reverse(mateData.getMappings());
              left = mateData.getMappings().get(0);
              right = mateData.getMappings().get(1);
            }
          }
          // Since they align to the same contig we can compute the distance
          // and orientation.
          String leftStrand =
              left.getContigStart() <= left.getContigEnd() ? "F" : "R";
          String rightStrand =
              right.getContigStart() <= right.getContigEnd() ? "F" : "R";

          this.increment(
              "Contrail-libraries",
              mateData.getLibraryId() + "-" + leftStrand + rightStrand);
        }
      }

      this.getContext().getCounter(
          "Contrail",
          String.format(
              "mappings-per-mate-%03d",
              mateData.getMappings().size())).increment(1);
      emitter.emit(mateData);
    }
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    Source<BowtieMapping> source = From.avroFile(
        inputPath, Avros.specifics(BowtieMapping.class));

    deleteExistingPath(new Path(outputPath));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(GroupBowtieAlignments.class, getConf());

    PCollection<BowtieMapping> raw = pipeline.read(source);
    PGroupedTable<String, BowtieMapping> alignments = raw.parallelDo(
            new KeyByMateIdDo(), Avros.tableOf(
                Avros.strings(),
                    Avros.specifics(BowtieMapping.class))).groupByKey();

    PCollection<MatePairMappings> mappings = alignments.parallelDo(
        new GroupMappings(), Avros.specifics(MatePairMappings.class));

    mappings.write(To.avroFile(outputPath));

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new GroupBowtieAlignments(), args);
    System.exit(res);
  }
}
