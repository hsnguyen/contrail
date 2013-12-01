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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import contrail.sequences.MatePairId;
import contrail.sequences.Read;
import contrail.sequences.ReadId;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;

/**
 * Pipeline to group reads by the library they belong to.
 */
public class GroupByLibrary
    extends CrunchStage implements Tool, Serializable {

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

  public static class ParseReadId extends DoFn<Read, Pair<ReadId, Read>> {
    @Override
    public void process(Read read, Emitter<Pair<ReadId, Read>> emitter) {
      String id = read.getFastq().getId().toString();
      String[] pieces = id.split("\\.");
      ReadId readId = new ReadId();

      String nonLibrary = "";
      if (pieces.length == 1 ) {
         readId.setLibrary("");
         // TODO(jeremy@lewi.us): Add a counter.
         nonLibrary = pieces[0];
         increment("ParseReadId", "error-no-library");
      } else if (pieces.length == 2) {
        nonLibrary= pieces[1];
        readId.setLibrary(pieces[0]);
      } else {
        return;
      }
      String[] matePieces = nonLibrary.split("/");
      if (matePieces.length == 1) {
        readId.setId(matePieces[0]);
        readId.setMateId(null);
        increment("ParseReadId", "single-read");
      } else if (matePieces.length == 2) {
        readId.setId(matePieces[0]);
        Integer mateNum = Integer.parseInt(matePieces[1]);
        switch(mateNum) {
         case 1:
           readId.setMateId(MatePairId.LEFT);
           break;
         case 2:
           readId.setMateId(MatePairId.RIGHT);
           break;
         default:
           readId.setId(nonLibrary);
           increment("ParseReadId", "error-parsing-mate-id");
        }
      }
      emitter.emit(new Pair<ReadId,Read>(readId, read));
    }
  }

  @Override
  protected void stageMain() {
    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(GroupByLibraryTest.class, getConf());

    String inputPath = (String) stage_options.get("inputpath");

    Source<Read> readSource = From.avroFile(inputPath, Read.class);
    PCollection<Read> reads = pipeline.read(readSource);

    // For each read parse the read id into the library name, read, and
    // mate id.
    PTable<ReadId, Read> keyedReads = reads.parallelDo(
        new ParseReadId(), Avros.tableOf(
            Avros.specifics(ReadId.class),
            Avros.specifics(Read.class)));


    // Should we use a secondary sort?

    // Define a function that splits each line in a PCollection of Strings into
    // a
    // PCollection made up of the individual words in the file.
//    PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
//      @Override
//      public void process(String line, Emitter<String> emitter) {
//        for (String word : line.split("\\s+")) {
//          emitter.emit(word);
//        }
//      }
//    }, Writables.strings()); // Indicates the serialization format

    // The count method applies a series of Crunch primitives and returns
    // a map of the unique words in the input PCollection to their counts.
    // Best of all, the count() function doesn't need to know anything about
    // the kind of data stored in the input PCollection.
//    PTable<String, Long> counts = words.count();
//
//    // Instruct the pipeline to write the resulting counts to a text file.
//    pipeline.writeTextFile(counts, args[1]);
//    // Execute the pipeline as a MapReduce.
//    PipelineResult result = pipeline.done();
//
//    return result.succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(
        new Configuration(), new GroupByLibrary(), args);
    System.exit(result);
  }
}

