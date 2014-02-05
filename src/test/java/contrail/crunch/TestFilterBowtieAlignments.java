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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.avro.specific.SpecificData;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import contrail.scaffolding.BowtieMapping;

public class TestFilterBowtieAlignments {
  @Test
  public void testPipeline() {
    // Run the entire pipeline to make sure mappings are properly filtered.

    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();

    ArrayList<BowtieMapping> expectedOutput = new ArrayList<BowtieMapping>();
    // Mate pair to keep.
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId/1");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);

      expectedOutput.add(SpecificData.get().deepCopy(
          mapping.getSchema(), mapping));
    }

    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId/2");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig2");
      mappings.add(mapping);

      expectedOutput.add(SpecificData.get().deepCopy(
          mapping.getSchema(), mapping));
    }

    // Mate pair with only one aligned read.
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId2/1");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);
    }


    // Both reads align to same contig.
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId3/1");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);
    }

    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId3/2");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);
    }

    PCollection<BowtieMapping> input =
        MemPipeline.typedCollectionOf(
            Avros.specifics(BowtieMapping.class),
                mappings);

    PCollection<BowtieMapping> outputs =
        FilterBowtieAlignments.buildFilterPipeline(input);

    Iterable<BowtieMapping> materializedOutput = outputs.materialize();

    Collections.reverse(expectedOutput);
    assertEquals(expectedOutput, materializedOutput);
  }
}
