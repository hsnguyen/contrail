/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package contrail.crunch;

import static org.junit.Assert.assertEquals;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import contrail.crunch.GroupByLibrary.ParseReadId;
import contrail.sequences.FastQRecord;
import contrail.sequences.MatePairId;
import contrail.sequences.Read;
import contrail.sequences.ReadId;

public class GroupByLibraryTest {
  @Test
  public void testInMemory() throws Exception {
    Read read1 = new Read();
    Read read2 = new Read();

    read1.setFastq(new FastQRecord());
    read1.getFastq().setId("SRR208.2fsf213/1");
    read1.getFastq().setQvalue("!@#SDF");
    read1.getFastq().setRead("ACTCG");

    // No library.
    read2.setFastq(new FastQRecord());
    read2.getFastq().setId("ABC/2");
    read2.getFastq().setQvalue("!@#SDF");
    read2.getFastq().setRead("ACTCG");

    PCollection<Read> input = MemPipeline.typedCollectionOf(
        Avros.specifics(Read.class), read1, read2);
    PTable<ReadId, Read> outputs = input.parallelDo(
        new ParseReadId(), Avros.tableOf(
            Avros.specifics(ReadId.class),
            Avros.specifics(Read.class)));

    ReadId expected1 = new ReadId();
    expected1.setId("2fsf213");
    expected1.setLibrary("SRR208");
    expected1.setMateId(MatePairId.LEFT);

    ReadId expected2 = new ReadId();
    expected2.setId("ABC");
    expected2.setLibrary("");
    expected2.setMateId(MatePairId.RIGHT);
    
    Iterable<Pair<ReadId, Read>> materializedOutput = outputs.materialize();
    assertEquals(ImmutableList.of(
        new Pair<ReadId, Read>(expected1, read1),
        new Pair<ReadId, Read>(expected2, read2)), materializedOutput);
  }
}
