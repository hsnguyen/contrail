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
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.crunch;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigLink;
import contrail.scaffolding.ContigNode;

public class TestMatePairGraph extends MatePairGraph {
  @Test
  public void testBuildNodes() {
    ArrayList<Pair<String, BowtieMapping>> inPairs =
        new ArrayList<Pair<String, BowtieMapping>>();
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
      inPairs.add(new Pair<String, BowtieMapping>("mate", mapping));
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
      inPairs.add(new Pair<String, BowtieMapping>("mate", mapping));
    }

    PTable<String, BowtieMapping> input =
        MemPipeline.typedTableOf(
            Avros.tableOf(
                Avros.strings(),
                Avros.specifics(BowtieMapping.class)),
                inPairs);

    PGroupedTable<String, BowtieMapping> grouped = input.groupByKey();

    PTable<String, ContigNode> links = grouped.parallelDo(
        new BuildNodes(), Avros.tableOf(
            Avros.strings(),
            Avros.specifics(ContigNode.class)));


    Iterable<Pair<String, ContigNode>> materializedOutput = links.materialize();

    Pair<String, ContigNode> expected1 = null;
    {
      ContigNode node = new ContigNode();
      node.setContigId("contig1");
      node.setLinks(new ArrayList<ContigLink>());

      ContigLink link = new ContigLink();
      link.setContigId("contig2");
      link.setNumMatePairs(1);
      node.getLinks().add(link);

      expected1 = new Pair<String, ContigNode>(
          node.getContigId().toString(), node);
    }

    Pair<String, ContigNode> expected2 = null;
    {
      ContigNode node = new ContigNode();
      node.setContigId("contig2");
      node.setLinks(new ArrayList<ContigLink>());

      ContigLink link = new ContigLink();
      link.setContigId("contig1");
      link.setNumMatePairs(1);
      node.getLinks().add(link);

      expected2 = new Pair<String, ContigNode>(
          node.getContigId().toString(), node);
    }

    assertEquals(ImmutableList.of(expected2, expected1), materializedOutput);
  }

  @Test
  public void testCombineNodes() {
    ArrayList<Pair<String, ContigNode>> inPairs =
        new ArrayList<Pair<String, ContigNode>>();
    {
      ContigNode node = new ContigNode();
      node.setContigId("contig1");
      node.setLinks(new ArrayList<ContigLink>());

      ContigLink link = new ContigLink();
      link.setContigId("contig2");
      link.setNumMatePairs(1);
      node.getLinks().add(link);
      inPairs.add(new Pair<String, ContigNode>(
          node.getContigId().toString(), node));
    }

    {
      ContigNode node = new ContigNode();
      node.setContigId("contig1");
      node.setLinks(new ArrayList<ContigLink>());

      ContigLink link = new ContigLink();
      link.setContigId("contig3");
      link.setNumMatePairs(1);
      node.getLinks().add(link);
      inPairs.add(new Pair<String, ContigNode>(
          node.getContigId().toString(), node));
    }

    PTable<String, ContigNode> input =
        MemPipeline.typedTableOf(
            Avros.tableOf(
                Avros.strings(),
                Avros.specifics(ContigNode.class)),
                inPairs);

    PGroupedTable<String, ContigNode> grouped = input.groupByKey();

    PCollection<ContigNode> nodes = grouped.parallelDo(
        new CombineNodes(),  Avros.specifics(ContigNode.class));

    Iterable<ContigNode> materializedOutput = nodes.materialize();
    ArrayList<ContigNode> outputs = new ArrayList<ContigNode>();
    for (ContigNode node : materializedOutput) {
      outputs.add(node);
    }

    ContigNode expected1 = new ContigNode();
    {
      expected1.setContigId("contig1");
      expected1.setLinks(new ArrayList<ContigLink>());

      ContigLink link2 = new ContigLink();
      link2.setContigId("contig3");
      link2.setNumMatePairs(1);
      expected1.getLinks().add(link2);

      ContigLink link = new ContigLink();
      link.setContigId("contig2");
      link.setNumMatePairs(1);
      expected1.getLinks().add(link);
    }

    assertEquals(ImmutableList.of(expected1), materializedOutput);
  }

  @Test
  public void testDegreeCounts() {
    // Run the entire pipeline to count the degree.

    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();
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
    }

    PCollection< BowtieMapping> input =
        MemPipeline.typedCollectionOf(
            Avros.specifics(BowtieMapping.class),
                mappings);

    Outputs outputs = buildDegreePipeline(input);


    Iterable<Pair<Integer, Long>> materializedOutput =
        outputs.degreeCounts.materialize();

    assertEquals(
        ImmutableList.of(new Pair<Integer, Long>(1, 2L)),
        materializedOutput);
  }
}
