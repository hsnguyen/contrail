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
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.scaffolding.BowtieDoFns.KeyByMateIdDo;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigLink;
import contrail.scaffolding.ContigNode;
import contrail.sequences.ReadIdUtil;
import contrail.stages.ContrailParameters;
import contrail.stages.CrunchStage;
import contrail.stages.ParameterDefinition;

/**
 * Constructs the graph formed by mate pairs.
 */
public class MatePairGraph extends CrunchStage {
  private static final Logger sLogger = Logger.getLogger(
      MatePairGraph.class);

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
   * Convert a sequence of alignments into a ContigNode representing two
   * contigs linked by a mate pair.
   *
   * Input is keyed by the id for a mate pair. The values are the alignments
   * of the reads associated with that mate pair to contigs.
   *
   * The output is the id of a contig. The value is ContigNode representing
   * that node. The ContigNode contains links to other nodes in the graph.
   * The links are formed by the mate pairs.
   *
   * TODO(jeremy@lewi.us): We should probably reuse
   * FilterBowtieAlignments.BuildMatePairMappings.
   */
  public static class BuildNodes
      extends DoFn<Pair<String, Iterable<BowtieMapping>>,
                   Pair<String, ContigNode>> {
    @Override
    public void process(
        Pair<String, Iterable<BowtieMapping>> pair,
        Emitter<Pair<String, ContigNode>> emitter) {
      this.increment("Contrail", "mates");

      // Keep track of the reads that the left and right mates align to.
      // The key is the mate pair suffix. The value is the set of contigs
      // that read aligns to.
      HashMap<String, HashSet<String>> suffixContigs =
          new HashMap<String, HashSet<String>>();

      for (BowtieMapping mapping : pair.second()) {
        String suffix =
            ReadIdUtil.getMatePairSuffix(mapping.getReadId().toString());

        if (!suffixContigs.containsKey(suffix)) {
          suffixContigs.put(suffix, new HashSet<String>());
        }

        HashSet<String> contigs = suffixContigs.get(suffix);

        String contigId = mapping.getContigId().toString();
        contigs.add(contigId);
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
        return;
      }

      Iterator<HashSet<String>> iter = suffixContigs.values().iterator();
      HashSet<String> left = iter.next();
      HashSet<String> right = iter.next();

      // Take the cross product and emit two contigs for each pairing.
      for (String leftContig : left) {
        for (String rightContig : right) {
          if (leftContig.equals(rightContig)) {
            increment("Contrail", "self-links", 1);
          }

          {
            ContigNode node = new ContigNode();
            node.setLinks(new ArrayList<ContigLink>());

            ContigLink link = new ContigLink();
            node.getLinks().clear();
            node.setContigId(leftContig);
            node.getLinks().add(link);
            link.setContigId(rightContig);
            link.setNumMatePairs(1);
            emitter.emit(new Pair<String, ContigNode>(
                node.getContigId().toString(), node));
          }

          {
            ContigNode node = new ContigNode();
            node.setLinks(new ArrayList<ContigLink>());

            ContigLink link = new ContigLink();
            node.getLinks().clear();
            node.setContigId(rightContig);
            node.getLinks().add(link);
            link.setContigId(leftContig);
            link.setNumMatePairs(1);
            emitter.emit(new Pair<String, ContigNode>(
                node.getContigId().toString(), node));
          }

        }
      }
    }
  }

  /**
   * Combine ContigNodes.
   */
  public static class CombineNodes
      extends DoFn<Pair<String, Iterable<ContigNode>>, ContigNode> {
    @Override
    public void process(
        Pair<String, Iterable<ContigNode>> pair, Emitter<ContigNode> emitter) {
      // Count the number of links to some other contig.
      HashMap<String, Integer> linkCounts = new HashMap<String, Integer>();

      for (ContigNode other : pair.second()) {
        for (ContigLink link : other.getLinks()) {
          if (!linkCounts.containsKey(link.getContigId().toString())) {
            linkCounts.put(link.getContigId().toString(), 0);
          }
          linkCounts.put(
              link.getContigId().toString(),
              linkCounts.get(link.getContigId().toString()) + 1);
        }
      }

      ContigNode node = new ContigNode();
      node.setContigId(pair.first());
      node.setLinks(new ArrayList<ContigLink>());

      for (String other : linkCounts.keySet()) {
        ContigLink link = new ContigLink();
        link.setContigId(other);
        link.setNumMatePairs(linkCounts.get(other));
        node.getLinks().add(link);
      }
      emitter.emit(node);
    }
  }

  /**
   * Compute the degree of a node.
   */
  public static class Degree
      extends DoFn<ContigNode, Integer> {
    @Override
    public void process(
        ContigNode node, Emitter<Integer> emitter) {
      emitter.emit(node.getLinks().size());
    }
  }

  protected static class Outputs {
    PCollection<ContigNode> nodes;
    PTable<Integer, Long> degreeCounts;
  }

  protected Outputs buildDegreePipeline(
      PCollection<BowtieMapping> mappings) {
    PGroupedTable<String, BowtieMapping> alignments = mappings.parallelDo(
        new KeyByMateIdDo(), Avros.tableOf(
            Avros.strings(),
                Avros.specifics(BowtieMapping.class))).groupByKey();

    PGroupedTable<String, ContigNode>  links = alignments.parallelDo(
        new BuildNodes(), Avros.tableOf(
            Avros.strings(),
            Avros.specifics(ContigNode.class))).groupByKey();

    PCollection<ContigNode> nodes = links.parallelDo(
        new CombineNodes(), Avros.specifics(ContigNode.class));

    PTable<Integer, Long> degreeCounts = nodes.parallelDo(
        new Degree(), Avros.ints()).count();

    Outputs outputs = new Outputs();
    outputs.degreeCounts = degreeCounts;
    outputs.nodes = nodes;
    return outputs;
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

    Outputs outputs = buildDegreePipeline(raw);

    String nodesPath = FilenameUtils.concat(outputPath, "Nodes");
    outputs.nodes.write(To.avroFile(nodesPath));

    String degreeCountsPath = FilenameUtils.concat(outputPath, "DegreeCounts");
    outputs.degreeCounts.write(To.avroFile(degreeCountsPath));

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new MatePairGraph(), args);
    System.exit(res);
  }
}
