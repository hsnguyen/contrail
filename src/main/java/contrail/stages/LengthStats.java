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
// Author: Michael Schatz, Jeremy Lewi (jeremy@lewi.us)

package contrail.stages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import contrail.graph.EdgeDirection;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.LengthStatsData;
import contrail.sequences.DNAStrand;

/**
 * Group contigs by length and compute some basic statistics for
 * the contigs at each length.
 *
 */
public class LengthStats extends MRStage {
  private static final Logger sLogger = Logger.getLogger(LengthStats.class);

  public static final List<String> BASE_NAMES = Collections.unmodifiableList(
      Arrays.asList(new String[]{"coverage", "degree"}));
  /**
   * Compute the mean, max, and min of two records. Assumes the fields are
   * named
   * baseName + ["_mean", "_max", "_min"]. and there is a field named "count"
   * containing the count.
   *
   * TODO(jeremy@lewi.us): We could probably generalize this to a class
   * and allow the user to specify the prefixes to use for each field.
   *
   * @param baseName
   * @param left
   * @param right
   * @param result
   */
  public static LengthStatsData combine(
      Iterable<String> baseNames, Iterator<LengthStatsData> inputs) {
    LengthStatsData first = inputs.next();
    LengthStatsData result =
        SpecificData.get().deepCopy(first.getSchema(), first);

    Long count = first.getCount();
    Double coverageTotal = result.getCoverageMean() * count;
    Double degreeTotal = result.getDegreeMean() * count;

    while (inputs.hasNext()) {
      LengthStatsData item = inputs.next();
      Long itemCount = item.getCount();
      count += itemCount;

      coverageTotal += item.getCoverageMean() * itemCount;
      degreeTotal += item.getDegreeMean() * itemCount;

      result.setCoverageMax(
          Math.max(result.getCoverageMax(), item.getCoverageMax()));
      result.setCoverageMin(
          Math.min(result.getCoverageMin(), item.getCoverageMin()));
      result.setDegreeMax(
          Math.max(result.getDegreeMax(), item.getDegreeMax()));
      result.setDegreeMin(
          Math.min(result.getDegreeMin(), item.getDegreeMin()));
    }

    result.setCount(count);
    result.setCoverageMean(coverageTotal / count);
    result.setDegreeMean(degreeTotal / count);
    return result;
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      definitions.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(definitions);
  }

  protected static class StatsMapper extends
      AvroMapper<GraphNodeData, Pair<Integer, LengthStatsData>> {
    private LengthStatsData graphStats;
    private GraphNode node;
    Pair<Integer, LengthStatsData> outPair;

    @Override
    public void configure(JobConf job) {
      graphStats = new LengthStatsData();
      node = new GraphNode();
      outPair = new Pair<Integer, LengthStatsData>(-1, graphStats);
    }

    @Override
    public void map(GraphNodeData nodeData,
        AvroCollector<Pair<Integer, LengthStatsData>> collector,
        Reporter reporter) throws IOException {
      node.setData(nodeData);
      long len     = node.getSequence().size();
      int fdegree = node.getEdgeTerminals(
          DNAStrand.FORWARD, EdgeDirection.OUTGOING).size();
      int rdegree = node.getEdgeTerminals(
          DNAStrand.REVERSE, EdgeDirection.OUTGOING).size();
      double cov   = node.getCoverage();


      graphStats.setCount(1L);
      graphStats.setLength(len);
      graphStats.setCoverageMean(cov);
      graphStats.setCoverageMax(cov);
      graphStats.setCoverageMin(cov);

      double degree = fdegree + rdegree;
      graphStats.setDegreeMean(degree);
      graphStats.setDegreeMax(degree);
      graphStats.setDegreeMin(degree);

      // The output key is the negative of the bin index so that we
      // sort the bins in descending order.
      outPair.key((int) len);
      collector.collect(outPair);
    }
  }

  public static class GenericIterator
      implements Iterator<LengthStatsData>{
    private final Iterator<AvroValue<LengthStatsData>> iter;

    public GenericIterator(Iterator<AvroValue<LengthStatsData>> other) {
      iter = other;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public LengthStatsData next() {
      return iter.next().datum();
    }

    @Override
    public void remove() {
      throw new NotImplementedException();
    }
  }

  public static class StatsReducer extends MapReduceBase
      implements Reducer<AvroKey<Long>, AvroValue<LengthStatsData>,
                         Text, NullWritable>  {

    private Text outKey;
    private ObjectMapper jsonMapper;
    private Schema schema;
    private DatumWriter<Object> writer;
    private ByteArrayOutputStream outStream;
    private JsonGenerator generator;
    private JsonEncoder encoder;

    @Override
    public void configure(JobConf job) {
      outKey = new Text();
      jsonMapper = new ObjectMapper();

      schema = (new LengthStatsData()).getSchema();

      writer = new GenericDatumWriter<Object>(schema);

      outStream = new ByteArrayOutputStream();
      JsonFactory factory = new JsonFactory();
      try {
        generator = factory.createJsonGenerator(outStream);
      } catch (IOException e) {
        sLogger.fatal("Could not create json generator.", e);
      }

      try {
        encoder = EncoderFactory.get().jsonEncoder(schema, generator);
      } catch (IOException e) {
        sLogger.fatal("Could not create avro encoder.", e);
      }
    }

    @Override
    public void close() {
      try {
        outStream.close();
      } catch (IOException e) {
        sLogger.fatal("Could not close the output stream.", e);
      }
    }

    @Override
    public void reduce(AvroKey<Long> length,
        Iterator<AvroValue<LengthStatsData>> values,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
        throws IOException {
      GenericIterator iterator = new GenericIterator(values);

      LengthStatsData result = combine(BASE_NAMES, iterator);

      outStream.reset();
      writer.write(result, encoder);
      encoder.flush();
      outStream.flush();
      outKey.set(outStream.toByteArray());

      collector.collect(outKey, NullWritable.get());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    Pair<Long, LengthStatsData> mapOutput =
        new Pair<Long, LengthStatsData> (0, new LengthStatsData());

    AvroJob.setInputSchema(conf, new GraphNodeData().getSchema());
    AvroJob.setMapOutputSchema(conf, mapOutput.getSchema());
    AvroJob.setOutputSchema(conf, mapOutput.value().getSchema());

    AvroJob.setMapperClass(conf, StatsMapper.class);

    conf.setReducerClass(StatsReducer.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LengthStats(), args);
    System.exit(res);
  }
}
