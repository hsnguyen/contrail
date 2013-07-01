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

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import contrail.graph.EdgeDirection;
import contrail.graph.GraphN50StatsData;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphStatsData;
import contrail.graph.LengthStatsData;
import contrail.sequences.DNAStrand;
import contrail.stages.GraphStats.GraphStatsCombiner;
import contrail.stages.GraphStats.GraphStatsMapper;
import contrail.stages.GraphStats.GraphStatsReducer;
import contrail.util.AvroFileContentsIterator;

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
  public static void combine(
      Iterable<String> baseNames, Iterator<GenericRecord> inputs,
      GenericRecord result) {
    GenericRecord first = inputs.next();
    result.put("count", first.get("count"));
    String countKey = "count";

    result.put(countKey, first.get(countKey));

    while (inputs.hasNext()) {
      GenericRecord item = inputs.next();
      Long resultCount = (Long) result.get(countKey);
      Long itemCount = (Long) item.get(countKey);
      Long newCount = resultCount + itemCount;
      result.put(countKey, newCount);

      for (String baseName : baseNames) {
        String meanName = baseName + "_mean";
        String minName = baseName + "_min";
        String maxName = baseName + "_max";

        Double itemValue = (Double) item.get(baseName);
        Double resultValue = (Double) result.get(baseName);

        Double mean = (itemValue * itemCount + resultValue * resultCount) /
            newCount;
        result.put(meanName, mean);

        result.put(minName, Math.min(itemValue, resultValue));
        result.put(maxName, Math.max(itemValue, resultValue));
      }
    }
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
      implements Iterator<GenericRecord>{
    private final Iterator<AvroValue<LengthStatsData>> iter;

    public GenericIterator(Iterator<AvroValue<LengthStatsData>> other) {
      iter = other;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public GenericRecord next() {
      return (GenericRecord) iter.next();
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
      LengthStatsData result = new LengthStatsData();
      result.setLength(length.datum());

      GenericIterator iterator = new GenericIterator(values);

      combine(BASE_NAMES, iterator, (GenericRecord) result);

      outStream.reset();
      writer.write(result, encoder);
      encoder.flush();
      outStream.flush();
      outKey.set(outStream.toByteArray());

      collector.collect(outKey, NullWritable.get());
    }
  }

  /**
   * Compute the statistics given the data for each bin.
   *
   * This function runs after the mapreduce stage has completed. It takes
   * as input the sufficient statistics for each bin and computes the final
   * statistics.
   *
   * @param iterator: An iterator over the GraphStatsData where each
   *   GraphStatsData contains the data for a different bin. The bins
   *   should be sorted in descending order with respect to the lengths
   *   of the contigs.
   */
  protected static ArrayList<GraphN50StatsData> computeN50Stats(
      Iterator<GraphStatsData> binsIterator) {
    // The output is an array of GraphN50StatsData. Each record gives the
    // N50 stats for a different bin.
    ArrayList<GraphN50StatsData> outputs = new ArrayList<GraphN50StatsData>();

    // Keep a running sum of the lengths across bins.
    long binLengthsSum = 0;

    // Keep a running total of the weighted coverage and degree.
    double binCoverageSum = 0;
    double binDegreeSum = 0;

    // A list of the lengths of the contigs in descending order.
    // This allows us to find the N50 length.
    ArrayList<Integer> contigLengths = new ArrayList<Integer>();

    // This is the index into contigLengths where we continue summing.
    Integer contigIndex = -1;

    // Keep track of  sum(contigLengths[0],..., contigLengths[contigIndex]).
    long contigSum = 0;

    while (binsIterator.hasNext()) {
      GraphStatsData binData = binsIterator.next();
      if (outputs.size() > 0) {
        // Make sure we are sorted in descending order.
        GraphN50StatsData lastBin = outputs.get(outputs.size() - 1);

        if (binData.getLengths().get(0) > lastBin.getMinLength()) {
          throw new RuntimeException(
              "The bins aren't sorted in descending order with respect to " +
              "the contig lengths.");
        }
      }

      contigLengths.addAll(binData.getLengths());
      binLengthsSum += binData.getLengthSum();

      binCoverageSum += binData.getCoverageSum();
      binDegreeSum += (double) binData.getDegreeSum();

      // Compute the N50 length for this value.
      double N50Length = binLengthsSum / 2.0;

      // Continue iterating over the sequences in descending order until
      // we reach a sequence such that the running sum is >= N50Length.
      while (contigSum < N50Length) {
        ++contigIndex;
        contigSum += contigLengths.get(contigIndex);
      }

      // So at this point contigIndex corresponds to the index of the N50
      // cutoff.
      GraphN50StatsData n50Data = new GraphN50StatsData();
      n50Data.setN50Length(contigLengths.get(contigIndex));
      n50Data.setMaxLength(contigLengths.get(0));
      n50Data.setMinLength(contigLengths.get(contigLengths.size() - 1));
      n50Data.setLengthSum(binLengthsSum);
      n50Data.setNumContigs((long) contigLengths.size());
      n50Data.setN50Index(contigIndex);
      n50Data.setMeanCoverage(binCoverageSum / binLengthsSum);
      n50Data.setMeanDegree(binDegreeSum / binLengthsSum);
      outputs.add(n50Data);
    }

    return outputs;
  }

  /**
   * Create an iterator to iterate over the output of the MR job.
   * @return
   */
  protected AvroFileContentsIterator<GraphStatsData> createOutputIterator() {
    String outputDir = (String) stage_options.get("outputpath");
    ArrayList<String> files = new ArrayList<String>();
    FileSystem fs = null;
    try{
      Path outputPath = new Path(outputDir);
      fs = FileSystem.get(this.getConf());
      for (FileStatus status : fs.listStatus(outputPath)) {
        String fileName = status.getPath().getName();
        if (fileName.startsWith("part-") && fileName.endsWith("avro")) {
          files.add(status.getPath().toString());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }
    return new AvroFileContentsIterator<GraphStatsData>(files, getConf());
  }

  /**
   * Get the lengths of the top N contigs.
   *
   * @param iterator: An iterator over the GraphStatsData where each
   *   GraphStatsData contains the data for a different bin. The bins
   *   should be sorted in descending order with respect to the lengths
   *   of the contigs.
   * @param topN: number of sequences to return.
   */
  protected List<Integer> topNContigs(
      Iterator<GraphStatsData> binsIterator, int topN) {
    ArrayList<Integer> outputs = new ArrayList<Integer>();

    while (binsIterator.hasNext() && (outputs.size() < topN)) {
      GraphStatsData binData = binsIterator.next();
      Iterator<Integer> contigIterator = binData.getLengths().iterator();
      int lastLength = Integer.MAX_VALUE;
      while (contigIterator.hasNext() && (outputs.size() < topN)) {
        int length = contigIterator.next();
        if (length > lastLength) {
          throw new RuntimeException(
              "The bins aren't sorted in descending order with respect to " +
              "the contig lengths.");
        }
        outputs.add(length);
        lastLength = length;
      }
    }
    return outputs;
  }

  protected void writeN50StatsToFile(ArrayList<GraphN50StatsData> records) {
    String outputDir = (String) stage_options.get("outputpath");
    Path outputPath = new Path(outputDir, "n50stats.avro");

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    // Write the data to the file.
    Schema schema = records.get(0).getSchema();
    DatumWriter<GraphN50StatsData> datumWriter =
        new SpecificDatumWriter<GraphN50StatsData>(schema);
    DataFileWriter<GraphN50StatsData> writer =
        new DataFileWriter<GraphN50StatsData>(datumWriter);

    try {
      FSDataOutputStream outputStream = fs.create(outputPath);
      writer.create(schema, outputStream);
      for (GraphN50StatsData stats: records) {
        writer.append(stats);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the N50 stats to an avro file. " +
          "Exception: " + exception.getMessage());
    }
  }

  protected void writeTopNContigs(List<Integer> lengths) {
    String outputDir = (String) stage_options.get("outputpath");
    Path outputPath = new Path(outputDir, "topn_contigs.avro");

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    // Write the data to the file.
    Schema schema = Schema.create(Schema.Type.INT);
    DatumWriter<Integer> datumWriter =
        new SpecificDatumWriter<Integer>(schema);
    DataFileWriter<Integer> writer =
        new DataFileWriter<Integer>(datumWriter);

    try {
      FSDataOutputStream outputStream = fs.create(outputPath);
      writer.create(schema, outputStream);
      for (Integer record: lengths) {
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the top N lengths to an avro file. " +
          "Exception: " + exception.getMessage());
    }
  }

  /**
   * Create an HTML report to describe the result.
   */
  protected void writeReport(
      long numNodes, ArrayList<GraphN50StatsData> n50Records) {
    // We create a temporary local file to write the data to.
    // We then copy that file to the output path which could be on HDFS.
    File temp = null;
    try {
      temp = File.createTempFile("temp",null);
    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
    }

    String outputDir = (String) stage_options.get("outputpath");
    Path outputPath = new Path(outputDir, "report.html");

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    try {
      FileWriter fileWriter = new FileWriter(temp);
      BufferedWriter writer = new BufferedWriter(fileWriter);

      //writer.create(schema, outputStream);
      writer.append("<html><body>");
      writer.append(String.format("Number of nodes:%d", numNodes));
      writer.append("<br>");
      writer.append("Path:" + (String)stage_options.get("inputpath"));
      writer.append("<br><br>");
      writer.append("N50 Statistics");
      writer.append("<table border=1>");
      writer.append(
          "<tr><td>Min Length</td><td>Max Length</td><td>Length Sum</td>" +
          "<td>N50 Length</td><td>N50 Index</td><td>Num Contigs</td>" +
          "<td>Mean Coverage</td><td>MeanDegree</td></tr>");
      for (GraphN50StatsData record: n50Records) {
        writer.append(String.format("<td>%d</td>", record.getMinLength()));
        writer.append(String.format("<td>%d</td>", record.getMaxLength()));
        writer.append(String.format("<td>%d</td>", record.getLengthSum()));
        writer.append(String.format("<td>%d</td>", record.getN50Length()));
        writer.append(String.format("<td>%d</td>", record.getN50Index()));
        writer.append(String.format("<td>%d</td>", record.getNumContigs()));
        writer.append(String.format("<td>%f</td>", record.getMeanCoverage()));
        writer.append(String.format("<td>%f</td>", record.getMeanDegree()));
        writer.append("<tr>");
      }
      writer.append("</table>");
      writer.append("</body></html>");
      writer.close();
      fs.moveFromLocalFile(new Path(temp.toString()), outputPath);

      sLogger.info("Wrote HTML report to: " + outputPath.toString());
    } catch (IOException exception) {
      fail("There was a problem writing the html report. " +
          "Exception: " + exception.getMessage());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    Pair<Integer, GraphStatsData> mapOutput =
        new Pair<Integer, GraphStatsData> (0, new GraphStatsData());

    GraphNodeData nodeData = new GraphNodeData();
    AvroJob.setInputSchema(conf, nodeData.getSchema());
    AvroJob.setMapOutputSchema(conf, mapOutput.getSchema());
    AvroJob.setOutputSchema(conf, mapOutput.value().getSchema());

    AvroJob.setMapperClass(conf, GraphStatsMapper.class);
    AvroJob.setCombinerClass(conf, GraphStatsCombiner.class);
    AvroJob.setReducerClass(conf, GraphStatsReducer.class);

    // Use a single reducer task that we accumulate all the stats in one
    // reducer.
    conf.setNumReduceTasks(1);
  }

  @Override
  protected void postRunHook() {
 // Create iterators to read the output
    Iterator<GraphStatsData> binsIterator = createOutputIterator();

    // Compute the N50 stats for each bin.
    ArrayList<GraphN50StatsData> N50Stats = computeN50Stats(binsIterator);

    // Write the N50 stats to a file.
    writeN50StatsToFile(N50Stats);

    // Create an HTML report.
    long numNodes = -1;
    try {
        numNodes = job.getCounters().findCounter(
            "org.apache.hadoop.mapred.Task$Counter",
            "MAP_INPUT_RECORDS").getValue();
    } catch (IOException e) {
      sLogger.fatal("Couldn't get counters.", e);
      System.exit(-1);
    }

    writeReport(numNodes, N50Stats);
    Integer topn_contigs = (Integer) stage_options.get("topn_contigs");
    if (topn_contigs > 0) {
      // Get the lengths of the n contigs.
      binsIterator = createOutputIterator();
      List<Integer> topN = topNContigs(binsIterator, topn_contigs);
      writeTopNContigs(topN);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LengthStats(), args);
    System.exit(res);
  }
}
