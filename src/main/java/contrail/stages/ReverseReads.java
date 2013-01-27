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
package contrail.stages;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Mapper only job to reverse the reads in a FastQ file.
 *
 * TODO(jeremy@lewi.us): This probably doesn't belong in package scaffolding.
 */
public class ReverseReads extends Stage {
    private static final Logger sLogger = Logger.getLogger(ReverseReads.class);

    /**
     * Mapper.
     */
//    public static class FastqPreprocessorMapper extends MapReduceBase
//    implements Mapper<LongWritable, Text, AvroWrapper<CompressedRead>, NullWritable>
//    {
//      private int idx = 0;
//
//      private String filename = null;
//
//      private int mate_id = 0x0;
//
//      /**
//       *  The alphabet for encoding the sequences.
//       */
//      private Alphabet alphabet;
//
//      private String counter = "pair_unknown";
//
//      private CompressedRead read = new CompressedRead();
//      private AvroWrapper<CompressedRead> out_wrapper =
//          new AvroWrapper<CompressedRead>(read);
//
//      private ByteReplaceAll replacer = null;
//
//      // The byte value to replace multi-byte characters with
//      // this an underscore.
//      public final byte MULTIBYTE_REPLACE_VALUE = 0x5f;
//
//      // The sequence.
//      private Sequence sequence;
//
//      // An array which can be used to tell if a UTF8 value
//      // is whitespace
//      private boolean[] utf8_whitespace;
//
//      // Store the utf8 byte values of various characters
//      private byte utf8_at;
//      private byte utf8_space;
//
//      public void configure(JobConf job)
//      {
//        filename = job.get("map.input.file");
//
//        boolean usesuffix = Integer.parseInt(job.get("PREPROCESS_SUFFIX")) == 1;
//
//        String suffix = null;
//        if (usesuffix)
//        {
//          if  (filename.contains("_1.")) {
//            suffix = "_1";
//            mate_id = 0x1;
//            counter = "pair_1";
//          }
//          else if (filename.contains("_2.")) {
//            suffix = "_2";
//            mate_id = 0x2;
//            counter = "pair_2";
//          }
//          else {
//            counter = "pair_unpaired";
//          }
//
//          System.err.println(filename + " suffix: \"" + suffix + "" + "\"");
//        }
//        read.mate_pair_id = mate_id;
//        replacer = new ByteReplaceAll(":#-.|/$%&'()*+,-./:","_");
//
//        alphabet = DNAAlphabetFactory.create();
//        sequence = new Sequence(alphabet);
//
//        // utf8_whitespace[x] = True for the utf8 charater with value
//        // x, if x is a white space character. False otherwise.
//        utf8_whitespace = new boolean[255];
//        java.util.Arrays.fill(utf8_whitespace, false);
//
//        String white_space = " \n\t";
//        byte[] white_space_bytes = ByteUtil.stringToBytes(white_space);
//
//        for (int pos = 0; pos < white_space_bytes.length; pos++) {
//          utf8_whitespace[pos] = true;
//        }
//
//        utf8_at = ByteUtil.stringToBytes("@")[0];
//        utf8_space = ByteUtil.stringToBytes(" ")[0];
//      }
//
//      public void map(LongWritable lineid, Text line,
//          OutputCollector<AvroWrapper<CompressedRead>, NullWritable> output, Reporter reporter)
//              throws IOException
//              {
//        if (idx == 0)
//        {
//          // We operate on the bytes instead of converting to a string.
//          // The advantage is that we can use our more efficient implementation
//          // for replace all.
//          byte[] data = line.getBytes();
//
//          // Replace any multibyte characters with "_"
//          int valid_length = ByteUtil.replaceMultiByteChars(
//              data, MULTIBYTE_REPLACE_VALUE, line.getLength());
//
//          // make sure it starts with the @ symbol
//          if (data[0] != utf8_at)
//          {
//            throw new IOException("ERROR: Invalid readname: " + line.toString() + " in " + filename);
//          }
//
//          // Find the location of the first space in the name.
//          int end_index = valid_length-1;
//          for (int index = 1; index <= end_index; index++){
//            if (data[index] == utf8_space){
//              end_index = index -1;
//              break;
//            }
//          }
//
//          // Remove any trailing whitespace.
//          while (utf8_whitespace[ByteUtil.byteToUint(data[end_index])]) {
//            end_index--;
//          }
//
//          // Remove the leading '@' and chop everything after the first space.
//          data = java.util.Arrays.copyOfRange(data, 1, end_index+1);
//
//          // Replace any funny characters.
//          replacer.replaceAll(data);
//
//          read.setId(new String(data, ByteReplaceAll.encoding));
//        }
//        else if (idx == 1) {
//          byte[] raw_bytes = line.getBytes();
//          // TODO(jeremy@lewi.us): We should really only be checking the bytes
//          // up to line.getLength()
//          if (ByteUtil.hasMultiByteChars(raw_bytes)){
//            throw new RuntimeException("DNA sequence contained illegal characters. Sequence is: " + line.toString());
//          }
//
//          sequence.readUTF8(raw_bytes, line.getLength());
//          int num_bytes =  (int)Math.ceil((alphabet.bitsPerLetter() * sequence.size())/ 8.0);
//
//          read.setDna(ByteBuffer.wrap(sequence.toPackedBytes(), 0, num_bytes));
//          read.setLength(line.getLength());
//        }
//        else if (idx == 2) {
//        }
//        else if (idx == 3)
//        {
//          output.collect(out_wrapper, NullWritable.get());
//
//          reporter.incrCounter("Contrail", "preprocessed_reads", 1);
//          reporter.incrCounter("Contrail", counter, 1);
//        }
//
//        idx = (idx + 1) % 4;
//      }
//
//      public void close() throws IOException {
//        if (idx != 0)
//        {
//          throw new IOException("ERROR: closing with idx = " + idx + " in " + filename);
//        }
//      }
//    }
//
//    @Override
//    public RunningJob runJob() throws Exception {
//      sLogger.info("Tool name: FastqPreprocessorAvroCompressed");
//      String inputPath = (String) stage_options.get("inputpath");
//      String outputPath = (String) stage_options.get("outputpath");
//      sLogger.info(" - input: "  + inputPath);
//      sLogger.info(" - output: " + outputPath);
//
//      Configuration base_conf = getConf();
//      JobConf conf = null;
//      if (base_conf != null) {
//        conf = new JobConf(getConf(), this.getClass());
//      } else {
//        conf = new JobConf(this.getClass());
//      }
//
//      sLogger.info("mapred.map.tasks=" + conf.get("mapred.map.tasks", ""));
//      conf.setJobName("FastqPreprocessorAvroCompressed " + inputPath);
//
//      // Stage specific configuration options.
//      conf.setLong("PREPROCESS_SUFFIX", 1);
//
//      FileInputFormat.addInputPath(conf, new Path(inputPath));
//      FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//      conf.setInputFormat(TextInputFormat.class);
//
//      conf.setMapOutputKeyClass(Text.class);
//      conf.setMapOutputValueClass(Text.class);
//
//      conf.setMapperClass(FastqPreprocessorMapper.class);
//
//      conf.setInputFormat(NLineInputFormat.class);
//      conf.setInt("mapred.line.input.format.linespermap", 2000000); // must be a multiple of 4
//
//      // This is a mapper only job.
//      conf.setNumReduceTasks(0);
//
//      // TODO(jlewi): use setoutput codec to set the compression codec.
//      AvroJob.setOutputSchema(conf,new CompressedRead().getSchema());
//
//      if (stage_options.containsKey("writeconfig")) {
//        writeJobConfig(conf);
//      } else {
//        //delete the output directory if it exists already
//        FileSystem.get(conf).delete(new Path(outputPath), true);
//
//        long start_time = System.currentTimeMillis();
//        RunningJob job = JobClient.runJob(conf);
//        long numReads =
//            job.getCounters().findCounter(
//                "org.apache.hadoop.mapred.Task$Counter",
//                "MAP_OUTPUT_RECORDS").getValue();
//        sLogger.info("Number of reads:" + numReads);
//        long end_time = System.currentTimeMillis();
//        double nseconds = (end_time - start_time) / 1000.0;
//        sLogger.info("Job took: " + nseconds + " seconds");
//        return job;
//      }
//      return null;
//    }
//
//    /**
//     * Get the options required by this stage.
//     */
//    protected Map<String, ParameterDefinition> createParameterDefinitions() {
//      HashMap<String, ParameterDefinition> defs =
//          new HashMap<String, ParameterDefinition>();
//
//      defs.putAll(super.createParameterDefinitions());
//
//      for (ParameterDefinition def:
//        ContrailParameters.getInputOutputPathOptions()) {
//        defs.put(def.getName(), def);
//      }
//      return Collections.unmodifiableMap(defs);
//    }
//

    public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(
          new Configuration(), new ReverseReads(), args);
      System.exit(res);
    }
}
