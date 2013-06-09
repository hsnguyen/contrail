/*
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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.io;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import contrail.sequences.FastQRecord;

/**
 * Reader for an indexed
 */
public class AvroFileFastQIndex extends AvroIndexReader<String, FastQRecord> {
  /**
   * Create the indexed graph based on the inputpath.
   * @param inputPath: Path to the graph.
   * @param vSchema: The schema for the value. This must match V or else you
   *   will have problems.
   */
  public AvroFileFastQIndex(String inputPath, Configuration conf) {
    super(inputPath, conf, Schema.create(Schema.Type.STRING),
          (new FastQRecord()).getSchema());
  }
}
