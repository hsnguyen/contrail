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
package contrail.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.NonMRStage;

public class ListHDFSNotInGCS extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      ListHDFSNotInGCS.class);

  @Override
  protected void stageMain() {
    Path path = new Path(
        "/human/contigs.0612_0949.K41/ResolveThreadsPipeline.0723_0849/part-00596.avro");
    try {
      FileSystem fs = path.getFileSystem(getConf());
      FileChecksum check = fs.getFileChecksum(path);
      if (check == null) {
        sLogger.info("Checksum is null.");
      } else {
        sLogger.info("Checksum:" + check.getBytes().toString());
      }
    } catch(IOException e) {
      sLogger.fatal("IOException.", e);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new ListHDFSNotInGCS(), args);
    System.exit(res);
  }
}
