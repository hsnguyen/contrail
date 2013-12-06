/* Licensed under the Apache License, Version 2.0 (the "License");
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

import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.util.FileHelper;

public class TestAssemblyReport {
  private StageInfo newStageInfo() {
    StageInfo info = new StageInfo();
    info.setStageClass("SomeStage");
    info.setCounters(new ArrayList<CounterInfo>());
    info.setState(StageState.SUCCESS);
    info.setParameters(new ArrayList<StageParameter>());
    info.setModifiedParameters(new ArrayList<StageParameter>());
    info.setSubStages(new ArrayList<StageInfo>());
    return info;
  }

  @Test
  public void testStageCount() {
    File tempDir = FileHelper.createLocalTempDir();

    StageInfoWriter writer = new StageInfoWriter(
        new Configuration(), tempDir.toString());

    StageInfo parent = newStageInfo();
    writer.write(parent);

    // Add some children;
    StageInfo child = newStageInfo();
    child.setStageClass("Child A");
    parent.getSubStages().add(child);
    parent.getSubStages().add(child);

    child = newStageInfo();
    child.setStageClass("Child B");
    parent.getSubStages().add(child);

    writer.write(parent);
    AssemblyReport reporter = new AssemblyReport();
    reporter.setParameter("inputpath", tempDir.toString());
    reporter.execute();
  }
}
