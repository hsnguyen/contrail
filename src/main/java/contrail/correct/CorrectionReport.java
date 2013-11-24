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
// Author: Jeremy Lewi(jeremy@lewi.us)

package contrail.correct;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.stages.StageInfo;
import contrail.stages.StageInfoHelper;

/**
 * Take a stage info file containing information about error correction
 * and print out a report of useful information.
 *
 */
public class CorrectionReport extends NonMRStage {
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String,
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def :
         ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  @Override
  protected void stageMain() {
    // TODO Auto-generated method stub
    StageInfoHelper infoHelper = StageInfoHelper.loadMostRecent(
        getConf(), (String) stage_options.get("inputpath"));

    for (StageInfo stageInfo: infoHelper.DFSSearch()) {


    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CorrectionReport(), args);
    System.exit(res);
  }
}
