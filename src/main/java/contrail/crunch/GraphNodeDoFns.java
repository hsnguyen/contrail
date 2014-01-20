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

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import contrail.graph.GraphNodeData;

/**
 * A collection of DoFns for working with GraphNodeData.
 */
public class GraphNodeDoFns {
  /**
   * Key the bowtie mapping by the contig id.
   */
  public static class KeyByNodeId
      extends DoFn<GraphNodeData, Pair<String, GraphNodeData>> {
    @Override
    public void process(
        GraphNodeData node,
        Emitter<Pair<String, GraphNodeData>> emitter) {
      emitter.emit(new Pair<String, GraphNodeData>(
          node.getNodeId().toString(), node));
    }
  }
}
