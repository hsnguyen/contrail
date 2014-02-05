/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.sequences;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Some utility routines for working with reads in the SRR database.
 * The expectation is that ids come in the form:
 * SRR<library num>.<read_id>/<mate_num>
 */
public class SRRReadIdUtil extends ReadIdUtil{
  // We use a capturing group to match the library name.
  public final static Pattern LIBRARAY_PATTERN =
      Pattern.compile("(SRR[0-9]*)\\..*");

  public static String getLibraryId(String readId) {
    Matcher prefix = LIBRARAY_PATTERN.matcher(readId);
    if(!prefix.find()) {
      throw new IllegalArgumentException(
          "ReadId doesn't match the SRR pattern.");
    }

    String libraryId = prefix.group(1);
    if (libraryId == null) {
      throw new RuntimeException("Could not parse the library id.");
    }
    return libraryId;
  }
}
