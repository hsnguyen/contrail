package contrail.tools;

import java.util.HashMap;
import java.util.Map;

import contrail.stages.ParameterDefinition;

/**
 * Base class for filters.
 */
abstract public class FilterBase {
  /**
   * Return a list of the parameter definitions for this filter.
   */
  public Map<String, ParameterDefinition> getParameterDefinitions() {
    return new HashMap<String, ParameterDefinition>();
  }

  /**
   * Return the class definition for the mapper which this filter uses.
   * @return
   */
  abstract public Class filterClass();
}
