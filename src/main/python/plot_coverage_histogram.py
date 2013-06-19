"""
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Make a plot of the number of KMERs vs. the number of KMERs.
This requires matplotlib and numpy
"""

__author__ = "jeremy@lewi.us (Jeremy Lewi)"

import gflags
import logging
from matplotlib import pylab
import numpy as np
import os
import sys

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
    "inputpath", None,
    "The path to the text file containing coverage counts.")
gflags.DEFINE_string(
    "outputpath", None,
    "Where to save an image of the graph")


def main(argv):
  try:
    argv = FLAGS(argv)  # parse flags
  except gflags.FlagsError as e:
    print "%s\\nUsage: %s ARGS\\n%s" % (e, sys.argv[0], FLAGS)
    sys.exit(1)

  if not FLAGS.inputpath:
    logging.fatal("You must specify an inputpath")
    sys.exit(-1)

  if not FLAGS.outputpath:
    logging.fatal("You must specify an outputpath")
    sys.exit(-1)
  
  if not os.path.exists(FLAGS.inputpath):
    logging.fatal("Input file %s doesn't exist" % FLAGS.inputpath)
    sys.exit(-1)
  
  num_nodes = 0
  dt = np.dtype([("coverage", np.int), ("count", np.int)])

  delta_size = 1000
  data = np.empty(delta_size, dtype=dt)
  
  with file(FLAGS.inputpath, "r") as hf:
    for line in hf:
      num_nodes += 1
      if num_nodes > data.size:
        old_data = data
        data = np.empty(delta_size + data.size, dtype=dt)
        data[0:num_nodes - 1] = old_data[0:num_nodes - 1]
        
      pieces = line.split()
      
      index = num_nodes - 1
      data[index]["coverage"] = int(pieces[0])
      data[index]["count"] = int(pieces[1])

  data = data[0:num_nodes]

  # Make a 2-d plot.  
  hf = pylab.figure()
  ha = hf.add_subplot(1, 1, 1)
  ha.plot(data["coverage"], data["count"], '.')
  ha.set_xscale("log")
  ha.set_yscale("log")
  ha.set_xlabel("KMER count")
  ha.set_ylabel("Number of KMERs")
  ha.set_title("The number of distinct KMERs as a function of KMER count.")
  if not os.path.exists(FLAGS.outputpath):
    os.makedirs(FLAGS.outputpath)

  hf.savefig(os.path.join(FLAGS.outputpath, "cov_vs_count_scatter.png"));
  
if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main(sys.argv)
