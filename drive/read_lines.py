#!/usr/bin/python
"""
A simple python script to read the lines in the specified range.

Line numbers are 1 based.

To use this you need the gflags library.

easy_install gflags
 or 
pip install gflags
"""

import gflags
import re
import sys

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

gflags.DEFINE_integer("start", 1, "The line to start at.")
gflags.DEFINE_integer("end", None, "The line to end at.")

# By default remove the hadoop progress lines.
gflags.DEFINE_string(
  "remove", ".*JobClient.*map.*reduce.*", "A regular expression. Lines which "
  "match will be removed.")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

def main(argv):
  try:
    unparsed = FLAGS(argv)  # parse flags
  except gflags.FlagsError, e:
    usage = """Usage:
{name} {flags}
"""
    print "%s" % e
    print usage.format(name=argv[0], flags=FLAGS)
    sys.exit(1)

  if len(unparsed) != 2:
    raise Exception("You must specify a file to read from.")

    
  lines = []
  with file(unparsed[1], "r") as hf:
    lines = hf.readlines()
    if FLAGS.end:
      lines = lines[FLAGS.start - 1 : FLAGS.end]
    else:
      # Take all lines from start.
      lines = lines[FLAGS.start - 1 :]

  if FLAGS.remove:
    remove_filter = re.compile(FLAGS.remove)
    
    def keep(line):
      return remove_filter.match(line) is None
    
    lines = filter(keep, lines)
    
  print ''.join(lines)
  
  
if __name__ == "__main__":
  main(sys.argv)