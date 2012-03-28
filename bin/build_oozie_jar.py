#!/usr/bin/python
"""
Setup an HDFS directory for oozie.

We do the following to setup the oozie application directory.
1. Delete the directory if it already exists.
2. Create the directory by unpacking the contrail job jar so that
its contents are
app_dir/contrail/...
app_dir/contrail/...
3. We copy the workflow XML file to app_dir.
4. We generate job configuration files for the pipeline stages and
then post process them to be suitable for use with oozie.
5. We copy the job configuration files to app_dir
6. We copy the properties XML file to the app_dir.
"""

__author__ = "jeremy@lewi.us (Jeremy Lewi)"

import logging

import gflags
import os
import subprocess
import sys
import tempfile

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
  "path", None, "The workflow application path.")
gflags.MarkFlagAsRequired("path")


def main(argv):
  try:
    argv = FLAGS(argv)  # parse flags
  except gflags.FlagsError, e:
    print "%s\nUsage: %s ARGS\n%s" % (e, sys.argv[0], FLAGS)
    sys.exit(1)

  # Check if the application path exists.
  code = subprocess.call(["hadoop", "fs", "-test", "-e", FLAGS.path])
  if code == 0:
    answer = raw_input(
      "Directory {0} exists. Do you want to delete it (y/n)?".format(
        FLAGS.path))
    answer = answer.lower()
    if answer != "y":
      logging.error("Directory {0} exists. Can't continue.".format(FLAGS.path))
      return -1

    code = subprocess.call(["hadoop", "fs", "-rmr", FLAGS.path])
    if code:
      logging.error("Could not delete directory :{0}".format(FLAGS.path))
      return -1
  # Get the contrail jar.
  repo_dir = os.path.dirname(os.path.dirname(__file__))
  jarpath = os.path.join(repo_dir, "target", "contrail-1.0-SNAPSHOT-job.jar")
  if not os.path.exists(jarpath):
    logging.error("File doesn't exist: {0}.\nPossible causes are:"
                  "\n1. You haven't built the code."
                  "\n2. You aren't running this script from the top of the "
                  "repo.".format(jarpath))
    return -1

  # Create a temporary directory to extract the files to.
  tempdir = tempfile.mkdtemp()
  start_dir = os.getcwd()
  try:
    os.chdir(tempdir)
    subprocess.check_call(["jar", "-xvf", jarpath])
  finally:
    os.chdir(start_dir)
    return
  return
if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main(sys.argv)