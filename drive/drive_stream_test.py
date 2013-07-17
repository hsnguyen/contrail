#!/usr/bin/python
"""
This script is meant to test streaming data to a Google doc.
"""
# TODO: Download a google doc from drive
# Parse the output line by line
# Run each line in a shell process saving the stdout and stderr to files
# Upload the logs to a folder named "doc.logs" where "doc" is the name of the 
# file we are running.
#!/usr/bin/python
#
# To use this you need to install the google api python client library
#
# easy_install --upgrade google-api-python-client
# easy_install gflags
# or
#
# pip install --upgrade google-api-python-client
# pip install gflags
#
# For reference: https://developers.google.com/drive/examples/python#updating_files
import httplib2
import pprint

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage

import auth_util
import datetime
import gflags
import os
import stat
import subprocess
import sys
import time

# The basepath for settings files for this script.
# TODO(jlewi): Should we use a single json file? Or maybe a single directory?
_default_base_path =  os.path.join(
  os.path.expanduser("~"), '.' + os.path.basename(__file__).split('.')[0])

gflags.DEFINE_string("doc", "stream-test", "The path to a file to update")
gflags.DEFINE_string(
  "credentials", _default_base_path + '.credentials', 
  'The path to a file storing the credentials. Defaults to a name ' 
  'based on the script.')
gflags.DEFINE_string(
  "secret", 
  _default_base_path + '.secrets', 
  "The path to a file storing the secret.")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = [
  'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/datastore',
  'https://www.googleapis.com/auth/userinfo.email']

class Services(object):
  """Container for all the service references
  """

services_ = None

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

  if not FLAGS.doc:
    raise Exception("You must specify a file to upload using the --path argument.")

  auth_helper = auth_util.OAuthHelper()
  auth_helper.Setup(
    credentials_file=FLAGS.credentials, secrets_file=FLAGS.secret,
    scopes=OAUTH_SCOPE)
  
  http = auth_helper.CreateHttpClient()

  global services_
  services_ = Services()
  services_.drive = build('drive', 'v2', http=http)
  
  local_file = '/tmp/drive_stream_test'
  with file(local_file, 'w') as hf:
    hf.write("File created.\n")
    hf.flush()
    media_body = MediaFileUpload(local_file, mimetype='text/plain', resumable=True)
    mime_type = "text/plain"    
    
    body = {
      'title': 'drive_stream_test',
      'description': 'Test drive_stream_test',
      'mimeType': mime_type,      
    }    
    
    create_result = services_.drive.files().insert(
      body=body, media_body=media_body, convert='true').execute()
    # Every second write a timestamp to the file.
    # I think we have to upload the entire file each time.
    for i in range(100):
      text = datetime.datetime.strftime(
        datetime.datetime.now(), "%Y/%m/%d-%H:%M:%S")
      hf.write(text + "\n")
      hf.flush()
      
      media_body = MediaFileUpload(local_file, mimetype='text/plain', resumable=True)
      update_result = services_.drive.files().update(
        fileId = create_result["id"], body=body, media_body=media_body, 
        convert="true", newRevision="true").execute()
      time.sleep(1)


if __name__ == "__main__":
  main(sys.argv)