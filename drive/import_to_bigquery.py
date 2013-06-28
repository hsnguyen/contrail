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
# For cloud storage you need to enable API access for cloud storage json API
# and not just cloud storage.
import httplib2
import pprint

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage

import gflags
import json
import os
import fnmatch
import stat
import sys

from urlparse import urlparse

gflags.DEFINE_string("project", None, "The API project.")
gflags.DEFINE_string("path", None, "A globular expresssion matching the google cloud store files to import into big query.")
gflags.DEFINE_string("schema", None, "The scheme for the data. This should be a json string.")
gflags.DEFINE_string("dataset", None, "The dataset.")
gflags.DEFINE_string("table", None, "The table.")


gflags.MarkFlagAsRequired("path")
gflags.MarkFlagAsRequired("project")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

# Copy your credentials from the APIs Console
CLIENT_ID = '978102606005.apps.googleusercontent.com'
CLIENT_SECRET = 'WA9ScXrkIQw6ArmiwpImnZ4q'

# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = ['https://www.googleapis.com/auth/drive',
               'https://www.googleapis.com/auth/bigquery',
               'https://www.googleapis.com/auth/devstorage.read_write']              

# Redirect URI for installed apps
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'


def NewCredentials():
  """Get a new set of credentials."""
  # Run through the OAuth flow and retrieve credentials
  flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
  authorize_url = flow.step1_get_authorize_url()
  print 'Go to the following link in your browser: ' + authorize_url
  code = raw_input('Enter verification code: ').strip()
  credentials = flow.step2_exchange(code)
  return credentials


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

  #if not FLAGS.path:
  #  raise Exception("You must specify a file to upload using the --path argument.")

  # Credential store.
  # TODO(jeremy@lewi.us): We should make sure the credentials file is only readable by
  # the user.
  credentials_file = os.path.join(os.path.expanduser("~"), ".import_to_bigquery")
  storage = Storage(credentials_file)

  # TODO(jeremy@lewi.us): How can we detect when the credentials have expired and
  # refresh.
  if os.path.exists(credentials_file):
    # Load the credentials from the file
    credentials = storage.get()
  else:
    # get new credentials
    credentials = NewCredentials()
    storage.put(credentials)

  # Make sure the credentials file is only accessible by the user.
  os.chmod(credentials_file, stat.S_IWUSR | stat.S_IRUSR)

  # Create an httplib2.Http object and authorize it with our credentials
  http = httplib2.Http()
  http = credentials.authorize(http)

  drive_service = build('drive', 'v2', http=http)
  bq_service = build('bigquery', 'v2', http=http)
  gs_service = build('storage', 'v1beta2', http=http)
    
  gs_url = urlparse(FLAGS.path)
  
  if gs_url.scheme.lower() != 'gs':
    raise Exception('path must be a google storage uri with scheme gs')
  
  gs_bucket = gs_url.hostname
  gs_path = gs_url.path  
  # Get a list of gs objects matching the path.  
  gs_prefix = os.path.dirname(gs_path.lstrip('/')) + '/'
  gs_objects = gs_service.objects().list(
    bucket=gs_bucket, delimiter='/', prefix=gs_prefix).execute()
  
  if not 'items' in gs_objects:
    print 'No items matched prefix={0}'.format(gs_prefix)
    return
    
  print '{0} items matched the prefix {1}'.format(len(gs_objects['items']), gs_prefix)

  matches = [] 
  glob = os.path.basename(gs_path)
  names = [i['name'] for i in gs_objects['items']]
  for name in names:
    if fnmatch.fnmatch(os.path.basename(name), glob):
      matches.append(name)
  
  fields = json.loads(FLAGS.schema)
  body = {
    'configuration' : {
      'load' : {
        'sourceUris' : [gs_url.scheme + '://' + gs_bucket + '/' + n for n in names],
        'schema' : {
          'fields' : fields,
         },
        'destinationTable' : {
          'projectId' : FLAGS.project,
          'datasetId': FLAGS.dataset,
          'tableId': FLAGS.table,
         },
         # If the table already exists only write the data if the table is empty.
         'writeDisposition' : 'WRITE_EMPTY',
         'sourceFormat' : 'NEWLINE_DELIMITED_JSON',
      }
    }
  }
  
  bq_jobs = bq_service.jobs();
  bq_insert = bq_jobs.insert(projectId=FLAGS.project, body=body).execute()
  
  pprint.pprint(bq_insert)
  print "Done"
  
  
if __name__ == "__main__":
  main(sys.argv)
