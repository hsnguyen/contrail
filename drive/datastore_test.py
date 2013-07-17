#!/usr/bin/python
"""
Execute the shell commands in a Google doc.
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
import httplib2
import pprint

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage

import gflags
import json
import os
import stat
import subprocess
import sys

gflags.DEFINE_string(
  "credentials", None, 
  'The path to a file storing the credentials. Defaults to a name ' 
  'based on the script.')
gflags.DEFINE_string(
  "client_secret", 
  "~/.biocloudops-app.client_secrets", "The path to a file storing the secret.")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

# Copy your credentials from the APIs Console
CLIENT_ID = None
CLIENT_SECRET = None

# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = [
  'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/datastore',
  'https://www.googleapis.com/auth/userinfo.email']

# Redirect URI for installed apps
REDIRECT_URI = None

def NewCredentials():
  """Get a new set of credentials."""
  # Run through the OAuth flow and retrieve credentials
  flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
  authorize_url = flow.step1_get_authorize_url()
  print 'Go to the following link in your browser: ' + authorize_url
  code = raw_input('Enter verification code: ').strip()
  credentials = flow.step2_exchange(code)
  return credentials


def LoadSecrets():
  if not FLAGS.client_secret:
    raise Exception('No client secrets file specified.')
  secret_file = os.path.expanduser(FLAGS.client_secret)
  if not os.path.exists(secret_file):
    raise Exception('File doesnt exist:' + secret_file)
    
  global CLIENT_ID
  global CLIENT_SECRET
  global REDIRECT_URI
  with file(secret_file, 'r') as hf:
    data = hf.readlines()
    secret = json.loads(data[0])['installed']
    CLIENT_ID = secret['client_id']
    CLIENT_SECRET = secret['client_secret']
    REDIRECT_URI = secret['redirect_uris'][0]


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

  LoadSecrets()
  # Credential store.
  # TODO(jeremy@lewi.us): We should make sure the credentials file is only readable by
  # the user.
  credentials_file = FLAGS.credentials
  if not credentials_file:
    credentials_file = '.' + os.path.basename(__file__).split('.')[0] + '.credentials'
    credentials_file = os.path.join(os.path.expanduser("~"), credentials_file)
    
  storage = Storage(credentials_file)

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
  
  store_service = build('datastore', 'v1beta1', http=http)
  datasets = store_service.datasets()
  body = { "keys": [
  {
   "path": [
    {
     "id": "5629499534213120",
     "kind": "kind1"
    },
   ],
  }
 ]
}
  datasetId = 'biocloudops-app'
  lookup = datasets.lookup(datasetId=datasetId, body=body)
  lookup.uri += "&trace=email:jlewi"
  lookup_response = lookup.execute()
  

if __name__ == "__main__":
  httplib2.debuglevel = 3
  main(sys.argv)