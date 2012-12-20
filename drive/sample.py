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

import gflags
import os
import sys

gflags.DEFINE_string("path", None, "The path to the file to upload")
gflags.DEFINE_string("name", None, "The name for the file, defaults to the filename")
gflags.DEFINE_string("description", "", "Description for the file.")

gflags.MarkFlagAsRequired("path")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

# Copy your credentials from the APIs Console
CLIENT_ID = '978102606005.apps.googleusercontent.com'
CLIENT_SECRET = 'WA9ScXrkIQw6ArmiwpImnZ4q'

# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'

# Redirect URI for installed apps
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'


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
  
  if not FLAGS.path:
    raise Exception("You must specify a file to upload using the --path argument.")
  
  name = FLAGS.name
  if not name:
    name = os.path.basename(FLAGS.path)
    
  # Run through the OAuth flow and retrieve credentials
  flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
  authorize_url = flow.step1_get_authorize_url()
  print 'Go to the following link in your browser: ' + authorize_url
  code = raw_input('Enter verification code: ').strip()
  credentials = flow.step2_exchange(code)
  
  # Create an httplib2.Http object and authorize it with our credentials
  http = httplib2.Http()
  http = credentials.authorize(http)
  
  drive_service = build('drive', 'v2', http=http)
    
  # Insert a file
  media_body = MediaFileUpload(FLAGS.path, mimetype='text/plain', resumable=True)
  body = {
    'title': name,
    'description': FLAGS.description,
    'mimeType': 'text/plain'
  }

  file = drive_service.files().insert(body=body, media_body=media_body).execute()
  pprint.pprint(file)

if __name__ == "__main__":  
  main(sys.argv)