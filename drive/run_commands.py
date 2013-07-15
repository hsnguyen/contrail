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
import os
import stat
import subprocess
import sys

gflags.DEFINE_string("doc", None, "The path to a file containing the commands")
#gflags.DEFINE_string("folder", None, "(Optional) the folder in google drive to insert this into.")
#gflags.DEFINE_string("name", None, "The name to assign the document. Defaults to the filename.")
#gflags.DEFINE_string("description", "", "Description for the file.")
#gflags.DEFINE_bool("convert", True, "Whether to convert the file.")

#gflags.MarkFlagAsRequired("path")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

# Copy your credentials from the APIs Console
CLIENT_ID = '978102606005.apps.googleusercontent.com'
CLIENT_SECRET = 'WA9ScXrkIQw6ArmiwpImnZ4q'

# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/datastore']

# Redirect URI for installed apps
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

class Services(object):
  """Container for all the service references
  """

services_ = None

def NewCredentials():
  """Get a new set of credentials."""
  # Run through the OAuth flow and retrieve credentials
  flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
  authorize_url = flow.step1_get_authorize_url()
  print 'Go to the following link in your browser: ' + authorize_url
  code = raw_input('Enter verification code: ').strip()
  credentials = flow.step2_exchange(code)
  return credentials


def FindFile(target):
  """Find the file or folder.
  
  Args: 
    The path of the file or folder we want to find.
    
  Returns:
    file_info:   
  """
  global services_
  folders = target.split('/')
  parent = None
  file_info = None
  for i, folder in enumerate(folders):
    #if i == len(folders) - 1:
    #  mimeType = 'application/vnd.google-apps.file'
    #else:
    #  mimeType = 'application/vnd.google-apps.folder'
    query = 'title = "{0}"'.format(folder)
    if parent:
      query += ' and "{0}" in parents'.format(parent)
    files = services_.drive.files().list(q=query).execute()
    if not 'items' in files:
      raise Exception('Could not locate folder: ' + folder)
    if not files['items']:
      raise Exception('Could not locate folder: ' + folder)
    if len(files['items']) > 1:
      raise Exception('More than one folder named: ' + folder + ' need to handle this')
    parent = files['items'][0]['id']
    
    if i == len(folders) - 1:
      file_info = files['items'][0]  
  
  return file_info


def CreateFolder(name, parent_id=None, description=''):
  """Create a folder.
  """
  global services_
  body = {
    'title': name,
    'description': description,
    'mimeType': 'application/vnd.google-apps.folder',      
  }

  if parent_id:
    body['parents'] = [{'id' : parent_id}]
          
  result = services_.drive.files().insert(body=body).execute()  

  return result


def CreateFolderRecursively(name):
  folders = name.split("/")
  parent = None
  
  # Reverse folders so it is a stack
  folders = folders[::-1]
  
  # Find folders which exist
  while folders:
    folder = folders.pop()
    #if i == len(folders) - 1:
    #  mimeType = 'application/vnd.google-apps.file'
    #else:
    #  mimeType = 'application/vnd.google-apps.folder'
    query = 'title = "{0}"'.format(folder)
    if parent:
      query += ' and "{0}" in parents'.format(parent['id'])
    files = services_.drive.files().list(q=query).execute()
    if not 'items' in files:
      raise Exception('Could not locate folder: ' + folder)
    if not files['items']:
      # This item doesn't exit.
      folders.append(folder)
      break
    if len(files['items']) > 1:
      raise Exception('More than one folder named: ' + folder + ' need to handle this')
    parent = files['items'][0]
    
  if not folders:
    # Folder exists
    return parent
  
  # Create any needed directories.
  while folders:
    folder = folders.pop()
    parent = CreateFolder(folder, parent_id=parent['id'])
  
  return parent


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

  # Credential store.
  # TODO(jeremy@lewi.us): We should make sure the credentials file is only readable by
  # the user.
  credentials_file = os.path.join(os.path.expanduser("~"), ".run_commands")
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
  
  global services_
  services_ = Services()
  services_.drive = drive_service
  
  store_service = build('datastore', 'v1beta1', http=http)
  datasets = store_service.datasets()
  body = { "keys": [
  {
   "path": [
    {
     "id": "5629499534213120",
     "kind": "test-0712-1341"
    },
   ],
   #"partitionId": {
   # "namespace": "testing"
   #}
  }
 ]
}
  datasetId = 'biocloudops-app'
  lookup = datasets.lookup(datasetId=datasetId, body=body)
  lookup.uri += "&trace=email:jlewi"
  lookup_response = lookup.execute()
  #datasets.
  files = []

  # Identify the file.
  if FLAGS.doc:    
    file_info = FindFile(FLAGS.doc)
            
  # Create a folder for the logs
  parent_id = file_info['parents'][0]['id']
  
  logs_folder_path = FLAGS.doc + '.logs'
  
  logs_folder = CreateFolderRecursively(logs_folder_path)
  
  if not file_info:
    raise Exception('Could not locate the file:' + FLAGS.doc)

  if not 'exportLinks' in file_info:
    raise Exception('File data is missing exportLinks')
  
  if not 'text/plain' in file_info['exportLinks']:
    raise Exception('Missing text/plain export link')
  
  plain_link = file_info['exportLinks']['text/plain']
  
  resp, content = drive_service._http.request(plain_link)
  if resp.status != 200:
    print 'An error occurred: %s' % resp
    return None  
  
  # Strip leading characters.
  content = content.lstrip('\xef\xbb\xbf')
  lines = content.splitlines()
  for l in lines:
    l = l.strip()
    if l.startswith("#"):
      continue
    
    print "Execute: " + l
    subprocess.check_call(l.split(" "))
  #return 
  #file = drive_service.files().insert(body=body, media_body=media_body, convert=FLAGS.convert).execute()
  #pprint.pprint(file)


if __name__ == "__main__":
  main(sys.argv)