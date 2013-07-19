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

import auth_util
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

gflags.DEFINE_string("doc", None, "The path to a file containing the commands")
gflags.DEFINE_string(
  "credentials", _default_base_path + '.credentials', 
  'The path to a file storing the credentials. Defaults to a name ' 
  'based on the script.')
gflags.DEFINE_string(
  "client_secret", 
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


def ProcessLine(line, services):
  """Process the command given by line."""
  print "Execute: " + l
  
  proc = subprocess.Popen(
    l.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

  # Buffer for stdout and stderr
  stdout_buffer = ""
  stderr_buffer = ""
  
  while proc.poll() is None:
    # TODO(jeremy) We might want to check whether the process has finished
    # quite often but use a much smaller rate for updating the logs.
    time.sleep(1)

    #TODO(Make drive requests to update the logs.)
    
    
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

  auth_helper = auth_util.OAuthHelper(
    credentials_file=FLAGS.credentials_file, secrets_file=FLAGS.secrets_file,
    scopes=OAUTH_SCOPE)
  
  http = auth_helper.CreateHttpClient()

  global services_
  services_ = Services()
  services_.drive = build('drive', 'v2', http=http)
  
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
  
  # TODO(jlewi): How can the user specify environment variables
  # to use in the names? Maybe we should just go through and gather
  # all the lines beginning with export and add them to a temporary
  # file. 
  for l in lines:
    l = l.strip()
    if l.startswith("#"):
      continue
  
    ProcessLine(l, services_)
  #return 
  #file = drive_service.files().insert(body=body, media_body=media_body, convert=FLAGS.convert).execute()
  #pprint.pprint(file)


if __name__ == "__main__":
  main(sys.argv)