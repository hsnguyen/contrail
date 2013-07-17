"""
Some utilities for handling the oauth dance in my scripts.
"""

import httplib2

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage

import gflags
import json
import os
import stat
#import subprocess
import sys

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()



class OAuthHelper(object):
  CLIENT_ID = None  
  CLIENT_SECRET = None
  
  OAUTH_SCOPE = None
  
  # Redirect URI for installed apps.
  REDIRECT_URI = None  
  
  # File where credentials are cached.
  credentials_file = None
  
  _credentials = None
  
  def __init__(self):
    pass

  
  def Setup(self, credentials_file=None, scopes=None, secrets_file=None):
    """Setup the class.
    """
    self._LoadSecretsFile(secrets_file)
    self.OAUTH_SCOPE = scopes
    self._GetCredentials(credentials_file)
    
    
  def _RunOAuthFlow(self):
    """Run the OAuthFlow to Get a new set of credentials."""
    # Run through the OAuth flow and retrieve credentials
    flow = OAuth2WebServerFlow(
      self.CLIENT_ID, self.CLIENT_SECRET, self.OAUTH_SCOPE, self.REDIRECT_URI)
    authorize_url = flow.step1_get_authorize_url()
    print 'Go to the following link in your browser: ' + authorize_url
    code = raw_input('Enter verification code: ').strip()
    credentials = flow.step2_exchange(code)
    return credentials


  def _GetCredentials(self, credentials_file):
    """Return a set of credentials.
    """    
    credentials_file = os.path.expanduser(credentials_file)  
    storage = Storage(credentials_file)

    if os.path.exists(credentials_file):
      # Load the credentials from the file
      credentials = storage.get()
    else:
      # Get new credentials
      credentials = self._RunOAuthFlow()
      storage.put(credentials)

    # Make sure the credentials file is only accessible by the user.
    os.chmod(credentials_file, stat.S_IWUSR | stat.S_IRUSR)
    
    self._credentials = credentials
    return self._credentials
  

  def CreateHttpClient(self):
    """Create an authorized http client.
    """
    if self._credentials is None:
      raise Exception(
        "You must call GetCredentials first to create crednetials.")
    
    # Create an httplib2.Http object and authorize it with our credentials
    http = httplib2.Http()
    http = self._credentials.authorize(http)
  
    return http
    
  
  def _LoadSecretsFile(self, secrets_file):
    """Load the client secrets from a json file.
    """
    # TODO(jeremy@lewi.us): Should we check that the scope of the secrets
    # files is a superset of the scopes in our credentials?
    if not secrets_file:
      raise Exception('No client secrets file specified.')
    
    if not os.path.exists(secrets_file):
      raise Exception('File doesnt exist:' + secrets_file)

    with file(secrets_file, 'r') as hf:
      data = hf.readlines()
      secret = json.loads(data[0])['installed']
      self.CLIENT_ID = secret['client_id']
      self.CLIENT_SECRET = secret['client_secret']
      self.REDIRECT_URI = secret['redirect_uris'][0]
