# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
from __future__ import absolute_import
import os
import logging
import sys
import simplejson as json
import requests
from apibase import ApiPeer
from optparse import OptionParser
from threading import RLock

apiBase = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger('apiagent')
logFormat = '%(levelname)s:%(asctime)s %(message)s'
logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
logfile = '%s/log/apiAgent.log' % apiBase
fileHandler = logging.FileHandler(logfile)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)

# -------------------------------------------------------------- #
# ApiAgent
# ---------------------------------------------------------------#
class ApiAgent(object):
  _lock = RLock()

	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def run(self):
    
    self.start()
    self._isRunning = self.isRunning()
    self._run()

	# -------------------------------------------------------------- #
	# start
	# ---------------------------------------------------------------#
  def start(self):
    
    parser = OptionParser()
    parser.add_option("-r", "--reload-module", dest="moduleName", default=None,
                  help="reload a service module, where service name matches apiservices.json")
    (options, args) = parser.parse_args()      

    if len(args) > 1:
      errmsg = 'usage : apiAgent [serviceName -r moduleName]'
      errmsg += '\t no args = start the http api, loading all services in apiservice.json'
      errmsg += '\t-r = reload module referenced by serviceName'
      logger.error(errmsg)
      raise Exception(errmsg)

    self.starting = len(args) == 0

    try:
      regFile = apiBase + '/apiservices.json'
      with open(regFile,'r') as fhr:
        register = json.load(fhr)
        self.register = register['services']
    except (IOError, ValueError) as ex:
      logger.error('failed loading service register : ' + str(ex))
      raise
    except KeyError:
      errmsg = "services.json root node 'services' does not exist"
      logger.error(errmsg)
      raise Exception(errmsg)

    self.moduleName = None
    if len(args) == 1 and options.moduleName:
      serviceName = args[0]
      try:
        self.service = self.register[serviceName]
        self.moduleName = options.moduleName
        self.serviceName = serviceName
      except KeyError:
        errmsg = 'service name is not registered : ' + serviceName
        logger.error(errmsg)
        raise Exception(errmsg)

	# -------------------------------------------------------------- #
	# isRunning
	# ---------------------------------------------------------------#
  def isRunning(self):

    try:
      response = requests.get('http://localhost:5000/api/v1/ping')
      result = json.loads(response.text)
      if result['status'] == 200:
        logmsg = 'webapi service is running, pid : %d' % result['pid']
        logger.info(logmsg)
      else:
        logmsg = 'webapi service is not available, status : %d' % result['status']
        logger.info(logmsg)
    except requests.exceptions.RequestException as ex:
      if 'Errno 111' in ex.__repr__():
        return False
    except Exception:
      print('### DEBUG 1000 ###')
      return False
    return True        

	# -------------------------------------------------------------- #
	# isLoaded
	# ---------------------------------------------------------------#
  def isLoaded(self):
    
    apiUrl = 'http://localhost:5000/api/v1/service/%s'
    response = requests.get(apiUrl % self.serviceName)
    return json.loads(response.text)['loaded']

	# -------------------------------------------------------------- #
	# loadService
	# ---------------------------------------------------------------#
  def loadService(self):
    
    apiUrl = 'http://localhost:5000/api/v1/service/%s'
    apiUrl = apiUrl % self.serviceName
    data = [('service',json.dumps(self.service))]
    response = requests.put(apiUrl,data=data)
    logger.info('api response ' + response.text)

	# -------------------------------------------------------------- #
	# reloadModule
	# ---------------------------------------------------------------#
  def reloadModule(self):
    
    apiUrl = 'http://localhost:5000/api/v1/service/%s'
    apiUrl = apiUrl % self.serviceName
    data = [('module',self.moduleName)]
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)
      
	# -------------------------------------------------------------- #
	# _run
	# ---------------------------------------------------------------#
  def _run(self):
    
    if self._isRunning:
      if self.starting:
        logger.warn('start action ignored, api is already started ...')
      elif self.moduleName:
        logmsg = '### reloading service module : %s, %s ###'
        logger.info(logmsg % (self.serviceName, self.moduleName))
        self.reloadModule()
    else:
      if self.moduleName:
        logger.warn('reload module action ignored, api is not started')
      elif self.starting:
        try:
          logger.info('### starting apiservicepeer ... ###')
          ApiPeer._make(apiBase)
          for serviceName, serviceRef in self.register.items():
            ApiPeer.appPrvdr.loadService(serviceName, serviceRef)
          apiPeer = ApiPeer._start(5000)
          apiPeer.start()
        except KeyboardInterrupt:
          apiPeer.stop()
      else:
        logger.warn('a valid option was not provided')

if __name__ == '__main__':

  with ApiAgent._lock:
    apiAgent = ApiAgent()
    apiAgent.run()

