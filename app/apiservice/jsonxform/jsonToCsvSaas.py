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
from __future__ import division
from abc import ABCMeta, abstractmethod
from apibase import AppDirector, AppListener, AppResolvar, MetaReader, logger
from jsonToCsvXform import HHProvider
from apitools.hardhash import LeveldbClient
from apitools.jsonxform import XformMetaPrvdr
from apitools.mailer import XformEmailPrvdr
from threading import RLock
import datetime
import leveldb
import math
import os, sys, time
import simplejson as json
import requests
import uuid

# -------------------------------------------------------------- #
# SaasError
# ---------------------------------------------------------------#
class SaasError(Exception):
  pass

# -------------------------------------------------------------- #
# JsonToCsvSaas
# ---------------------------------------------------------------#
class JsonToCsvSaas(AppDirector):

  def __init__(self, leveldb, actorId, caller):
    super(JsonToCsvSaas, self).__init__(leveldb, actorId)
    self.caller = caller
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = Resolvar(leveldb)
    self.resolve.state = self.state
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, **kwargs):
    logger.info('jsonToCsvSaas.JsonToCsvSaas._start')
    self.method = '_start'
    # if emailPrvdr.init fails, AppDirector will catch the exception
    XformEmailPrvdr.init('jsonToCsvSaas')
    try:
      mPrvdr = MetaPrvdr(self._leveldb, self.actorId)
      self.jobId, self.tsXref = mPrvdr()
      self.resolve._start(mPrvdr)
      self.mPrvdr = mPrvdr
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise SaasError(ex)

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    self.method = 'advance'
    if self.state.transition == 'NORMALISE_ASYNC':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('NORMALISE_ASYNC is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'NORMALISE_ASYNC failed, returned error signal : %d' % signal
        self.newMail('ERR1','system error',errmsg)
        raise SaasError(errmsg)
    elif self.state.transition == 'WRITE_CSV_ASYNC':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('WRITE_CSV_ASYNC is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'WRITE_CSV_ASYNC failed, returned error signal : %d' % signal
        self.newMail('ERR1','system error',errmsg)
        raise SaasError(errmsg)
    elif self.state.transition == 'FINAL_HANDSHAKE':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('FINAL_HANDSHAKE is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'FINAL_HANDSHAKE failed, returned error signal : %d' % signal
        self.newMail('ERR1','system error',errmsg)
        raise SaasError(errmsg)
    if self.state.hasNext:
      self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):
    self.method = 'quicken'
    try:    
      if self.state.transition in ['WRITE_CSV_ASYNC','NORMALISE_ASYNC','FINAL_HANDSHAKE']:
        self.putApiRequest(201)
    except Exception as ex:
      self.newMail('ERR1','api request failed',str(ex))
      raise SaasError(ex)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    if self.state.transition == 'FINAL_HANDSHAKE' or signal == 500:
      classRef = 'jsonxform.jsonToCsvClient:JsonToCsv'
      kwargs = {"signal":signal,"callee":self.actorId,"tsXref":self.tsXref}
      pdata = (self.caller,classRef,json.dumps(kwargs))
      params = '{"type":"director","id":"%s","service":"%s","args":[],"kwargs":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://%s/api/v1/smart' % self.mPrvdr['client:hostName']
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.transition == 'NORMALISE_ASYNC':
      classRef = 'jsonxform.jsonToCsvXform:JsonNormaliser'
      pdata = (self.actorId,classRef,json.dumps([self.tsXref]))
      params = '{"type":"delegate","id":"%s","service":"%s","args":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://%s/api/v1/async/%d' \
                        % (self.mPrvdr['hostName'], self.resolve.jobRange)
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.transition == 'WRITE_CSV_ASYNC':
      logger.info('DEBUG 3000')
      classRef = 'jsonxform.jsonToCsvXform:CsvComposer'
      pdata = (self.actorId,classRef,json.dumps([self.tsXref, self.resolve.jobRange]))
      params = '{"type":"delegate","id":"%s","service":"%s","args":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://%s/api/v1/async/%d' \
                        % (self.mPrvdr['hostName'], self.resolve.csvRange)
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    logger.info('jsonToCsvSaas.JsonToCsvSaas.onError')
    # test if the error is already handled
    if isinstance(ex,(SaasError,ResolveError,ListenerError,MetaPrvdrError)):
      self.sendMail()
    else:
      # in this case the source method is not visible, so send the state info
      stateDesc = 'JsonToCsvSaas.state.current : ' + self.state.current
      self.sendMail('ERR1',stateDesc,'system error',str(ex))
    try:
      # notify jsonToCsvClient about saas failure
      self.putApiRequest(500)
    except Exception as ex:
      self.newMail('ERR1','api request failed',str(ex))
      self.sendMail()

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):
    XformEmailPrvdr.sendMail('jsonToCsvSaas',*args)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

# -------------------------------------------------------------- #
# getLineCount
# ---------------------------------------------------------------#
def getLineCount(fname):
  with open(fname) as f:
    for i, l in enumerate(f):
      pass
  return i

# -------------------------------------------------------------- #
# getSplitFileTag
# ---------------------------------------------------------------#
def getSplitFileTag(taskNum):
  tagOrd = int((taskNum-1) / 26)
  tagChr1 = chr(ord('a') + tagOrd)
  tagOrd = int((taskNum-1) % 26)
  tagChr2 = chr(ord('a') + tagOrd)
  return tagChr1 + tagChr2

# -------------------------------------------------------------- #
# ResolveError
# ---------------------------------------------------------------#
class ResolveError(Exception):
  pass

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.method = '__init__'
    self.__dict__['EVAL_XFORM_META'] = self.EVAL_XFORM_META
    self.__dict__['NORMALISE_JSON'] = self.NORMALISE_JSON
    self.__dict__['WRITE_CSV_FILES'] = self.WRITE_CSV_FILES
    self.__dict__['MAKE_ZIP_FILE'] = self.MAKE_ZIP_FILE
    self.__dict__['REMOVE_WORKSPACE'] = self.REMOVE_WORKSPACE

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, mPrvdr):
    self.method = '_start'
    try:
      self.state.current = 'EVAL_XFORM_META'
      self.jobId, self.tsXref = mPrvdr.getJobVars()
      self.mPrvdr = mPrvdr
      msg = 'jsonToCsvSaas.Resolvar, starting job %s ...'
      logger.info(msg % self.jobId)
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

	# -------------------------------------------------------------- #
	# EVAL_CVS_META
	# ---------------------------------------------------------------#
  def EVAL_XFORM_META(self):
    try:
      self.evalXformMeta()
      self.state.next = 'NORMALISE_JSON'
      self.state.hasNext = True
      return self.state
    except ResolveError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # NORMALISE_JSON
  # - state.next = 'WRITE_JS_FILE'
  # ---------------------------------------------------------------#
  def NORMALISE_JSON(self):
    try:
      self.evalSysStatus()
      setSize = self.putXformMeta()
      self.getHardHashService(setSize)
      self.state.transition = 'NORMALISE_ASYNC'
      self.state.inTransition = True
      self.state.next = 'WRITE_CSV_FILES'
      self.state.hasNext = True
      #self.state.complete = True
      return self.state
    except ResolveError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # WRITE_CSV_FILES
  # - state.next = 'MAKE_ZIP_FILE'
  # ---------------------------------------------------------------#
  def WRITE_CSV_FILES(self):
    try:    
      self.getWriteTaskRange()
      self.state.transition = 'WRITE_CSV_ASYNC'
      self.state.inTransition = True
      self.state.next = 'MAKE_ZIP_FILE'
      self.state.hasNext = True
      return self.state
    except ResolveError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # MAKE_ZIP_FILE
  # - state.next = 'REMOVE_WORKSPACE'
  # ---------------------------------------------------------------#
  def MAKE_ZIP_FILE(self):
    try:
      self.makeGZipFile()
      self.state.transition = 'FINAL_HANDSHAKE'
      self.state.inTransition = True
      self.state.next = 'REMOVE_WORKSPACE'
      self.state.hasNext = True
      return self.state
    except ResolveError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # REMOVE_WORKSPACE
  # ---------------------------------------------------------------#
  def REMOVE_WORKSPACE(self):
    try:
      self.removeWorkSpace()
      self.closeHardHashService()
      HHProvider.delete(self.tsXref)
      self.state.hasNext = False
      self.state.complete = True
      return self.state
    except ResolveError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # evalXformMeta -
  # ---------------------------------------------------------------#
  def evalXformMeta(self):
    self.method = 'evalXformMeta'
    kwArgs = {'itemKey':'jsonToCsv'}
    repoMeta = self.mPrvdr.getSaasMeta('SaasXformMngr','xformDomain',kwArgs=kwArgs)
    metaFile = repoMeta['repoName'] + '/' + repoMeta['xformMeta']
    logger.info('xform meta file : ' + metaFile)
    if not os.path.exists(metaFile):
      errmsg = 'xform meta file does not exist : %s' % metaFile
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)

    xformMeta = XformMetaPrvdr()
    try:
      xformMeta.load(metaFile)
      xformMeta.validate('jsonToCsv')
      dbKey = '%s|XFORM|rootname' % self.tsXref
      self._leveldb.Put(dbKey,xformMeta.getRootName())
      self.xformMeta = xformMeta
    except Exception as ex:
      errmsg = '%s is not valid' % repoMeta['xformMeta']
      self.newMail('ERR1',errmsg,str(ex))
      raise

  # -------------------------------------------------------------- #
  # evalSysStatus
  # ---------------------------------------------------------------#
  def evalSysStatus(self):
    self.method = 'evalSysStatus'
    repoMeta = self.mPrvdr.getSaasMeta('SaasRepoMngr','repoDomain')
    if not os.path.exists(repoMeta['sysPath']):
      errmsg = 'xform input path does not exist : ' + repoMeta['sysPath']
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)

    catPath = self.mPrvdr['category']
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'consumer category %s does not exist in : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)
  
    repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('input json file repo path : ' + repoPath)

    inputJsonFile = '%s.%s' % (self.jobId, self.mPrvdr['fileExt'])
    logger.info('input json file : ' + inputJsonFile)

    jsonFilePath = '%s/%s' % (repoPath, inputJsonFile)
    if not os.path.exists(jsonFilePath):
      errmsg = 'xform input zip file does not exist in source repo'
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)

    if not os.path.exists(self.mPrvdr['workSpace']):
      errmsg = 'xform workspace path does not exist : ' + self.mPrvdr['workSpace']
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)

    workSpace = self.mPrvdr['workSpace'] + '/' + self.tsXref
    logger.info('session workspace : ' + workSpace)
    logger.info('creating session workspace ... ')

    cmdArgs = ['mkdir','-p',workSpace]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      self.newMail('ERR1','create workspace failed',str(ex))
      raise ResolveError(errmsg)

    cmdArgs = ['cp',jsonFilePath,workSpace]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = '%s copy to workspace failed' % inputJsonFile
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)
    
    self.jsonFileName = self.jobId

    jsonFilePath = workSpace + '/' + inputJsonFile
    lineCount = getLineCount(jsonFilePath)
    self.jobRange = 2
    splitSize = int(math.ceil(lineCount / self.jobRange))
    # round up to the nearest 50
    #splitSize = int(math.ceil(splitSize / 50.0)) * 50
    cmdArgs = ['split','-l',str(splitSize),inputJsonFile,self.jsonFileName]
    try:
      self.sysCmd(cmdArgs,cwd=workSpace)
    except Exception as ex:
      errmsg = 'command failed : ' + str(cmdArgs)
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)
    for i in range(1, self.jobRange+1):
      self.putSplitFilename(i)

    dbKey = '%s|REPO|workspace' % self.tsXref
    self._leveldb.Put(dbKey, workSpace)
    self.workSpace = workSpace

  # -------------------------------------------------------------- #
  # putSplitFilename
  # ---------------------------------------------------------------#
  def putSplitFilename(self, taskNum):
    fileTag = getSplitFileTag(taskNum)
    splitFileName = self.jobId + fileTag
    dbKey = '%s|TASK|%d|INPUT|jsonFile' % (self.tsXref, taskNum)
    self._leveldb.Put(dbKey, splitFileName)

  # -------------------------------------------------------------- #
  # putXformMeta
  # ---------------------------------------------------------------#
  def putXformMeta(self):
    self.method = 'putXformMeta'
    try:
      # put the json schema metainfo to storage for retrieval by workers
      metaIndex = 0
      for nodeName in self.xformMeta.get():
        metaIndex += 1
        csvMeta = self.xformMeta.get(nodeName)
        dbKey = '%s|XFORM|META|%s' % (self.tsXref, nodeName)
        self._leveldb.Put(dbKey,json.dumps(csvMeta))
        dbKey = '%s|XFORM|CSVNAME|%d' % (self.tsXref, metaIndex)
        self._leveldb.Put(dbKey, csvMeta['tableName'])
        csvPath = '%s/%s.csv' % (self.workSpace, csvMeta['tableName'])
        dbKey = '%s|XFORM|CSVPATH|%d' % (self.tsXref, metaIndex)
        self._leveldb.Put(dbKey, csvPath)
        logger.info('jsonToCsv %s meta index : %d' % (nodeName, metaIndex))
        logger.info('%s meta item : %s ' % (nodeName, str(csvMeta)))
      return metaIndex
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # getHardHashService - get a HardHash service instance
  # ---------------------------------------------------------------#
  def getHardHashService(self, setSize):    
    self.method = 'getHardHashService'
    try:
      params = {"id":None,"setSize":self.jobRange}
      data = [('job',json.dumps(params))]
      apiUrl = 'http://localhost:5500/api/v1/hardhash'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
      rdata = json.loads(response.text) 
      self.datastoreId = rdata['datastoreId'] 
      HHProvider.start(self.tsXref, rdata['routerAddr'])
      logger.info('### HardHash routerAddr : ' + rdata['routerAddr'])
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # getWriteTaskRange
  # ---------------------------------------------------------------#
  def getWriteTaskRange(self):
    self.method = 'getWriteTaskRange'
    try:
      self.csvRange = len(self.xformMeta.get())
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)
    
  # -------------------------------------------------------------- #
  # makeGZipFile
  # ---------------------------------------------------------------#
  def makeGZipFile(self):
    self.method = 'makeGZipFile'
    logger.info('making tar gzipfile %s.tar.gz ...' % self.jobId)
    try:
      gzipFile = '%s.tar.gz' % self.jobId
      cmd = 'tar -czf %s *.csv' % gzipFile
      self.sysCmd(cmd,cwd=self.workSpace,shell=True)
    except Exception as ex:
      errmsg = '%s gzip failed : ' % gzipFile
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

  # -------------------------------------------------------------- #
  # removeWorkSpace
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    self.method = 'removeWorkSpace'
    logger.info('csvToJson, removing %s workspace ...' % self.tsXref)
    try:
      cmdArgs = ['rm','-rf',self.workSpace]
      self.sysCmd(cmdArgs)
      msg = 'jsonToCsvSaas.Resolvar, job %s is now complete'
      logger.info(msg % self.jobId)
    except Exception as ex:
      self.newMail('ERR1','remove workspace failed',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # closeHardHashService
  # ---------------------------------------------------------------#
  def closeHardHashService(self):
    self.method = 'closeHardHashService'
    try:
      params = {"id":self.datastoreId}
      data = [('job',json.dumps(params))]
      apiUrl = 'http://localhost:5500/api/v1/hardhash'
      response = requests.delete(apiUrl,data=data)
      logger.info('api response ' + response.text)
      logger.info('### HardHash %s is now deleted' % self.datastoreId)
    except Exception as ex:
      self.newMail('ERR1','HardHash closure failed',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

# -------------------------------------------------------------- #
# ListenerError
# ---------------------------------------------------------------#
class ListenerError(Exception):
  pass

# -------------------------------------------------------------- #
# NormaliseLstnr
# ---------------------------------------------------------------#
class NormaliseLstnr(AppListener):

  def __init__(self, leveldb, caller, callerHost):
    super(NormaliseLstnr, self).__init__(leveldb, caller)
    self.callerHost = callerHost    
    self.state = None
    self.actors = []
    self.method = '__init__'
    self.lock = RLock()
    self.hasError = False

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self, event):
    with self.lock:
      if event.job_id not in self.actors or self.hasError:
        return
      if event.exception:
        self.hasError = True
        self.putApiRequest(500)
      elif self.state.transition in ['WRITE_CSV_ASYNC','NORMALISE_ASYNC']:
        self.evalEvent(event.job_id)

  # -------------------------------------------------------------- #
  # evalEvent
  # ---------------------------------------------------------------#
  def evalEvent(self, jobId):
    self.method = 'evalEvent'
    try:
      self.actors.remove(jobId)
      if not self.actors:
        logger.info('jsonToCsvSaas.NormaliseLstnr : sending resume signal to caller')
        self.putApiRequest(201)
    except ListenerError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ListenerError(ex)

  # -------------------------------------------------------------- #
  # register - add a list of live job ids
  # ---------------------------------------------------------------#
  def register(self, jobRange=None):
    logger.info('jsonToCsvSaas.NormaliseLstnr register : ' + str(jobRange))
    try:
      self.actors = actors = []
      for _ in jobRange:
        actors += [str(uuid.uuid4())]
      return actors
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ListenerError(ex)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    self.method = 'putApiRequest'
    try:
      self._putApiRequest(signal)
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ListenerError(ex)

  # -------------------------------------------------------------- #
  # _putApiRequest
  # ---------------------------------------------------------------#
  def _putApiRequest(self, signal):
    classRef = 'jsonxform.jsonToCsvSaas:jsonToCsvSaas'
    pdata = (self.caller,classRef,json.dumps({'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s}' % pdata
    data = [('job',params)]
    apiUrl = 'http://%s/api/v1/smart' % self.callerHost
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

# -------------------------------------------------------------- #
# MetaPrvdrError
# ---------------------------------------------------------------#
class MetaPrvdrError(Exception):
  pass

# -------------------------------------------------------------- #
# MetaPrvdr
# ---------------------------------------------------------------#
class MetaPrvdr(MetaReader):

  def __init__(self, leveldb, actorId):
    super(MetaPrvdr, self).__init__()
    self._leveldb = leveldb
    self.actorId = actorId

  # -------------------------------------------------------------- #
  # __getitem__
  # ---------------------------------------------------------------#
  def __getitem__(self, key):
    self.method = '__getitem__'
    try:
      if key == 'jobId':
        return self.jobId
      elif 'client:' in key:
        key = key.split(':')[1]
        return self.jobMeta['client'][key]
      return self.jobMeta[key]
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise MetaPrvdrError(ex)

  # -------------------------------------------------------------- #
  # __setitem__
  # ---------------------------------------------------------------#
  def __setitem__(self, key, value):
    pass

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):
    self.method = '__call__'
    try:
      pMetadoc = self.getProgramMeta()
      self.jobMeta = pMeta = json.loads(pMetadoc)
      logger.info('### PMETA : ' + str(pMeta))
      kwArgs = {'itemKey':pMeta['jobId']}
      _jobMeta = self.getSaasMeta('SaasEventMngr','eventDomain',queryArgs=['JOB'],kwArgs=kwArgs)
      logger.info('### JOB_META : ' + str(_jobMeta))    
      className = _jobMeta['service']
      self.jobMeta = _jobMeta[className]
      self.jobId = pMeta['jobId']
      logger.info('### SAAS JOB_META : ' + str(self.jobMeta))    
      className = _jobMeta['client'] 
      self.jobMeta['client'] = _jobMeta[className]
      logger.info('### CLIENT JOB_META : ' + str(self.jobMeta['client']))        
      self.tsXref = datetime.datetime.now().strftime('%y%m%d%H%M%S')
      logger.info('### jobId, tsXref : %s, %s' % (self.jobId, self.tsXref))
      return (self.jobId, self.tsXref)
    except MetaPrvdrError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise MetaPrvdrError(ex)

  # -------------------------------------------------------------- #
  # getJobVars
  # -------------------------------------------------------------- #
  def getJobVars(self):
    return (self.jobId, self.tsXref)

  # -------------------------------------------------------------- #
  # getSaasMeta
  # - generic method to lookup and return xform meta
  # ---------------------------------------------------------------#
  def getSaasMeta(self, className, domainKey, hostName=None, queryArgs=[], kwArgs={}):
    self.method = 'getSaasMeta'
    try:
      classRef = 'saaspolicy.saasContract:' + className
      args = (classRef,json.dumps(queryArgs),json.dumps(kwArgs))
      params = '{"service":"%s","args":%s,"kwargs":%s}' % args
      data = [('job',params)]
      if not hostName:
        hostName = self.jobMeta['hostName']
      apiUrl = 'http://%s/api/v1/saas/%s' % (hostName, self.jobMeta[domainKey])
      response = requests.get(apiUrl,data=data)
      return json.loads(response.text)      
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise MetaPrvdrError(ex)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    self.method = 'getProgramMeta'
    try:
      dbKey = 'PMETA|' + self.actorId
      return self._leveldb.Get(dbKey)
    except KeyError:
      errmsg = 'EEOWW! pmeta db resource not found : ' + dbKey
      self.newMail('ERR1','system error',errmsg)
      raise MetaPrvdrError(errmsg)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

