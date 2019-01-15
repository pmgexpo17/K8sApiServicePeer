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
from abc import ABCMeta, abstractmethod
from apibase import AppDirector, AppListener, AppResolvar, MetaReader, logger
from apitools.csvxform import XformMetaPrvdr
from apitools.mailer import XformEmailPrvdr
from csvToJsonXform import Compiler, CompileError
from threading import RLock
import datetime
import leveldb
import os, sys, time
import requests
import simplejson as json
import uuid

# -------------------------------------------------------------- #
# SaasError
# ---------------------------------------------------------------#
class SaasError(Exception):
  pass

# -------------------------------------------------------------- #
# CsvToJsonSaaS
# ---------------------------------------------------------------#
class CsvToJsonSaas(AppDirector):

  def __init__(self, leveldb, actorId, caller):
    super(CsvToJsonSaas, self).__init__(leveldb, actorId)
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
    logger.info('csvToJsonSaas.CsvToJsonSaas._start')
    self.method = '_start'
    # if emailPrvdr.init fails, AppDirector will catch the exception
    XformEmailPrvdr.init('csvToJsonSaas')
    try:
      mPrvdr = MetaPrvdr(self._leveldb, self.actorId)
      self.jobId, self.tsXref = mPrvdr()
      #XformEmailPrvdr.start('csvToJsonSaas',{})
      self.resolve._start(mPrvdr)
      self.mPrvdr = mPrvdr
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise SaasError

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
    logger.info('quicken state transition : ' + self.state.transition)
    try:    
      if self.state.transition in ['NORMALISE_ASYNC','FINAL_HANDSHAKE']:
        self.putApiRequest(201)
    except Exception as ex:
      self.newMail('ERR1','api request failed',str(ex))
      raise SaasError(ex)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    self.method = 'putApiRequest'
    if self.state.transition == 'FINAL_HANDSHAKE' or signal == 500:
      classRef = 'csvxform.csvToJsonClient:CsvToJson'
      kwargs = {"signal":signal,"callee":self.actorId,"tsXref":self.tsXref}
      pdata = (self.caller,classRef,json.dumps(kwargs))
      params = '{"type":"director","id":"%s","service":"%s","args":[],"kwargs":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://%s/api/v1/smart' % self.mPrvdr['client:hostName']
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.transition == 'NORMALISE_ASYNC':    
      classRef = 'csvxform.csvToJsonXform:NormaliserFactory'
      pdata = (self.actorId,classRef,json.dumps([self.tsXref]))
      params = '{"type":"delegate","id":"%s","service":"%s","args":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://%s/api/v1/async/%d' \
                        % (self.mPrvdr['hostName'], self.resolve.jobRange)
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onError
  # - since muliple concurrent jobs may fail and trigger onError
  # - limit evalError to run once
  # ---------------------------------------------------------------#
  def onError(self, ex):
    # test if the error is already handled
    if isinstance(ex,(SaasError,ResolveError,ListenerError,MetaPrvdrError)):
      self.sendMail()
    else:
      # in this case the source method is not visible, so send the state info
      stateDesc = 'CsvToJsonSaas.state.current : ' + self.state.current
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
    XformEmailPrvdr.sendMail('csvToJsonSaas',*args)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)

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
    self.__dict__['NORMALISE_CSV'] = self.NORMALISE_CSV
    self.__dict__['COMPILE_JSON'] = self.COMPILE_JSON
    self.__dict__['WRITE_JSON_FILE'] = self.WRITE_JSON_FILE
    self.__dict__['REMOVE_WORKSPACE'] = self.REMOVE_WORKSPACE

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, mPrvdr):
    try:
      self.state.current = 'EVAL_XFORM_META'
      self.jobId, self.tsXref = mPrvdr.getJobVars()
      self.mPrvdr = mPrvdr
      msg = 'csvToJsonSaas.Resolvar, starting job %s ...'
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
      self.state.next = 'NORMALISE_CSV'
      self.state.hasNext = True
      return self.state
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # NORMALISE_CSV
  # - state.next = 'NORMAL_TO_JSON'
  # ---------------------------------------------------------------#
  def NORMALISE_CSV(self):
    try:
      self.evalSysStatus()
      self.putXformMeta()
      self.state.transition = 'NORMALISE_ASYNC'
      self.state.inTransition = True
      self.state.next = 'COMPILE_JSON'
      self.state.hasNext = True
      return self.state
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # COMPILE_JSON
  # - state.next = complete
  # ---------------------------------------------------------------#
  def COMPILE_JSON(self):
    try:
      Compiler.start(self._leveldb, self.tsXref)
      self.compileJson()
      self.state.next = 'WRITE_JSON_FILE'
      self.state.hasNext = True
      return self.state
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # WRITE_JSON_FILE
  # - state.next = 'REMOVE_WORKSPACE'
  # ---------------------------------------------------------------#
  def WRITE_JSON_FILE(self):
    try:
      self.writeJsonFile()
      self.compressFile()
      self.state.transition = 'FINAL_HANDSHAKE'
      self.state.inTransition = True
      self.state.next = 'REMOVE_WORKSPACE'
      self.state.hasNext = True
      return self.state
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # REMOVE_WORKSPACE
  # ---------------------------------------------------------------#
  def REMOVE_WORKSPACE(self):
    try:    
      self.removeWorkSpace()
      self.purgeXformData()
      self.state.hasNext = False
      self.state.complete = True
      return self.state
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # evalXformMeta -
  # ---------------------------------------------------------------#
  def evalXformMeta(self):
    self.method = 'evalXformMeta'
    kwArgs = {'itemKey':'csvToJson'}
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
      xformMeta.validate('csvToJson')
      self.rootName = xformMeta.getRootName()            
      self.xformMeta = xformMeta
    except Exception as ex:
      errmsg = '%s is not valid' % repoMeta['xformMeta']
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

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
      errmsg = 'consumer category %s does not exist in source repo : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)
  
    repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('input zipfile repo path : ' + repoPath)

    inputZipFile = self.jobId + '.tar.gz'
    logger.info('input zipfile : ' + inputZipFile)

    zipFilePath = '%s/%s' % (repoPath, inputZipFile)
    if not os.path.exists(zipFilePath):
      errmsg = 'xform input zipfile does not exist in source repo'
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
      errmsg = 'failed to create workspace : ' + workSpace
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

    cmdArgs = ['cp',zipFilePath,workSpace]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to copy input zipfile to workspace'
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

    cmdArgs = ['tar','-xzf',inputZipFile]
    try:
      self.sysCmd(cmdArgs,cwd=workSpace)
    except Exception as ex:
      errmsg = 'failed to unzip %s in workspace' % inputZipFile
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

    # put workspace path in storage for subprocess access
    dbKey = '%s|REPO|workspace' % self.tsXref
    self._leveldb.Put(dbKey, workSpace)
    self.workSpace = workSpace

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
        #self.db[dbKey] = csvMeta
        dbKey = '%s|XFORM|META|%d' % (self.tsXref, metaIndex)
        self._leveldb.Put(dbKey,json.dumps(csvMeta))
        dbKey = '%s|XFORM|META|%s' % (self.tsXref, nodeName)
        self._leveldb.Put(dbKey,json.dumps(csvMeta))
        logger.info('csvToJson %s meta index : %d' % (nodeName, metaIndex))
        logger.info('%s meta item : %s ' % (nodeName, str(csvMeta)))
      self.jobRange = metaIndex
    except Exception as ex:
      errmsg = 'failed to store %s meta item' % nodeName
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self):
    self.method = 'getJsDomAsQueue'
    try:
      from collections import deque
      logger.info("### root name  : " + self.rootName)
      self.rootMember = Compiler.getRoot(self.rootName)
      domQueue = deque(self.rootMember.getMembers())
      while domQueue:
        nextMember = domQueue.popleft()
        memberList = nextMember.getMembers()
        if memberList:
          domQueue.extendleft(memberList)        
        yield nextMember
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  #------------------------------------------------------------------#
	# compileJson
	#------------------------------------------------------------------#
  def compileJson(self):
    self.method = 'compileJson'    
    try:
      jsonDom = list(self.getJsDomAsQueue())
      for objKey, objValue in self.getRootSet():
        self.compileJsObject(objKey, objValue, jsonDom)
      logger.info('### csvToJson compile step is done ...')
    except (ResolveError, CompileError):
      raise
    except Exception as ex:
      self.newMail('ERR1','compile error',str(ex))
      raise ResolveError(ex)

  #------------------------------------------------------------------#
	# getRootSet
	#------------------------------------------------------------------#
  def getRootSet(self):
    self.method = 'getRootSet'
    try:
      dbKey = '%s|%s|rowcount' % (self.tsXref, self.rootName)
      self.rowCount = int(self._leveldb.Get(dbKey))
      logger.info('### %s row count : %d' % (self.rootName, self.rowCount))
      keyLow = '%s|%s|%05d' % (self.tsXref,self.rootName,1)
      keyHigh = '%s|%s|%05d' % (self.tsXref,self.rootName,self.rowCount)
      dbIter = self._leveldb.RangeIter(key_from=keyLow, key_to=keyHigh)
      while True:
        try:
          key, value = dbIter.next()
          yield (key, value)
        except StopIteration:
          break
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

	#------------------------------------------------------------------#
  # compileJsObject
  #
  # Convert the python object to json text and write to the output file
  # Return a policy object, adding all the child components to the
  # policy details object
	#------------------------------------------------------------------#
  def compileJsObject(self, dbKey, rootUkey, jsonDom):
    self.method = 'compileJsObject'
    logger.debug('### root ukey : ' + rootUkey)
    try:
      self.rootMember.compile(dbKey, rootUkey)
      for nextMember in jsonDom:
        nextMember.compile()
      for nextMember in jsonDom:
        if nextMember.isLeafNode:
          self.buildJsObject(nextMember)
      self.rootMember.putJsObject(rootUkey)
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

	#------------------------------------------------------------------#
	# buildJsObject
  # - child object build depends on parent keys so this means start 
  # - at the leaf node and traverse back up
	#------------------------------------------------------------------#
  def buildJsObject(self, nextMember):
    self.method = 'buildJsObject'
    try:
      while not nextMember.isRoot:
        nextMember.build()
        nextMember = nextMember.parent
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # writeJsonFile
  # ---------------------------------------------------------------#
  def writeJsonFile(self):
    self.method = 'writeJsonFile'
    try:
      self.jsonFile = '%s.json' % self.jobId
      jsonPath = '%s/%s' % (self.workSpace, self.jsonFile)
      with open(jsonPath,'w') as jsFh:
        for objValue in self.getRootRangeIter():
          jsFh.write(objValue + '\n')
    except ResolveError:
      raise
    except Exception as ex:
      errmsg = 'write failed : ' + self.jsonFile
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # getRootRangeIter
  # ---------------------------------------------------------------#
  def getRootRangeIter(self):
    self.method = 'getRootRangeIter'
    try:
      keyLow = '%s|%05d' % (self.tsXref,1)
      keyHigh = '%s|%05d' % (self.tsXref,self.rowCount)
      rowIter = self._leveldb.RangeIter(key_from=keyLow,key_to=keyHigh)
      while True:
        try:
          key, value = rowIter.next()
          yield value
        except StopIteration:
          break
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # compressFile
  # ---------------------------------------------------------------#
  def compressFile(self):
    self.method = 'compressFile'
    try:
      logger.info('gzip json file ...')
      cmdArgs = ['gzip',self.jsonFile]
      self.sysCmd(cmdArgs,cwd=self.workSpace)
    except Exception as ex:
      errmsg = '%s gzip failed' % self.jsonFile
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

  # -------------------------------------------------------------- #
  # makeZipFile
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    self.method = 'removeWorkSpace'
    try:
      logger.info('removing %s workspace ...' % self.tsXref)
      cmdArgs = ['rm','-rf',self.workSpace]
      self.sysCmd(cmdArgs)
      msg = 'csvToJsonSaas.Resolvar, job %s is now complete'
      logger.info(msg % self.jobId)
    except Exception as ex:
      self.newMail('ERR1','remove workspace failed',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # purgeXformData
  # ---------------------------------------------------------------#
  def purgeXformData(self):
    self.method = 'purgeXformData'
    try:
      logger.info('jsonToCsv, purging xform data ...')
      self._purgeXformData()
    except Exception as ex:
      errmsg = 'purge xform data failed'
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(errmsg)

  # -------------------------------------------------------------- #
  # _purgeXformData
  # ---------------------------------------------------------------#
  def _purgeXformData(self):
    self.method = '_purgeXformData'
    startKey = '%s|0' % self.tsXref
    endKey = '%s|]' % self.tsXref
    batch = leveldb.WriteBatch()
    keyIter = self._leveldb.RangeIter(startKey, endKey, include_value=False)
    try:
      while True:
        key = keyIter.next()
        batch.Delete(key)
    except StopIteration:
      self._leveldb.Write(batch, sync=True)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)

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

  def __call__(self, event):
    with self.lock:
      if event.job_id not in self.actors or self.hasError:
        return
      if event.exception:
        self.hasError = True
        self.putApiRequest(500)
      elif self.state.transition == 'NORMALISE_ASYNC':
        self.evalEvent(event.job_id)

  # -------------------------------------------------------------- #
  # evalEvent
  # ---------------------------------------------------------------#
  def evalEvent(self, jobId):
    self.method = 'evalEvent'
    try:
      self.actors.remove(jobId)
      if not self.actors:
        logger.info('csvToJsonSaas.NormaliseLstnr : sending resume signal to caller')
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
    logger.info('csvToJsonSaas.NormaliseLstnr register : ' + str(jobRange))
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
      raise

  # -------------------------------------------------------------- #
  # _putApiRequest
  # ---------------------------------------------------------------#
  def _putApiRequest(self, signal):
    classRef = 'csvxform.csvToJsonSaas:CsvToJsonSaas'
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
      #tsXref = '181223155620'
      #dbKey = 'TSXREF|' + self.actorId
      #self._leveldb.Put(dbKey, tsXref)
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
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)

