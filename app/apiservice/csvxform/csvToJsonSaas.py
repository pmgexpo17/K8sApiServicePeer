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
from csvToJsonXform import Compiler
import datetime
import leveldb
import os, sys, time
import requests
import simplejson as json
import uuid

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

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, **kwargs):
    logger.info('csvToJsonSaas.CsvToJsonSaas._start')
    XformEmailPrvdr.init('csvToJson')
    mPrvdr = MetaPrvdr(self._leveldb, self.actorId)
    self.jobId, self.tsXref = mPrvdr()
    #XformEmailPrvdr.start('csvToJson',{})
    self.resolve._start(mPrvdr)
    self.mPrvdr = mPrvdr

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    if self.state.transition == 'NORMALISE_ASYNC':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('NORMALISE_ASYNC is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'NORMALISE_ASYNC failed, returned error signal : %d' % signal
        logger.error(errmsg)
        raise Exception(errmsg)
    elif self.state.transition == 'FINAL_HANDSHAKE':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('FINAL_HANDSHAKE is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'FINAL_HANDSHAKE failed, returned error signal : %d' % signal
        logger.error(errmsg)
        raise Exception(errmsg)
    if self.state.hasNext:
      self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):
    if self.state.transition in ['NORMALISE_ASYNC','FINAL_HANDSHAKE']:
      try:
        self.putApiRequest(201)
      except Exception as ex:
        logger.error('### putApiRequest failed : ' + str(ex))
        raise

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
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
  # ---------------------------------------------------------------#
  def onError(self, ex):
    # if WcResolvar has caught an exception an error mail is ready to be sent
    if not XformEmailPrvdr.hasMailReady('csvToJson'):
      method = 'CsvToJsonSaas.Resolvar._start.' + self.state.current
      errdesc = 'system error'
      logger.error(ex)
      self.sendMail('ERR1',method,errdesc,str(ex))
    else:
      self.sendMail('csvToJson')
    try:
      self.putApiRequest(500)
    except Exception as ex:
      logger.error('### putApiRequest failed : ' + str(ex))

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self,*args):      
    XformEmailPrvdr.sendMail('csvToJson',*args)

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
    self.state.current = 'EVAL_XFORM_META'
    self.jobId, self.tsXref = mPrvdr.getJobVars()
    self.mPrvdr = mPrvdr
    msg = 'csvToJsonSaas.Resolvar, starting job %s ...'
    logger.info(msg % self.jobId)

	# -------------------------------------------------------------- #
	# EVAL_CVS_META
	# ---------------------------------------------------------------#
  def EVAL_XFORM_META(self):
    self.evalXformMeta()
    self.state.next = 'NORMALISE_CSV'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # NORMALISE_CSV
  # - state.next = 'NORMAL_TO_JSON'
  # ---------------------------------------------------------------#
  def NORMALISE_CSV(self):
    self.evalSysStatus()
    self.putXformMeta()
    self.state.transition = 'NORMALISE_ASYNC'
    self.state.inTransition = True
    self.state.next = 'COMPILE_JSON'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # COMPILE_JSON
  # - state.next = complete
  # ---------------------------------------------------------------#
  def COMPILE_JSON(self):
    Compiler.start(self._leveldb, self.tsXref)
    self.compileJson()
    self.state.next = 'WRITE_JSON_FILE'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # WRITE_JSON_FILE
  # - state.next = 'REMOVE_WORKSPACE'
  # ---------------------------------------------------------------#
  def WRITE_JSON_FILE(self):
    self.writeJsonFile()
    self.compressFile()
    self.state.transition = 'FINAL_HANDSHAKE'
    self.state.inTransition = True
    self.state.next = 'REMOVE_WORKSPACE'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # REMOVE_WORKSPACE
  # ---------------------------------------------------------------#
  def REMOVE_WORKSPACE(self):
    self.removeWorkSpace()
    self.purgeXformData()
    self.state.hasNext = False
    self.state.complete = True
    return self.state

  # -------------------------------------------------------------- #
  # evalXformMeta -
  # ---------------------------------------------------------------#
  def evalXformMeta(self):
    self.method = 'evalXformMeta'
    kwArgs = {'itemKey':'csvToJson'}
    repoMeta = self.mPrvdr.getSaasMeta('SaasXformMngr','xformDomain',kwArgs=kwArgs)
    metaFile = repoMeta['repoName'] + '/' + repoMeta['xformMeta']
    logger.info('csvToJson, xform meta file : ' + metaFile)
    if not os.path.exists(metaFile):
      errmsg = 'xform meta file does not exist : %s' % metaFile
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    xformMeta = XformMetaPrvdr()
    try:
      xformMeta.load(metaFile)
      xformMeta.validate('csvToJson')
      self.rootName = xformMeta.getRootName()            
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
      errmsg = 'csvToJson, xform input path does not exist : ' + repoMeta['sysPath']
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    catPath = self.mPrvdr['category']
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'Consumer category %s does not exist in source repo : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      self.newMail('ERR1','system error',errmsg)
      raise Exception
  
    repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('csvToJson, input zipfile repo path : ' + repoPath)

    inputZipFile = self.jobId + '.tar.gz'
    logger.info('csvToJson, input zipfile : ' + inputZipFile)

    zipFilePath = '%s/%s' % (repoPath, inputZipFile)
    if not os.path.exists(zipFilePath):
      errmsg = 'csvToJson, xform input zipfile does not exist in source repo'
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    if not os.path.exists(self.mPrvdr['workSpace']):
      errmsg = 'xform workspace path does not exist : ' + self.mPrvdr['workSpace']
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    workSpace = self.mPrvdr['workSpace'] + '/' + self.tsXref
    logger.info('csvToJson, session workspace : ' + workSpace)
    logger.info('csvToJson, creating session workspace ... ')
    cmdArgs = ['mkdir','-p',workSpace]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to create workspace : ' + workSpace
      self.newMail('ERR1','system error',errmsg)
      raise Exception

    cmdArgs = ['cp',zipFilePath,workSpace]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to copy input zipfile to workspace'
      self.newMail('ERR1','system error',errmsg)
      raise Exception

    cmdArgs = ['tar','-xzf',inputZipFile]
    try:
      self.sysCmd(cmdArgs,cwd=workSpace)
    except Exception as ex:
      errmsg = 'failed to unzip %s in workspace' % inputZipFile
      self.newMail('ERR1','system error',errmsg)
      raise Exception

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
      raise

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self):
    self.method = 'csvToJsonSaas.Resolvar.getJsDomAsQueue'
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
    except Exception as ex:
      errmsg = 'Json compiler failed'
      self.newMail('ERR1',errmsg,str(ex))
      raise

  #------------------------------------------------------------------#
	# getRootSet
	#------------------------------------------------------------------#
  def getRootSet(self):
    self.method = 'getRootSet'
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

	#------------------------------------------------------------------#
  # compileJsObject
  #
  # Convert the python object to json text and write to the output file
  # Return a policy object, adding all the child components to the
  # policy details object
	#------------------------------------------------------------------#
  def compileJsObject(self, dbKey, rootUkey, jsonDom):
    self.method = 'compileJsObject'
    self.rootMember.compile(dbKey, rootUkey)
    for nextMember in jsonDom:
      nextMember.compile()
    for nextMember in jsonDom:
      if nextMember.isLeafNode:
        self.buildJsObject(nextMember)
    self.rootMember.putJsObject(rootUkey)

	#------------------------------------------------------------------#
	# buildJsObject
  # - child object build depends on parent keys so this means start 
  # - at the leaf node and traverse back up
	#------------------------------------------------------------------#
  def buildJsObject(self, nextMember):
    self.method = 'buildJsObject'
    while not nextMember.isRoot:
      nextMember.build()
      nextMember = nextMember.parent

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
    except Exception as ex:
      errmsg = 'write json file %s failed : %s' % (self.jsonFile, str(ex))
      self.newMail('ERR1','system error',errmsg)
      raise Exception

  # -------------------------------------------------------------- #
  # getRootRangeIter
  # ---------------------------------------------------------------#
  def getRootRangeIter(self):
    self.method = 'getRootRangeIter'
    keyLow = '%s|%05d' % (self.tsXref,1)
    keyHigh = '%s|%05d' % (self.tsXref,self.rowCount)
    rowIter = self._leveldb.RangeIter(key_from=keyLow,key_to=keyHigh)
    while True:
      try:
        key, value = rowIter.next()
        yield value
      except StopIteration:
        break

  # -------------------------------------------------------------- #
  # compressFile
  # ---------------------------------------------------------------#
  def compressFile(self):
    self.method = 'compressFile'
    logger.info('csvToJson, gzip json file ...')
    cmdArgs = ['gzip',self.jsonFile]
    try:
      self.sysCmd(cmdArgs,cwd=self.workSpace)
    except Exception as ex:
      errmsg = 'failed to gzip %s in workspace' % self.jsonFile
      self.newMail('ERR1',errmsg,str(ex))
      raise Exception

  # -------------------------------------------------------------- #
  # makeZipFile
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    self.method = 'removeWorkSpace'
    logger.info('csvToJson, removing workspace ...')
    try:
      cmdArgs = ['rm','-rf',self.workSpace]
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to remove workspace : ' + self.workSpace
      self.newMail('ERR1','system error',errmsg)
      raise Exception
    else:
      msg = 'csvToJsonSaas.Resolvar, job %s is now complete'
      logger.info(msg % self.jobId)

  # -------------------------------------------------------------- #
  # purgeXformData
  # ---------------------------------------------------------------#
  def purgeXformData(self):
    self.method = 'purgeXformData'
    logger.info('jsonToCsv, purging xform data ...')
    try:
      self._purgeXformData()
    except Exception as ex:
      errmsg = 'purge xform data failed'
      self.newMail('ERR1',errmsg,str(ex))
      raise Exception(errmsg)

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
    XformEmailPrvdr.newMail('csvToJson',bodyKey,method,*args)

# -------------------------------------------------------------- #
# NormaliseLstnr
# ---------------------------------------------------------------#
class NormaliseLstnr(AppListener):

  def __init__(self, leveldb, caller, callerHost):
    super(NormaliseLstnr, self).__init__(leveldb, caller)
    self.callerHost = callerHost    
    self.state = None
    self.actors = []

  def __call__(self, event):
    if not event.exception and self.state.transition == 'NORMALISE_ASYNC':
      if event.job_id in self.actors:
        with self.state.lock:
          self.actors.remove(event.job_id)
          if not self.actors:
            logger.info('csvToJsonSaas.NormaliseLstnr : send resume signal to the caller')
            self.putApiRequest(201)
    elif event.exception:
      self.putApiRequest(500)

  # -------------------------------------------------------------- #
  # register - add a list of live job ids
  # ---------------------------------------------------------------#
  def register(self, jobRange=None):
    logger.info('csvToJsonSaas.NormaliseLstnr register : ' + str(jobRange))
    self.actors = actors = []
    for _ in jobRange:
      actors += [str(uuid.uuid4())]
    return actors

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'csvxform.csvToJsonSaas:CsvToJsonSaas'
    pdata = (self.caller,classRef,json.dumps({'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s}' % pdata
    data = [('job',params)]
    apiUrl = 'http://%s/api/v1/smart' % self.callerHost
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

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
    try:
      if key == 'jobId':
        return self.jobId
      elif 'client:' in key:
        key = key.split(':')[1]
        return self.jobMeta['client'][key]
      return self.jobMeta[key]
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise    

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
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise
    return json.loads(response.text)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    self.method = 'getProgramMeta'
  
    dbKey = 'PMETA|' + self.actorId
    try:
      return self._leveldb.Get(dbKey)
    except KeyError:
      errmsg = 'EEOWW! pmeta db resource not found : ' + dbKey
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJson',bodyKey,method,*args)

