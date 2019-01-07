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
from jsonToCsvXform import JsonMember
from apitools.jsonxform import XformMetaPrvdr
from apitools.mailer import XformEmailPrvdr
import datetime
import leveldb
import math
import os, sys, time
import simplejson as json
import requests
import uuid

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

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, **kwargs):
    logger.info('jsonToCsvSaas.JsonToCsvSaas._start')
    XformEmailPrvdr.init('jsonToCsv')
    mPrvdr = MetaPrvdr(self._leveldb, self.actorId)
    self.jobId, self.tsXref = mPrvdr()
    #XformEmailPrvdr.start('jsonToCsv',{})
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
      args = [self.tsXref, self.resolve.jsonFileName]
      pdata = (self.actorId,classRef,json.dumps(args))
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
    if not XformEmailPrvdr.hasMailReady('JsonToCsv'):
      method = 'jsonToCsvSaas.Resolvar._start.' + self.state.current
      errdesc = 'system error'
      logger.error(ex)
      self.sendMail('ERR1',method,errdesc,str(ex))
    else:
      self.sendMail('JsonToCsv')
    try:
      self.putApiRequest(500)
    except Exception as ex:
      logger.error('### putApiRequest failed : ' + str(ex))


  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self,*args):      
    XformEmailPrvdr.sendMail('JsonToCsv',*args)

# -------------------------------------------------------------- #
# getLineCount
# ---------------------------------------------------------------#
def getLineCount(fname):
  with open(fname) as f:
    for i, l in enumerate(f):
      pass
  return i

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.method = '__init__'
    self.__dict__['EVAL_XFORM_META'] = self.EVAL_XFORM_META
    self.__dict__['NORMALISE_JSON'] = self.NORMALISE_JSON
    self.__dict__['MAKE_ZIP_FILE'] = self.MAKE_ZIP_FILE
    self.__dict__['REMOVE_WORKSPACE'] = self.REMOVE_WORKSPACE

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, mPrvdr):
    self.state.current = 'EVAL_XFORM_META'
    self.jobId, self.tsXref = mPrvdr.getJobVars()
    self.mPrvdr = mPrvdr
    msg = 'jsonToCsvSaas.Resolvar, starting job %s ...'
    logger.info(msg % self.jobId)

	# -------------------------------------------------------------- #
	# EVAL_CVS_META
	# ---------------------------------------------------------------#
  def EVAL_XFORM_META(self):
    self.evalXformMeta()
    self.state.next = 'NORMALISE_JSON'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # NORMALISE_JSON
  # - state.next = 'WRITE_JS_FILE'
  # ---------------------------------------------------------------#
  def NORMALISE_JSON(self):
    self.evalSysStatus()
    self.putXformMeta()
    self.state.transition = 'NORMALISE_ASYNC'
    self.state.inTransition = True
    self.state.next = 'MAKE_ZIP_FILE'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # MAKE_ZIP_FILE
  # - state.next = 'REMOVE_WORKSPACE'
  # ---------------------------------------------------------------#
  def MAKE_ZIP_FILE(self):
    JsonMember.reset()
    self.writeCsvFiles()
    self.makeGZipFile()
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
    kwArgs = {'itemKey':'jsonToCsv'}
    repoMeta = self.mPrvdr.getSaasMeta('SaasXformMngr','xformDomain',kwArgs=kwArgs)
    metaFile = repoMeta['repoName'] + '/' + repoMeta['xformMeta']
    logger.info('jsonToCsv, xform meta file : ' + metaFile)
    if not os.path.exists(metaFile):
      errmsg = 'xform meta file does not exist : %s' % metaFile
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

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
      errmsg = 'jsonToCsv, xform input path does not exist : ' + repoMeta['sysPath']
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    catPath = self.mPrvdr['category']
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'Consumer category %s does not exist in source repo : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      self.newMail('ERR1','system error',errmsg)
      raise Exception
  
    repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('jsonToCsv, input json file repo path : ' + repoPath)

    inputJsonFile = '%s.%s' % (self.jobId, self.mPrvdr['fileExt'])
    logger.info('jsonToCsv, input json file : ' + inputJsonFile)

    jsonFilePath = '%s/%s' % (repoPath, inputJsonFile)
    if not os.path.exists(jsonFilePath):
      errmsg = 'jsonToCsv, xform input zip file does not exist in source repo'
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    if not os.path.exists(self.mPrvdr['workSpace']):
      errmsg = 'xform workspace path does not exist : ' + self.mPrvdr['workSpace']
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    workSpace = self.mPrvdr['workSpace'] + '/' + self.tsXref
    logger.info('jsonToCsv, session workspace : ' + workSpace)
    logger.info('jsonToCsv, creating session workspace ... ')

    cmdArgs = ['mkdir','-p',workSpace]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to create workspace : ' + workSpace
      self.newMail('ERR1','system error',errmsg)
      raise Exception

    cmdArgs = ['cp',jsonFilePath,workSpace]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to copy input json file to workspace'
      self.newMail('ERR1','system error',errmsg)
      raise Exception
    
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
      errmsg = 'json file split linux command failed : %s\n cmd dir : %s' \
                                                    % (str(cmdArgs), workSpace)
      self.newMail('ERR1','system error',errmsg)
      raise Exception

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
        logger.info('jsonToCsv %s meta index : %d' % (nodeName, metaIndex))
        logger.info('%s meta item : %s ' % (nodeName, str(csvMeta)))
    except Exception as ex:
      errmsg = 'failed to store %s meta item' % nodeName
      self.newMail('ERR1',errmsg,str(ex))
      raise

  # -------------------------------------------------------------- #
  # writeCsvFiles
  # ---------------------------------------------------------------#
  def writeCsvFiles(self):
    self.method = 'writeCsvFiles'
    try:
      for csvName in self.xformMeta.get(attrName='tableName'):
        csvPath = '%s/%s.csv' % (self.workSpace, csvName)
        with open(csvPath,'w') as csvFh:
          self.writeCsvFile(csvFh, csvName)
    except Exception as ex:
      errmsg = 'write csv file %s failed : %s' % (csvName, str(ex))
      self.newMail('ERR1','system error',errmsg)
      raise Exception

  # -------------------------------------------------------------- #
  # writeCsvFile
  # ---------------------------------------------------------------#
  def writeCsvFile(self, csvFh, csvName):
    self.method = 'writeCsvFile'
    self.writeHeader(csvFh, csvName)
    jobRange = xrange(1,self.jobRange+1)
    for jobNum in jobRange:
      rangeKey = '%s|%s|%02d' % (self.tsXref, csvName, jobNum)
      rowIter = self.getIterByRangeKey(rangeKey)
      for row in rowIter:
        record = ','.join(json.loads(row))
        csvFh.write(record + '\n')

  # -------------------------------------------------------------- #
  # writeHeader
  # ---------------------------------------------------------------#
  def writeHeader(self, csvFh, csvName):
    self.method = 'writeHeader'
    try:
      dbKey = '%s|%s|header' % (self.tsXref, csvName)
      header = self._leveldb.Get(dbKey)
      record = ','.join(json.loads(header))
      csvFh.write(record + '\n')
    except KeyError:
      errmsg = 'JsonMember.factory session var is not reset properly'
      raise Exception(errmsg)

  # -------------------------------------------------------------- #
  # getIterByRangeKey
  # ---------------------------------------------------------------#
  def getIterByRangeKey(self, rangeKey):
    self.method = 'getIterByRangeKey'
    keyLow = rangeKey + '|00001'
    keyHigh = rangeKey + '|99999'
    rowIter = self._leveldb.RangeIter(key_from=keyLow,key_to=keyHigh)
    while True:
      try:
        key, value = rowIter.next()
        yield value
      except StopIteration:
        break

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
      errmsg = 'csv tar gzipfile creation failed : ' + str(ex)
      self.newMail('ERR1','system error',errmsg)
      raise Exception

  # -------------------------------------------------------------- #
  # removeWorkSpace
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    self.method = 'removeWorkSpace'
    logger.info('csvToJson, removing workspace ...')
    try:
      cmdArgs = ['rm','-rf',self.workSpace]
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to remove workspace : ' + self.workSpace
      self.newMail('ERR1',errmsg,str(ex))
      raise Exception
    else:
      msg = 'jsonToCsvSaas.Resolvar, job %s is now complete'
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
    XformEmailPrvdr.newMail('jsonToCsv',bodyKey,method,*args)

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
            logger.info('jsonToCsvSaas.NormaliseLstnr : send resume signal to the caller')
            self.putApiRequest(201)
    elif event.exception:
      self.putApiRequest(500)

  # -------------------------------------------------------------- #
  # register - add a list of live job ids
  # ---------------------------------------------------------------#
  def register(self, jobRange=None):
    logger.info('jsonToCsvSaas.NormaliseLstnr register : ' + str(jobRange))
    self.actors = actors = []
    for _ in jobRange:
      actors += [str(uuid.uuid4())]
    return actors

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'jsonxform.jsonToCsvSaas:jsonToCsvSaas'
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
    XformEmailPrvdr.newMail('jsonToCsv',bodyKey,method,*args)

