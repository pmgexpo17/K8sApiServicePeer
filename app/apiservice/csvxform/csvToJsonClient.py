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
from apibase import AppDirector, AppState, AppResolvar, AppListener, MetaReader, logger
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
from threading import RLock
import simplejson as json
import os, sys, time
import requests

# -------------------------------------------------------------- #
# ClientError
# ---------------------------------------------------------------#
class ClientError(Exception):
  pass

# -------------------------------------------------------------- #
# CsvToJson
# ---------------------------------------------------------------#
class CsvToJson(AppDirector):

  def __init__(self, leveldb, actorId):
    super(CsvToJson, self).__init__(leveldb, actorId)
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = Resolvar()
    self.resolve.state = self.state
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # runApp
  # - override to get the callee actorId sent from CsvToJsonSaas
  # ---------------------------------------------------------------#
  def runApp(self, signal=None, callee=None, tsXref=None):
    if callee:
      msg = 'csvToJsonClient.CsvToJson.runApp - callee, tsXref : %s, %s'
      logger.info(msg % (callee, tsXref))
      self.callee = callee
      self.resolve.tsXref = tsXref
    super(CsvToJson, self).runApp(signal)

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, **kwargs):
    logger.info('csvToJsonClient.CsvToJson._start')
    self.method = '_start'
    # if emailPrvdr.init fails, AppDirector will catch the exception
    XformEmailPrvdr.init('csvToJson')
    try:
      mPrvdr = MetaPrvdr(self._leveldb, self.actorId)
      self.jobId = mPrvdr()
      #XformEmailPrvdr.start('csvToJson',{})
      self.resolve._start(mPrvdr)
      self.mPrvdr = mPrvdr
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ClientError(ex)
    
  # -------------------------------------------------------------- #
  # advance
  # -------------------------------------------------------------- #
  def advance(self, signal=None):
    self.method = 'advance'
    if self.state.transition == 'CSV_TO_JSON_SAAS':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('CSV_TO_JSON_SAAS is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'CSV_TO_JSON_SAAS failed, returned error signal : %d' % signal
        self.newMail('ERR1','system error',errmsg)
        raise ClientError(errmsg)
    elif self.state.complete:
      msg = '### CsvToJsonClient, job %s is now complete'
      logger.info(msg % self.jobId)
    if self.state.hasNext:
      self.state.current = self.state.next
    return self.state
    
  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):
    self.method = 'quicken'
    try:
      self.putApiRequest()
    except Exception as ex:
      self.newMail('ERR1','api request failed',str(ex))
      raise ClientError(ex)
      
  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    self.method = 'putApiRequest'
    if self.state.transition == 'CSV_TO_JSON_SAAS':
      classRef = 'csvxform.csvToJsonSaas:CsvToJsonSaas'
      listener = 'csvxform.csvToJsonSaas:NormaliseLstnr'
      pdata = (classRef,listener,self.actorId,self.mPrvdr['service:hostName'])
      params = '{"type":"director","id":null,"service":"%s","listener":"%s","caller":"%s","callerHost":"%s"}' % pdata
      pMeta = self.mPrvdr.getProgramMeta()
      data = [('job',params),('pmeta',pMeta)]
      apiUrl = 'http://%s/api/v1/smart' % self.mPrvdr['service:hostName']
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.complete:
      classRef = 'csvxform.csvToJsonSaas:CsvToJsonSaas'
      pdata = (self.callee,classRef,json.dumps({"signal":201}))
      params = '{"type":"director","id":"%s","service":"%s","args":[],"kwargs":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://%s/api/v1/smart' % self.mPrvdr['service:hostName']
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onError
  # - since muliple concurrent jobs may fail and trigger onError
  # - limit evalError to run once
  # ---------------------------------------------------------------#
  def onError(self, ex):
    logger.info('csvToJsonClient.CsvToJson.onError')
    # test if the error is already handled    
    if isinstance(ex,(ClientError,ResolveError,MetaPrvdrError)):
      self.sendMail()
    else:
      # in this case the source method is not visible, so send the state info
      stateDesc = 'CsvToJsonClient.state.current : ' + self.state.current
      self.sendMail('ERR1',stateDesc,'system error',str(ex))

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):      
    XformEmailPrvdr.sendMail('csvToJson',*args)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJson',bodyKey,method,*args)

# -------------------------------------------------------------- #
# ResolveError
# ---------------------------------------------------------------#
class ResolveError(Exception):
  pass

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):
  
  def __init__(self):
    self.method = '__init__'
    self.__dict__['CSV_TO_JSON'] = self.CSV_TO_JSON
    self.__dict__['DOWNLOAD_JSON_FILE'] = self.DOWNLOAD_JSON_FILE

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, mPrvdr):
    try:
      self.method = '_start'
      self.jobId = mPrvdr.getJobVars()
      self.mPrvdr = mPrvdr
      msg = 'csvToJsonClient.Resolvar starting job %s ...'
      logger.info(msg % self.jobId)
      self.state.current = 'CSV_TO_JSON'
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # CSV_TO_JSON
  # - state.next = 'WRITE_JSON_FILE'
  # ---------------------------------------------------------------#
  def CSV_TO_JSON(self):
    try:
      self.state.transition = 'CSV_TO_JSON_SAAS'
      self.state.inTransition = True
      self.state.next = 'DOWNLOAD_JSON_FILE'
      self.state.hasNext = True
      return self.state
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # DOWNLOAD_JSON_FILE
  # ---------------------------------------------------------------#
  def DOWNLOAD_JSON_FILE(self):
    try:
      self.downloadJsonFile()
      self.uncompressFile()
      self.state.hasNext = False
      self.state.complete = True
      self.state.hasSignal = True
      return self.state
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # downloadJsonFile
  # ---------------------------------------------------------------#
  def downloadJsonFile(self):
    self.method = 'downloadJsonFile'
    repoMeta = self.mPrvdr.getSaasMeta('SaasRepoMngr','repoDomain')
    if not os.path.exists(repoMeta['sysPath']):
      errmsg = 'xform output path does not exist : ' + repoMeta['sysPath']
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)

    catPath = self.mPrvdr['category']
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'consumer category %s does not exist in : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)
  
    self.repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('output json gzipfile repo path : ' + self.repoPath)

    self.jsonZipFile = '%s.%s' % (self.jobId, self.mPrvdr['fileExt'])
    logger.info('output json gzipfile : ' + self.jsonZipFile)

    zipFilePath = '%s/%s' % (self.repoPath, self.jsonZipFile)
    dstream = self.getFileStream()

    try:
      with open(zipFilePath, 'wb') as fhwb:
        for chunk in dstream.iter_content(chunk_size=1024): 
          if chunk: # filter out keep-alive new chunks
            fhwb.write(chunk)
    except Exception as ex:
      errmsg = '%s write failed : ' + self.jsonZipFile
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # getFileStream
  # ---------------------------------------------------------------#
  def getFileStream(self):
    self.method = 'getFileStream'
    try:
      dstream = self._getFileStream()
    except Exception as ex:
        errmsg = 'json gzip file stream api request failed'
        self.newMail('ERR1',errmsg,str(ex))
        raise ResolveError(ex)
    if dstream.status_code != 201:
      errmsg = 'json gzip file stream api request failed : %s' % dstream.text
      self.newMail('ERR1','system error',errmsg)
      raise ResolveError(errmsg)
    return dstream

  # -------------------------------------------------------------- #
  # _getFileStream
  # ---------------------------------------------------------------#
  def _getFileStream(self):
    data = (self.tsXref, self.jsonZipFile)
    params = '{"service":"datatxn.dataTxnPrvdr:BinryFileStreamPrvdr","args":%s}' \
                                                        % json.dumps(data)
    data = [('job',params)]
    apiUrl = 'http://%s/api/v1/sync' % self.mPrvdr['service:hostName']
    # NOTE: stream=True parameter
    return requests.post(apiUrl,data=data,stream=True)

  # -------------------------------------------------------------- #
  # uncompressFile
  # ---------------------------------------------------------------#
  def uncompressFile(self):
    self.method = 'uncompressFile'
    try:
      cmdArgs = ['gunzip','-f',self.jsonZipFile]      
      self.sysCmd(cmdArgs,cwd=self.repoPath)
    except Exception as ex:
      errmsg = '%s gunzip failed' % self.jsonZipFile
      self.newMail('ERR1',errmsg,str(ex))
      raise ResolveError(ex)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJson',bodyKey,method,*args)

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
    self.pMeta = {}

  # -------------------------------------------------------------- #
  # __getitem__
  # ---------------------------------------------------------------#
  def __getitem__(self, key):
    self.method = '__getitem__'
    try:
      if key == 'jobId':
        return self.jobId
      elif 'service:' in key:
        key = key.split(':')[1]
        return self.jobMeta['service'][key]
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
      self.method = '__call__'
      pMetadoc = self.getProgramMeta()
      self.jobMeta = pMeta = json.loads(pMetadoc)
      logger.info('### PMETA : ' + str(pMeta))
      kwArgs = {'itemKey':pMeta['jobId']}
      _jobMeta = self.getSaasMeta('SaasEventMngr','eventDomain',queryArgs=['JOB'],kwArgs=kwArgs)
      logger.info('### JOB_META : ' + str(_jobMeta))    
      className = _jobMeta['client']
      self.jobMeta = _jobMeta[className]
      self.jobId = pMeta['jobId']
      logger.info('### CLIENT JOB_META : ' + str(self.jobMeta))    
      className = _jobMeta['service'] 
      self.jobMeta['service'] = _jobMeta[className]
      logger.info('### SAAS JOB_META : ' + str(self.jobMeta['service']))
      logger.info('### jobId : %s' % self.jobId)
      return self.jobId
    except MetaPrvdrError:
      raise
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise MetaPrvdrError(ex)

  # -------------------------------------------------------------- #
  # getJobVars
  # -------------------------------------------------------------- #
  def getJobVars(self):
    return self.jobId

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
      result = json.loads(response.text)
      if 'error' in result:
        raise Exception(result['error'])
      return result
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
    XformEmailPrvdr.newMail('csvToJson',bodyKey,method,*args)

