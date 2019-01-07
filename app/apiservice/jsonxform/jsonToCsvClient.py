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
from jsonToCsvXform import JsonMember
from apitools.jsonxform import XformMetaPrvdr
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
import datetime
import math
import logging
import simplejson as json
import os, sys, time
import requests
import uuid

# -------------------------------------------------------------- #
# JsonToCsv
# ---------------------------------------------------------------#
class JsonToCsv(AppDirector):

  def __init__(self, leveldb, actorId):
    super(JsonToCsv, self).__init__(leveldb, actorId)
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = Resolvar()
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # runApp
  # - override to get the callee actorId sent from NormalizeXml partner
  # ---------------------------------------------------------------#
  def runApp(self, signal=None, callee=None, tsXref=None):
    if callee:
      msg = 'jsonToCsvClient.JsonToCsv.runApp - callee, tsXref : %s, %s'
      logger.info(msg % (callee, tsXref))
      self.callee = callee
      self.resolve.tsXref = tsXref
    super(JsonToCsv, self).runApp(signal)

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, **kwargs):
    logger.info('jsonToCsvClient.JsonToCsv._start')
    XformEmailPrvdr.init('jsonToCsv')
    mPrvdr = MetaPrvdr(self._leveldb, self.actorId)
    self.jobId = mPrvdr()
    #XformEmailPrvdr.start('jsonToCsv',{})
    self.resolve._start(mPrvdr)
    self.mPrvdr = mPrvdr
    
  # -------------------------------------------------------------- #
  # advance
  # -------------------------------------------------------------- #
  def advance(self, signal=None):
    if self.state.transition == 'JSON_TO_CSV_SAAS':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('JSON_TO_CSV_SAAS is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'JSON_TO_CSV_SAAS failed, returned error signal : %d' % signal
        logger.error(errmsg)
        raise Exception(errmsg)
    if self.state.hasNext:
      self.state.current = self.state.next
    return self.state
    
  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    try:
      self.putApiRequest()
    except Exception as ex:
      logger.error('### putApiRequest failed : ' + str(ex))
      raise
    if self.state.complete:
      self.putApiRequest()
      msg = '### jsonToCsvClient, job %s is now complete'
      logger.info(msg % self.jobId)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    if self.state.transition == 'JSON_TO_CSV_SAAS':
      classRef = 'jsonxform.jsonToCsvSaas:JsonToCsvSaas'
      listener = 'jsonxform.jsonToCsvSaas:NormaliseLstnr'
      pdata = (classRef,listener,self.actorId,self.mPrvdr['service:hostName'])
      params = '{"type":"director","id":null,"service":"%s","listener":"%s","caller":"%s","callerHost":"%s"}' % pdata
      pMeta = self.mPrvdr.getProgramMeta()
      data = [('job',params),('pmeta',pMeta)]
      apiUrl = 'http://%s/api/v1/smart' % self.mPrvdr['service:hostName']
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.complete:
      classRef = 'jsonxform.jsonToCsvSaas:JsonToCsvSaas'
      pdata = (self.callee,classRef,json.dumps({"signal":201}))
      params = '{"type":"director","id":"%s","service":"%s","args":[],"kwargs":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://%s/api/v1/smart' % self.mPrvdr['service:hostName']
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    # if error is due to delegate failure then don't post an email
    if self.state.inTransition:
      return
    # if CcResolvar has caught an exception an error mail is ready to be sent
    if not XformEmailPrvdr.hasMailReady('JsonToCsv'):
      method = 'jsonToCsvClient.Resolvar._start : ' + self.state.current
      errdesc = 'system error'
      self.sendMail('ERR1',method,errdesc,str(ex))
      return
    self.sendMail()

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):      
    XformEmailPrvdr.sendMail('JsonToCsv',*args)
    
# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):
  
  def __init__(self):
    self.method = '__init__'
    self.__dict__['JSON_TO_CSV'] = self.JSON_TO_CSV
    self.__dict__['DOWNLOAD_ZIPFILE'] = self.DOWNLOAD_ZIPFILE

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, mPrvdr):
    self.method = '_start'
    self.jobId = mPrvdr.getJobVars()
    self.mPrvdr = mPrvdr
    msg = 'jsonToCsvClient.Resolvar starting job %s ...'
    logger.info(msg % self.jobId)
    self.state.current = 'JSON_TO_CSV'

  # -------------------------------------------------------------- #
  # JSON_TO_CSV
  # ---------------------------------------------------------------#
  def JSON_TO_CSV(self):
    self.state.transition = 'JSON_TO_CSV_SAAS'
    self.state.inTransition = True
    self.state.next = 'DOWNLOAD_ZIPFILE'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # DOWNLOAD_ZIPFILE
  # ---------------------------------------------------------------#
  def DOWNLOAD_ZIPFILE(self):
    self.downloadZipFile()
    self.uncompressFile()
    self.state.hasNext = False
    self.state.complete = True
    self.state.hasSignal = True
    return self.state

  # -------------------------------------------------------------- #
  # downloadZipFile
  # ---------------------------------------------------------------#
  def downloadZipFile(self):
    self.method = 'downloadZipFile'
    repoMeta = self.mPrvdr.getSaasMeta('SaasRepoMngr','repoDomain')
    if not os.path.exists(repoMeta['sysPath']):
      errmsg = 'jsonToCsv, xform output path does not exist : ' + repoMeta['sysPath']
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    catPath = self.mPrvdr['category']
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'Consumer category %s does not exist in source repo : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      self.newMail('ERR1','system error',errmsg)
      raise Exception
  
    self.repoPath = '%s/%s/%s' % (repoMeta['sysPath'], catPath, self.jobId)
    logger.info('jsonToCsv, making output csv zipfile repo path : ' + self.repoPath)

    cmdArgs = ['mkdir','-p',self.repoPath]
    try:
      self.sysCmd(cmdArgs)
    except Exception as ex:
      errmsg = 'failed to create output tar gzipfile folder' % self.repoPath
      self.newMail('ERR1','system error',errmsg)
      raise Exception

    self.csvGZipFile = '%s.%s' % (self.jobId, self.mPrvdr['fileExt'])
    logger.info('jsonToCsv, output csv zipfile : ' + self.csvGZipFile)

    csvGZipPath = '%s/%s' % (self.repoPath, self.csvGZipFile)
    dstream = self.getFileStream()
    with open(csvGZipPath, 'wb') as fhwb:
      for chunk in dstream.iter_content(chunk_size=1024): 
        if chunk: # filter out keep-alive new chunks
          fhwb.write(chunk)

  # -------------------------------------------------------------- #
  # getFileStream
  # ---------------------------------------------------------------#
  def getFileStream(self):
    self.method = 'getFileStream'
    try:
      dstream = self._getFileStream()
    except Exception as ex:
        errmsg = 'csv tar gzipfile stream api request failed'
        self.newMail('ERR1',errmsg,str(ex))
        raise Exception(errmsg)
    if dstream.status_code != 201:
      errmsg = 'csv tar gzipfile stream api request failed : %s' % dstream.text
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)
    return dstream

  # -------------------------------------------------------------- #
  # _getFileStream
  # ---------------------------------------------------------------#
  def _getFileStream(self):

    data = (self.tsXref, self.csvGZipFile)
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

    logger.info('gunzip and untar gzipfile %s ...' % self.csvGZipFile)
    cmdArgs = ['tar','-xzf',self.csvGZipFile]
    try:
      self.sysCmd(cmdArgs,cwd=self.repoPath)
    except Exception as ex:
      errmsg = 'failed to expand tar gzipfile %s' % self.csvGZipFile
      self.newMail('ERR1','system error',errmsg)
      raise Exception

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('jsonToCsv',bodyKey,method,*args)

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
    if key == 'jobId':
      return self.jobId
    elif 'service:' in key:
      key = key.split(':')[1]
      return self.jobMeta['service'][key]
    return self.jobMeta[key]

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
    className = _jobMeta['client']
    self.jobMeta = _jobMeta[className]
    self.jobId = pMeta['jobId']
    logger.info('### CLIENT JOB_META : ' + str(self.jobMeta))    
    className = _jobMeta['service'] 
    self.jobMeta['service'] = _jobMeta[className]
    logger.info('### SAAS JOB_META : ' + str(self.jobMeta['service']))
    logger.info('### jobId : %s' % self.jobId)
    return self.jobId

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
      self.newMail('ERR1','leveldb lookup failed',errmsg)
      raise Exception(errmsg)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJson',bodyKey,method,*args)

