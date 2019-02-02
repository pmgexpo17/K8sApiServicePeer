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
from apitools.hardhash import LeveldbClient
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
from threading import RLock
import csv
import logging
import os, sys, time
import simplejson as json
import subprocess
import zmq

logger = logging.getLogger('apiservice.async')

#------------------------------------------------------------------#
# HHProvider
# factory dict id to enable multiple current sessions, where the 
# the session id is tsXref
#------------------------------------------------------------------#
class HHProvider(object):
  lock = RLock()
  factory = {}

  def __init__(self, tsXref, routerAddr):
    self.tsXref = tsXref
    self.prefix = '%s|' % tsXref
    self.routerAddr = routerAddr
    self._cache = {}
    self._nodeName = {}
    self._context = zmq.Context.instance()
    self.lock = RLock()    

	#------------------------------------------------------------------#
	# start -
  # - normaliseFactory threads depend on initial HHProvider creation
	#------------------------------------------------------------------#
  @staticmethod
  def start(tsXref, routerAddr):
    with HHProvider.lock:
      if tsXref not in HHProvider.factory:
        HHProvider.factory[tsXref] = HHProvider(tsXref, routerAddr)

	#------------------------------------------------------------------#
	# remove
	#------------------------------------------------------------------#
  @staticmethod
  def delete(tsXref):
    with HHProvider.lock:
      if tsXref in HHProvider.factory:
        logger.info('### HHProvider %s will be deleted ...' % tsXref)
        HHProvider.factory[tsXref].terminate()
        del HHProvider.factory[tsXref]  
        logger.info('### HHProvider %s is deleted' % tsXref)

	#------------------------------------------------------------------#
	# close
	#------------------------------------------------------------------#
  @staticmethod
  def close(tsXref, clientId):
    with HHProvider.lock:
      HHProvider.factory[tsXref]._close(clientId)

	#------------------------------------------------------------------#
	# _close
	#------------------------------------------------------------------#
  def _close(self, clientId):
    try:
      self._cache[clientId].close()
      del self._cache[clientId]
    except KeyError:
      raise Exception('%s factory, client %s not found in cache' % (self.tsXref, clientId))
    except zmq.ZMQError as ex:
      raise Exception('%s factory, client %s closure failed : %s' % (self.tsXref, clientId, str(ex)))

	#------------------------------------------------------------------#
	# terminate
	#------------------------------------------------------------------#
  def terminate(self):
    try:
      [client.close() for key, client in self._cache.items()]
      self._context.term()
    except zmq.ZMQError as ex:
      raise Exception('%s factory closure failed : %s' % (self.tsXref, str(ex)))

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  def get(self, clientId):
    with self.lock:
      if clientId in self._cache:
        return self._cache[clientId]
      hhClient = LeveldbClient.make(self._context, self.routerAddr, prefix=self.prefix)
      self._cache[clientId] = hhClient
      logger.info('### %s factory, client created by clientId : %s' % (self.tsXref, clientId))
      return hhClient

# -------------------------------------------------------------- #
# NormaliserFactory
# ---------------------------------------------------------------#
class NormaliserFactory(object):

  def __init__(self, leveldb, jobId, caller):
    self._leveldb = leveldb
    self.jobId = jobId    
    self.caller = caller
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self, tsXref, taskNum):
    self.method = '__call__'
    try:
      logger.info('### NormaliserFactory is called ... ###')
      # unique nodeName and xformMeta is mapped by taskNum
      dbKey = '%s|XFORM|META|%d' % (tsXref, taskNum)
      jsData = self._leveldb.Get(dbKey)
      xformMeta = json.loads(jsData)
      logger.info('csvToJsonXform.NormaliserFactory - name, classTag : %s, %s ' 
          % (xformMeta['nodeName'], xformMeta['classTag']))
      try:
        className = 'Normalise' + xformMeta['classTag']
        klass = getattr(sys.modules[__name__], className)
      except AttributeError:
        errmsg = '%s class does not exist in %s' % (className, __name__)
        raise Exception(errmsg)
      dbKey = '%s|REPO|workspace' % tsXref
      workSpace = self._leveldb.Get(dbKey)

      csvFileName = '%s.csv' % xformMeta['tableName']
      logger.info('### normalise workspace : ' + workSpace)
      logger.info('### csv filename : ' + csvFileName)
      csvFilePath = '%s/%s' % (workSpace, csvFileName)
      if not os.path.exists(csvFilePath):
        errmsg = '%s does not exist in workspace' % csvFileName
        raise Exception(errmsg)

      obj = klass()
      obj.start(tsXref, xformMeta)      
      obj.normalise(csvFilePath)
    # apscheduler will only catch BaseException, EVENT_JOB_ERROR will not fire otherwise      
    except NormaliseError as ex:
      errmsg = 'csvfile :' + csvFileName
      self.newMail('ERR2','normalise failed',errmsg)
      raise BaseException(ex)
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise BaseException(ex)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)

# -------------------------------------------------------------- #
# NormaliseError
# ---------------------------------------------------------------#
class NormaliseError(Exception):
  pass

#------------------------------------------------------------------#
# Normaliser
#------------------------------------------------------------------#
class Normaliser(object):
  __metaclass__ = ABCMeta

  def __init__(self):
    self._hh = None
    self.isRoot = False
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # _putObjects
  # -------------------------------------------------------------- #
  @abstractmethod
  def _putObjects(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, csvFilePath):
    self.method = 'normalise'
    try:
      with open(csvFilePath) as csvfh:
        csvReader = csv.reader(csvfh,quotechar='"', 
                                    doublequote=False, escapechar='\\')
        keys = next(csvReader)
        dbKey = '%s|columns' % self.name
        self._hh[dbKey] = keys
        self.recnum = 0
        for values in csvReader:					
          self.recnum += 1
          self.putObjects(keys, values)
        if self.isRoot:
          dbKey = '%s|rowcount' % self.name
          self._hh[dbKey] = self.recnum
          logger.info('###-rootnode-%s rowcount, key, value : %s, %d' % (self.name, dbKey, self.recnum))
        logger.info('#### %s rowcount : %d' % (self.name, self.recnum))
    except NormaliseError:
      raise
    except csv.Error as ex:
      errmsg = 'csv reader error, file: %s, line: %d' \
                                      % (self.name, csvReader.line_num)
      self.newMail('ERR2',errmsg,str(ex))
      raise NormaliseError(ex)
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

	#------------------------------------------------------------------#
	# putObjects
	#------------------------------------------------------------------#
  def putObjects(self, keys, values):
    self.method = 'putObjects'
    try:
      self._putObjects(keys, values)
    except NormaliseError:
      raise
    except KeyError as ex:
      errdesc = 'all keys : ' + str(keys)
      errmsg = 'key error : ' + str(ex)
      self.newMail('ERR2',errdesc, errmsg)
      raise NormaliseError(errmsg)
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

	#------------------------------------------------------------------#
	# getUkValue
	#------------------------------------------------------------------#
  def getUkValue(self, record):
    ukvalue = [record[key] for key in self.ukey]
    return '|'.join(ukvalue)

	#------------------------------------------------------------------#
	# getFkValue
	#------------------------------------------------------------------#
  def getFkValue(self, record):
    fkvalue = [record[key] for key in self.fkey]
    return '|'.join(fkvalue)

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  def start(self, tsXref, xformMeta):
    self.method = 'start' 
    self.applyMeta(tsXref, xformMeta)
    self._hh = HHProvider.factory[tsXref].get(self.name)

	#------------------------------------------------------------------#
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, tsXref, xformMeta):
    self.method = 'applyMeta'
    try:
      self.tsXref = tsXref 
      self.name = xformMeta['nodeName']
      self.ukey = xformMeta['ukey']
      self.hasParent = xformMeta['parent'] is not None
      self.fkey = xformMeta['parent']['fkey'] if self.hasParent else None
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)

#------------------------------------------------------------------#
# NormaliseRN1
#------------------------------------------------------------------#
class NormaliseRN1(Normaliser):
  '''
  Normaliser, model - RootNode1
  ''' 
  def __init__(self, *args):
    super(NormaliseRN1, self).__init__(*args)
    self.isRoot = True

	#------------------------------------------------------------------#
	# _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    recordD = dict(zip(keys,values))
    ukvalue = self.getUkValue(recordD)
    self._hh[ukvalue] = list(zip(keys,values))
    dbKey = '%s|%05d' % (self.name, self.recnum)
    self._hh[dbKey] = ukvalue

#------------------------------------------------------------------#
# NormaliseUKN1
#------------------------------------------------------------------#
class NormaliseUKN1(Normaliser):
  '''
  Normaliser, model - Unique Key Node 1
  ''' 
  def __init__(self, *args):
    super(NormaliseUKN1, self).__init__(*args)

	#------------------------------------------------------------------#
	# _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    recordD = dict(zip(keys,values))
    if self.ukey:
      ukvalue = self.getUkValue(recordD)
      self._hh[ukvalue] = list(zip(keys,values))
      if self.hasParent:
        fkvalue = self.getFkValue(recordD)
        dbKey = '%s|%s' % (self.name,fkvalue)
        # store ukey value by fkey for retrieving the full record by ukey at compile time
        self._hh.append(dbKey, ukvalue)

#------------------------------------------------------------------#
# NormaliseFKN1
#------------------------------------------------------------------#
class NormaliseFKN1(Normaliser):
  '''
  Normaliser, model - Foreign Key Node 1
  ''' 
  def __init__(self, *args):
    super(NormaliseFKN1, self).__init__(*args)

	#------------------------------------------------------------------#
	# _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    recordD = dict(zip(keys,values))
    fkvalue = self.getFkValue(recordD)
    dbKey = '%s|%s' % (self.name,fkvalue)
    recordL = list(zip(keys,values))
    self._hh.append(dbKey, recordL)

# -------------------------------------------------------------- #
# CompileError
# ---------------------------------------------------------------#
class CompileError(Exception):
  pass

# -------------------------------------------------------------- #
# CompilerFactory
# ---------------------------------------------------------------#
class CompilerFactory(object):
  
  def __init__(self, leveldb, tsXref):
    self._leveldb = leveldb
    self.tsXref = tsXref
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  def get(self, nodeName, parent=None):
    self.method = 'get'
    try:
      dbKey = '%s|XFORM|META|%s' % (self.tsXref, nodeName)
      metaData = self._leveldb.Get(dbKey)
      xformMeta = json.loads(metaData)
      logger.info('csvToJsonXform.CompilerFactory - name, classTag : %s, %s ' 
          % (xformMeta['tableName'], xformMeta['classTag']))
      try:
        className = 'Compile' + xformMeta['classTag']
        klass = getattr(sys.modules[__name__], className)
      except AttributeError:
        errmsg = 'xformMeta class %s does not exist in %s' % (className, __name__)
        self.newMail('ERR2','system error',errmsg)
        raise CompileError(errmsg)
      obj = klass(self, parent)
      obj.start(self.tsXref, xformMeta)
      return obj
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

  # -------------------------------------------------------------- #
  # getMembers
  # ---------------------------------------------------------------#
  def getMembers(self, parent):
    self.method = 'getMembers'
    try:
      memberList = []
      for nodeName in parent.children:
        logger.info('%s member : %s ' % (parent.name, nodeName))
        memberList.append(self.get(nodeName, parent))
      return memberList
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)

#------------------------------------------------------------------#
# Compiler
#------------------------------------------------------------------#
class Compiler(object):
  __metaclass__ = ABCMeta

  def __init__(self, factory, parent):
    self._hh = None
    self.factory = factory
    self.parent = parent
    self.isRoot = False
    self.ukeys = []
    self.fkeyMap = {}
    self.jsObject = {}
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  @abstractmethod
  def compile(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # getJsObject
  # -------------------------------------------------------------- #
  @abstractmethod
  def getJsObject(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  def start(self, tsXref, xformMeta):
    self.method = 'start' 
    self.applyMeta(tsXref, xformMeta)
    self._hh = HHProvider.factory[tsXref].get(self.name)

	#------------------------------------------------------------------#
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, tsXref, xformMeta):
    self.method = 'applyMeta'
    try:      
      self.tsXref = tsXref
      self.name = xformMeta['nodeName']
      self.ukeyName = '|'.join(xformMeta['ukey']) if xformMeta['ukey'] else None
      self.fkeyName = '|'.join(xformMeta['parent']['fkey']) if not self.isRoot else None
      self.nullPolicy = xformMeta['nullPolicy']
      self.isLeafNode = xformMeta['children'] is None
      self.children = xformMeta['children']
      self.ukeyType = None
      self.subType = None
      if self.isRoot:
        return
      ukeyPolicy = xformMeta['parent']['ukeyPolicy']    
      self.ukeyType = ukeyPolicy['type']
      self.subType = ukeyPolicy['subType']
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# getMembers
	#------------------------------------------------------------------#
  def getMembers(self):
    self.method = 'getMembers'
    try:
      if self.isLeafNode:
        return None
      memberList = self.factory.getMembers(self)
      if self.isRoot:  
        return memberList
      memberList.reverse()
      return memberList
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# build
	#------------------------------------------------------------------#
  def build(self):
    self.method = 'build'
    try:
      for ukey in self.parent.ukeys:
        jsObject = self.parent.jsObject[ukey]
        if jsObject: # test if parent obj != {}
          jsObject[self.name] = self.getJsObject(ukey)
          self.parent.jsObject[ukey] = jsObject
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# getEmptyObj
  # only relevent if subType == HASH
	#------------------------------------------------------------------#
  def getEmptyObj(self):
    self.method = 'getEmptyObject'
    try:
      if self.nullPolicy['IncEmptyObj']:
        dbKey = '%s|columns' % self.name
        columns = self._hh[dbKey]
        return OrderedDict([(colname, "") for colname in columns])
      return {}
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self):
    pass

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)

#------------------------------------------------------------------#
# CompileRN1
#------------------------------------------------------------------#
class CompileRN1(Compiler):
  '''
  CompileJson, model - RootNode1
  ''' 
  def __init__(self, *args):
    super(CompileRN1, self).__init__(*args)
    self.isRoot = True

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  def compile(self, rowNum):
    self.method = 'compile'
    try:
      dbKey = '%s|%s' % (self.name, rowNum)
      rootUkey = self._hh[dbKey]
      self.ukeys = []
      self.fkeyMap = {}
      self.jsObject = {}
      self.fkeyMap[rootUkey] = [rootUkey]
      self.ukeys = [rootUkey]
      self.jsObject[rootUkey] = OrderedDict(self._hh[rootUkey])
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','CompileRN1 failed',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, rowNum):
    self.method = 'getJsObject'
    try:
      return self._hh[rowNum]
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self, rowNum):
    self.method = 'putJsObject'    
    # special case : json object build is complete, now put it to db
    try:
      jsObject = {}
      rootUkey = self.ukeys[0]
      jsObject[self.name] = self.jsObject[rootUkey]
      self._hh[rowNum] = jsObject
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

#------------------------------------------------------------------#
# CompileUKN1
#------------------------------------------------------------------#
class CompileUKN1(Compiler):
  '''
  CompileJson, model - Unique Key Node1
  ''' 
  def __init__(self, *args):
    super(CompileUKN1, self).__init__(*args)

	#------------------------------------------------------------------#
	# compile
	#------------------------------------------------------------------#
  def compile(self):
    self.method = 'compile'
    try:
      self.ukeys = []
      self.fkeyMap = {}
      self.jsObject = {}
      for fkey in self.parent.ukeys:
        if fkey is None:
          return
        dbKey = '%s|%s' % (self.name,fkey)
        fkRecord = self._hh[dbKey]
        if not fkRecord:
          # 0 child objects exist 
          self.fkeyMap[fkey] = None
          self.jsObject[fkey] = [] if self.subType == 'LIST' else self.getEmptyObj()
          continue
        self.fkeyMap[fkey] = []
        for ukey in fkRecord:
          self.fkeyMap[fkey] += [ukey]
          self.ukeys += [ukey]        
          self.jsObject[ukey] = OrderedDict(self._hh[ukey])
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','CompileUKN1 failed',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, fkey):
    self.method = 'getJsObject'
    try:
      ukeys = self.fkeyMap[fkey]
      if not ukeys:
        return self.jsObject[fkey]
      jsObject = [] if self.subType == 'LIST' else {}
      for ukey in ukeys:
        if self.subType == 'LIST':
          jsObject += [self.jsObject[ukey]]
        else:
          jsObject[ukey] = self.jsObject[ukey]
      return jsObject
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

#------------------------------------------------------------------#
# CompileJsonFKN1
#------------------------------------------------------------------#
class CompileFKN1(Compiler):
  '''
  CompileJson, model - Foreign Key Node1
  Foreign Key model is defined by hasUkey == False
  In this case, the ukeyPolicy type, subType are always OneToMany, LIST respectively
  ''' 
  def __init__(self, *args):
    super(CompileFKN1, self).__init__(*args)

	#------------------------------------------------------------------#
	# compile
	#------------------------------------------------------------------#
  def compile(self):
    self.method = 'compile'
    try:
      self.ukeys = []
      self.fkeyMap = {}
      self.jsObject = {}
      for fkey in self.parent.ukeys:
        if fkey is None:
          return
        dbKey = '%s|%s' % (self.name,fkey)
        fkRecord = self._hh[dbKey]
        if not fkRecord:
          # 0 child objects exist 
          self.fkeyMap[fkey] = None
          self.jsObject[fkey] = []
          continue
        self.fkeyMap[fkey] = []
        for jsData in fkRecord:
          jsObject = OrderedDict(jsData)
          # there is no fkey to ukey mapping when ukey is not defined
          self.fkeyMap[fkey] = fkey
          if fkey in self.jsObject:
            self.jsObject[fkey] += [jsObject]
          else:
            self.jsObject[fkey] = [jsObject]
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','CompileFKN1 failed',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, fkey):
    self.method = 'getJsObject'
    try:
      return self.jsObject[fkey]
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

#------------------------------------------------------------------#
# ComposeError
#------------------------------------------------------------------#
class ComposeError(Exception):
  pass

#------------------------------------------------------------------#
# JsonComposer
#------------------------------------------------------------------#
class JsonComposer(object):
  def __init__(self, leveldb, jobId, caller):
    self._leveldb = leveldb
    self.jobId = jobId
    self.caller = caller
    self.method = '__init__'

  #------------------------------------------------------------------#
	# __call__
	#------------------------------------------------------------------#
  def __call__(self, tsXref, taskNum):
    self.method = '__call__'
    try:
      logger.info('### JsonComposer is called ... ###')
      dbKey = '%s|TASK|workspace' % tsXref
      self.workSpace = self._leveldb.Get(dbKey)
      dbKey = '%s|TASK|OUTPUT|jsonFile' % tsXref
      self.jsonFile = self._leveldb.Get(dbKey)
      dbKey = '%s|XFORM|rootname' % tsXref
      self.rootName = self._leveldb.Get(dbKey)

      logger.info('### task workspace : ' + self.workSpace)
      logger.info('### output json file : ' + self.jsonFile)
      logger.info('### xform root nodename : ' + self.rootName)

      clientId = 'task%02d' % taskNum
      self._hh = HHProvider.factory[tsXref].get(clientId)
      self.compileJson(tsXref)
      self.writeJsonFile()
      self.compressFile()
      HHProvider.close(tsXref, clientId)
      
    except (CompileError, ComposeError) as ex:
      raise BaseException(ex)
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise BaseException(ex)

  #------------------------------------------------------------------#
	# compileJson
	#------------------------------------------------------------------#
  def compileJson(self, tsXref):
    self.method = 'compileJson'
    try:      
      self.factory = CompilerFactory(self._leveldb, tsXref)
      self.rootMember = self.factory.get(self.rootName)
      jsonDom = list(self.getJsDomAsQueue())
      for rootKey in self.rootKeySet():
        self.compileJsObject(rootKey, jsonDom)
      logger.info('### csvToJson compile step is done ...')
    except (CompileError, ComposeError):
      raise
    except Exception as ex:
      self.newMail('ERR2','compile error',str(ex))
      raise ComposeError(ex)

	#------------------------------------------------------------------#
  # compileJsObject
  #
  # Convert the python object to json text and write to the output file
  # Return a policy object, adding all the child components to the
  # policy details object
	#------------------------------------------------------------------#
  def compileJsObject(self, rootKey, jsonDom):
    self.method = 'compileJsObject'
    try:
      self.rootMember.compile(rootKey)
      for nextMember in jsonDom:
        nextMember.compile()
      for nextMember in jsonDom:
        if nextMember.isLeafNode:
          self.buildJsObject(nextMember)
      self.rootMember.putJsObject(rootKey)
    except ComposeError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise ComposeError(ex)

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
    except ComposeError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise ComposeError(ex)

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self):
    self.method = 'getJsDomAsQueue'
    try:
      from collections import deque
      logger.info("### root name  : " + self.rootName)
      domQueue = deque(self.rootMember.getMembers())
      while domQueue:
        nextMember = domQueue.popleft()
        memberList = nextMember.getMembers()
        if memberList:
          domQueue.extendleft(memberList)        
        yield nextMember
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise ComposeError(ex)

  #------------------------------------------------------------------#
	# rootKeySet
	#------------------------------------------------------------------#
  def rootKeySet(self):
    self.method = 'rootKeySet'
    try:
      dbKey = '%s|rowcount' % self.rootName
      rowCount = int(self._hh[dbKey])      
      logger.info('### %s row count : %d' % (self.rootName, rowCount))

      for rowNum in xrange(1, rowCount+1):
        yield '%05d' % rowNum

    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise ComposeError(ex)

  # -------------------------------------------------------------- #
  # writeJsonFile
  # ---------------------------------------------------------------#
  def writeJsonFile(self):
    self.method = 'writeJsonFile'
    try:
      self.rootMember._hh.setRestoreMode('JSON')
      jsonPath = '%s/%s' % (self.workSpace, self.jsonFile)
      with open(jsonPath,'w') as jsfh:
        for rootKey in self.rootKeySet():
          jsObject = self.rootMember.getJsObject(rootKey)
          jsfh.write(jsObject + '\n')
    except Exception as ex:
      errmsg = 'write failed : ' + self.jsonFile
      self.newMail('ERR2',errmsg,str(ex))
      raise ComposeError(errmsg)

  # -------------------------------------------------------------- #
  # compressFile
  # ---------------------------------------------------------------#
  def compressFile(self):
    self.method = 'compressFile'
    try:
      logger.info('gzip %s ...' % self.jsonFile)
      subprocess.call(['gzip',self.jsonFile],cwd=self.workSpace)
    except Exception as ex:
      errmsg = '%s gzip failed' % self.jsonFile
      self.newMail('ERR2',errmsg,str(ex))
      raise ComposeError(errmsg)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJsonSaas',bodyKey,method,*args)
