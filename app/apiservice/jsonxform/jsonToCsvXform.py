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
from apitools.hardhash import LeveldbClient
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
from threading import RLock
import logging
import os, sys, time
import simplejson as json
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
# NormaliseError
# ---------------------------------------------------------------#
class NormaliseError(Exception):
  pass

#------------------------------------------------------------------#
# JsonNormaliser
#------------------------------------------------------------------#
class JsonNormaliser(object):

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
      dbKey = '%s|REPO|workspace' % tsXref
      workSpace = self._leveldb.Get(dbKey)
      dbKey = '%s|TASK|%d|INPUT|jsonFile' % (tsXref, taskNum)
      inputJsonFile = self._leveldb.Get(dbKey)
      dbKey = '%s|XFORM|rootname' % tsXref
      rootName = self._leveldb.Get(dbKey)

      logger.info('### normalise workspace : %s' % workSpace)
      logger.info('### task|%02d input json file : %s ' % (taskNum, inputJsonFile))
      logger.info('### xform root nodename : ' + rootName)

      jsonFilePath = workSpace + '/' + inputJsonFile
      if not os.path.exists(jsonFilePath):
        errmsg = '%s does not exist in workspace' % inputJsonFile
        raise Exception(errmsg)
      factory = MemberFactory(self._leveldb, tsXref, taskNum)
      self.rootMember = factory.get(rootName)
      with open(jsonFilePath) as jsfh:
        jsonDom = list(self.getJsDomAsQueue())
        recnum = 1
        for jsRecord in jsfh:
          self.normalise(jsRecord, jsonDom)
          recnum += 1
        logger.info('### %s rowcount : %d' % (inputJsonFile, recnum))
    # apscheduler will only catch BaseException, EVENT_JOB_ERROR will not fire otherwise
    except (NormaliseError, CompileError) as ex:
      errmsg = 'splitfile, recnum : %s, %d' % (inputJsonFile, recnum)
      self.newMail('ERR2','normalise failed',errmsg)
      raise BaseException(ex)
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise BaseException(ex)

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self):
    self.method = 'getJsDomAsQueue'
    try:
      from collections import deque
      domQueue = deque(self.rootMember.getMembers())
      while domQueue:
        nextMember = domQueue.popleft()
        memberList = nextMember.getMembers()
        if memberList:
          domQueue.extendleft(memberList)        
        yield nextMember
    except CompileError as ex:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, jsRecord, jsonDom):
    self.method = 'normalise'
    try:
      jsObject = self.getJsObject(jsRecord)
      self.rootMember.evalDataset(jsObject)
      for nextMember in jsonDom:
        nextMember.evalDataset()
      self.putDatasets(jsonDom)
    except NormaliseError:
      raise    
    except CompileError as ex:
      self.method = 'normalise'
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

	#------------------------------------------------------------------#
	# getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, jsRecord):
    self.method = 'getJsObject'
    try:
      return json.loads(jsRecord, object_pairs_hook=OrderedDict)
    except ValueError as ex:
      self.newMail('ERR2','json decode error',str(ex))
      raise NormaliseError(ex)

	#------------------------------------------------------------------#
	# putDatasets
	#------------------------------------------------------------------#
  def putDatasets(self, jsonDom):
    self.method = 'putDatasets'
    try:
      hasObjects = True
      while hasObjects:
        hasObjects = self.rootMember.hasDataset
        for nextMember in jsonDom:
          hasObjects = hasObjects or nextMember.hasDataset
          if nextMember.isLeafNode:
            #logger.debug('Leaf node found !')
            self.putDsByMember(nextMember)
        self.rootMember.putDataset()
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)
    
	#------------------------------------------------------------------#
	# putDsByMember
	#------------------------------------------------------------------#
  def putDsByMember(self, nextMember):
    self.method = 'putDsByMember'
    # child objects must be removed and put to db first before 
    # the parent object is put to db, so this means start at the
    # leaf node and traverse back up
    try:
      while not nextMember.isRoot:
        nextMember.putDataset()
        #logger.debug('%s has dataset : %s' % (nextMember.name, str(nextMember.hasDataset)))      
        nextMember = nextMember.parent
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

# -------------------------------------------------------------- #
# CompileError
# ---------------------------------------------------------------#
class CompileError(Exception):
  pass

# -------------------------------------------------------------- #
# MemberFactory
# ---------------------------------------------------------------#
class MemberFactory(object):

  def __init__(self, leveldb, tsXref, taskNum):
    self._leveldb = leveldb
    self.tsXref = tsXref
    self.taskNum = taskNum
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
      logger.info('jsonToCsvXform.MemberFactory - name, classTag : %s, %s ' 
          % (xformMeta['nodeName'], xformMeta['classTag']))
      try:
        className = 'JsonMember' + xformMeta['classTag']
        klass = getattr(sys.modules[__name__], className)
      except AttributeError:
        errmsg = 'xformMeta class %s does not exist in %s' % (className, __name__)
        self.newMail('ERR2','system error',errmsg)
        raise
      obj = klass(self, parent)
      obj.start(self.tsXref, self.taskNum, xformMeta)
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
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

#------------------------------------------------------------------#
# RowIndex
# - enables multitasking csvWriter capacity
# - the related class is a singleton and cls.lock ensures that
# - concurrent writing is atomic
#------------------------------------------------------------------#
class RowIndex(object):
  lock = None
  rowCount = None
  taskNum = None

  # -------------------------------------------------------------- #
  # incCount
  # ---------------------------------------------------------------#
  @classmethod
  def incCount(cls):
    with cls.lock:
      cls.rowCount += 1
      return '%02d|%05d' % (cls.taskNum, cls.rowCount)

	#------------------------------------------------------------------#
	# start
  # - rowcount must be a class variable, to accmulate the total count
  # - when the json DOM contains multiple node class instances
	#------------------------------------------------------------------#
  @classmethod
  def start(cls, taskNum):
    if cls.lock is None:
      logger.info('### start %s counter ###' % cls.__name__)
      cls.lock = RLock()
      cls.taskNum = taskNum
      cls.rowCount = 0

#------------------------------------------------------------------#
# JsonMember
#------------------------------------------------------------------#
class JsonMember(object):
  __metaclass__ = ABCMeta
  lock = None

  def __init__(self, factory, parent):
    self._hh = None
    self.factory = factory
    self.parent = parent
    self.isRoot = False
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  @abstractmethod
  def _getDataset(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  @abstractmethod  
  def putDataset(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  def start(self, tsXref, taskNum, xformMeta):
    self.method = 'start' 
    self.applyMeta(tsXref, taskNum, xformMeta)
    clientId = 'task%02d' % taskNum
    self._hh = HHProvider.factory[tsXref].get(clientId)

	#------------------------------------------------------------------#
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, tsXref, taskNum, xformMeta):
    self.method = 'start'
    try:
      self.tsXref = tsXref
      self.name = xformMeta['nodeName']
      self.ukeyName = '|'.join(xformMeta['ukey']) if xformMeta['ukey'] else None
      self.fkeyName = '|'.join(xformMeta['parent']['fkey']) if not self.isRoot else None
      self.nullPolicy = xformMeta['nullPolicy']
      self.isLeafNode = xformMeta['children'] is None
      self.children = xformMeta['children']
      className = '%s%02d%s' % ('RowIndex',taskNum,xformMeta['tableTag'])
      if hasattr(sys.modules[__name__], className):
        self.rowIndex = getattr(sys.modules[__name__], className)
      else:
        rindexClass = type(className,(RowIndex,),{})
        rindexClass.start(taskNum)
        self.rowIndex = rindexClass
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
	# evalDataset
  # - eval the next csvDataset, ready to put to the datastore
	#------------------------------------------------------------------#
  def evalDataset(self, csvDataset=None):
    self.method = 'evalDataset'
    self.hasDataset = False
    self.csvDataset = csvDataset
    try:
      self.csvDataset = self.getDataset()
      if self.hasDataset:
        self.putHeader()
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# getDataset
	#------------------------------------------------------------------#
  def getDataset(self):
    self.method = 'getDataset'
    try:
      csvDataset = self._getDataset()
    except KeyError as ex:
      errmsg = '%s json object %s does not exist in csv dataset ' \
                                              % (self.name, str(ex))
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(errmsg)
    if not csvDataset: # empty list or dict
      csvDataset = [] # if dict, convert empty dict to list
    elif not isinstance(csvDataset,list):
      csvDataset = [csvDataset]
    self.hasDataset = len(csvDataset) > 0
    return csvDataset    
    
	#------------------------------------------------------------------#
	# putHeader
	#------------------------------------------------------------------#
  def putHeader(self):
    self.method = 'putHeader'
    try:
      csvObject = self.csvDataset[0]
      keyName = self.ukeyName if self.ukeyName else self.fkeyName
      keySet = set(keyName.split('|'))
      if not keySet.issubset(csvObject):
        errmsg = '%s key %s does not exist in json record' \
                                            % (self.name, keyName)
        self.newMail('ERR2','system error',errmsg)
        raise CompileError(errmsg)
      dbKey = '%s|header' % self.name
      self._hh[dbKey] = list(csvObject.keys())
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# putCsvObject
	#------------------------------------------------------------------#
  def putCsvObject(self, csvObject):
    self.method = 'putCsvObject'
    try:
      # self.rowIndex.incCount = klass.incCount
      rowIndex = self.rowIndex.incCount()
      dbKey = '%s|%s' % (self.name, rowIndex)
      self._hh[dbKey] = list(csvObject.values())
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
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

#------------------------------------------------------------------#
# JsonMemberRN1
#------------------------------------------------------------------#
class JsonMemberRN1(JsonMember):
  '''
  NormaliseJson, model - RootNode1
  ''' 
  def __init__(self, *args):
    super(JsonMemberRN1, self).__init__(*args)
    self.isRoot = True

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  def _getDataset(self):
    return self.csvDataset.pop(self.name)

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  def putDataset(self):
    self.method = 'putDataset'
    try:
      if self.hasDataset:
        csvObject = self.csvDataset.pop(0)
        self.putCsvObject(csvObject)
        self.hasDataset = len(self.csvDataset) > 0
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

#------------------------------------------------------------------#
# JsonMemberUKN1
#------------------------------------------------------------------#
class JsonMemberUKN1(JsonMember):
  '''
  JsonMemberJson, model - Unique Key Node 1
  ''' 
  def __init__(self, *args):
    super(JsonMemberUKN1, self).__init__(*args)

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  def _getDataset(self):
    if self.parent.hasDataset:
      return self.parent.csvDataset[0].pop(self.name)

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  def putDataset(self, evalRoot=False):
    self.method = 'putDataset'
    try:
      if self.hasDataset:
        csvObject = self.csvDataset.pop(0)
        self.putCsvObject(csvObject)
        self.hasDataset = len(self.csvDataset) > 0
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

#------------------------------------------------------------------#
# JsonMemberJsonFKN1
#------------------------------------------------------------------#
class JsonMemberFKN1(JsonMember):
  '''
  JsonMemberJson, model - Foreign Key Node 1
  ''' 
  def __init__(self, *args):
    super(JsonMemberFKN1, self).__init__(*args)

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  def _getDataset(self):
    if self.parent.hasDataset:
      return self.parent.csvDataset[0][self.name]

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  def putDataset(self, evalRoot=False):
    self.method = 'putDataset'
    try:
      if self.parent.hasDataset:
        csvDataset = self.parent.csvDataset[0].pop(self.name)
        for csvObject in csvDataset:
          self.putCsvObject(csvObject)
        self.hasDataset = False
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise

#------------------------------------------------------------------#
# ComposeError
#------------------------------------------------------------------#
class ComposeError(Exception):
  pass

#------------------------------------------------------------------#
# CsvComposer
#------------------------------------------------------------------#
class CsvComposer(object):
  def __init__(self, leveldb, jobId, caller):
    self._leveldb = leveldb
    self.jobId = jobId
    self.caller = caller
    self.method = '__init__'

  #------------------------------------------------------------------#
	# __call__
	#------------------------------------------------------------------#
  def __call__(self, tsXref, jobRange, taskNum):
    self.method = '__call__'
    try:
      dbKey = '%s|XFORM|CSVPATH|%d' % (tsXref, taskNum)
      csvPath = self._leveldb.Get(dbKey)
      dbKey = '%s|XFORM|CSVNAME|%d' % (tsXref, taskNum)      
      csvName = self._leveldb.Get(dbKey)
      logger.info('### task|%02d output csv file : %s.csv' % (taskNum, csvName))

      with open(csvPath,'w') as csvfh:
        try:
          writer = CsvWriter(csvName, jobRange)
          writer.start(tsXref, taskNum)
          writer.write(csvfh)
          logger.info('### %s rowcount : %d' % (csvName, writer.total))
        # apscheduler will only catch BaseException, EVENT_JOB_ERROR will not fire otherwise
        except ComposeError as ex:
          errmsg = '%s csv write failed' % csvName
          self.newMail('ERR2',errmsg,str(ex))
          raise BaseException(ex)
        except Exception as ex:
          self.newMail('ERR2','system error',str(ex))
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
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)

#------------------------------------------------------------------#
# CsvWriter
#------------------------------------------------------------------#
class CsvWriter(object):
  def __init__(self, csvName, jobRange):
    self.csvName = csvName
    self.jobRange = jobRange

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  def start(self, tsXref, taskNum):
    self.method = 'start' 
    clientId = 'task%02d' % taskNum
    self._hh = HHProvider.factory[tsXref].get(clientId)

  #------------------------------------------------------------------#
	# write
	#------------------------------------------------------------------#
  def write(self, csvfh):    
    self.method = 'write'
    try:
      self.writeHeader(csvfh)
      self.total = 0
      for key, record in self.csvDataset():
        csvfh.write(','.join(record) + '\n')
        self.total += 1
    except ComposeError:
      raise
    except Exception as ex:
      self.newMail('ERR2','compose error',str(ex))
      raise ComposeError(ex)

  #------------------------------------------------------------------#
	# csvDataset
	#------------------------------------------------------------------#
  def csvDataset(self):
    self.method = 'csvDataset'
    try:
      keyLow = '%s|%02d|%05d' % (self.csvName,1,1)
      keyHigh = '%s|%02d|%05d' % (self.csvName,self.jobRange,99999)
      return self._hh.select(keyLow,keyHigh)
    except Exception as ex:
      self.newMail('ERR2','compose error',str(ex))
      raise ComposeError(ex)

  #------------------------------------------------------------------#
	# writeHeader
	#------------------------------------------------------------------#
  def writeHeader(self, csvfh):
    self.method = 'writeHeader'
    try:
      dbKey = '%s|header' % self.csvName
      record = ','.join(self._hh[dbKey])
      csvfh.write(record + '\n')
    except Exception as ex:
      self.newMail('ERR2','compose error',str(ex))
      raise ComposeError(ex)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('jsonToCsvSaas',bodyKey,method,*args)
