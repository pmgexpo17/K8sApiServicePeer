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
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
from threading import RLock
import logging
import os, sys, time
import simplejson as json

logger = logging.getLogger('apiservice.async')

def getFileTag(taskNum):
  tagOrd = int((taskNum-1) / 26)
  tagChr1 = chr(ord('a') + tagOrd)
  tagOrd = int((taskNum-1) % 26)
  tagChr2 = chr(ord('a') + tagOrd)
  return tagChr1 + tagChr2

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
  def __call__(self, tsXref, jsonFileName, taskNum):
    self.method = 'jsonToCsvXform.JsonNormaliser.__call__'
    try:
      dbKey = '%s|REPO|workspace' % tsXref
      workSpace = self._leveldb.Get(dbKey)
      fileTag = getFileTag(taskNum)
      inputJsonFile = jsonFileName + fileTag
      logger.info('### normalise workspace : ' + workSpace)
      logger.info('### split file name : ' + inputJsonFile)
      jsonFilePath = workSpace + '/' + inputJsonFile
      if not os.path.exists(jsonFilePath):
        errmsg = '%s does not exist in workspace' % inputJsonFile
        raise Exception(errmsg)

      JsonMember.start(self._leveldb, tsXref)
      with open(jsonFilePath) as jsfh:
        jsonDom = list(self.getJsDomAsQueue(taskNum))      
        recnum = 1
        for jsRecord in jsfh:
          self.normalise(jsRecord, jsonDom)
          recnum += 1
        logger.info('### JsonNormaliser split file %s rowcount : %d' % (fileTag, recnum))
    # apscheduler will only catch BaseException, EVENT_JOB_ERROR will not fire otherwise
    except (NormaliseError, JsonMemberError) as ex:
      errmsg = 'splitfile, recnum : %s, %d' % (inputJsonFile, recnum)
      self.newMail('ERR2','normalise failed',errmsg)
      raise BaseException(ex)
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise BaseException(ex)

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self, taskNum):
    self.method = 'getJsDomAsQueue'
    try:
      with JsonMember.lock:
        logger.info('task %d MemberFactory.getJsDomAsQueue' % taskNum)
        from collections import deque
        self.rootMember = JsonMember.getRoot(taskNum)
        domQueue = deque(self.rootMember.getMembers())
        while domQueue:
          nextMember = domQueue.popleft()
          #logger.debug('### Next member : ' + nextMember.name)
          memberList = nextMember.getMembers()
          if memberList:
            domQueue.extendleft(memberList)        
          yield nextMember
    except JsonMemberError as ex:
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
    except JsonMemberError as ex:
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
    except JsonMemberError:
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
    except JsonMemberError:
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
# JsonMemberError
# ---------------------------------------------------------------#
class JsonMemberError(Exception):
  pass

# -------------------------------------------------------------- #
# MemberFactory
# ---------------------------------------------------------------#
class MemberFactory(object):

  def __init__(self, leveldb, tsXref):
    self._leveldb = leveldb
    self.tsXref = tsXref
    self.lock = RLock()
    self.rootName = None
    self.method = '__init__'

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  def get(self, nodeName, parent):
    with self.lock:
      return self._get(nodeName, parent)

  # -------------------------------------------------------------- #
  # getRoot
  # ---------------------------------------------------------------#
  def getRoot(self, taskNum):
    self.method = 'getRoot'
    try:
      self.taskNum = taskNum
      if self.rootName is None:
        dbKey = '%s|XFORM|rootname' % self.tsXref
        self.rootName = self._leveldb.Get(dbKey)
      return self._get(self.rootName, None)
    except JsonMemberError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  def _get(self, nodeName, parent):
    self.method = '_get'
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
      memberObj = klass(self._leveldb, parent)
      memberObj.applyMeta(self.tsXref, self.taskNum, xformMeta)
      return memberObj
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

  # -------------------------------------------------------------- #
  # getMembers
  # ---------------------------------------------------------------#
  def getMembers(self, parent):
    with self.lock:
      return self._getMembers(parent)

  # -------------------------------------------------------------- #
  # getMembers
  # ---------------------------------------------------------------#
  def _getMembers(self, parent):
    self.method = '_getMembers'
    try:
      memberList = []
      for nodeName in parent.children:
        logger.info('%s member : %s ' % (parent.name, nodeName))
        memberList.append(self._get(nodeName, parent))
      return memberList
    except JsonMemberError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

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
class JsonMember(Exception):
  __metaclass__ = ABCMeta
  lock = None
  factory = None

  def __init__(self, leveldb, parent=None):
    self._leveldb = leveldb
    self.parent = parent
    self.isRoot = False
    self.method = '__init__'

	#------------------------------------------------------------------#
	# getRootMember
	#------------------------------------------------------------------#
  @staticmethod
  def getRoot(taskNum):
    return JsonMember.factory.getRoot(taskNum)

	#------------------------------------------------------------------#
	# reset
	#------------------------------------------------------------------#
  @staticmethod
  def reset():
    with JsonMember.lock:
      JsonMember.factory = None

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  @staticmethod
  def start(leveldb, tsXref):
    if JsonMember.lock is None:
      JsonMember.lock = RLock()
    if JsonMember.factory is None:    
      JsonMember.factory = MemberFactory(leveldb, tsXref)
    logger.info('### factory tsXref : %s' % JsonMember.factory.tsXref)

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
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, tsXref, taskNum, xformMeta):
    self.method = 'applyMeta'
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
      raise JsonMemberError(ex)

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
    except JsonMemberError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

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
    except JsonMemberError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

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
      raise JsonMemberError(errmsg)
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
        raise JsonMemberError(errmsg)
      record = list(csvObject.keys())
      dbKey = self.name + '|header'
      self.putRecord(dbKey, record)
    except JsonMemberError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

	#------------------------------------------------------------------#
	# putCsvObject
	#------------------------------------------------------------------#
  def putCsvObject(self, csvObject):
    self.method = 'putCsvObject'
    try:
      # self.rowIndex.incCount = klass.incCount
      rowIndex = self.rowIndex.incCount()
      record = list(csvObject.values())
      dbKey = '%s|%s' % (self.name, rowIndex)
      self.putRecord(dbKey, record)
    except JsonMemberError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

	#------------------------------------------------------------------#
	# getRecord
	#------------------------------------------------------------------#
  def getRecord(self, dbKey, restore=True):
    self.method = 'getRecord'
    dbKey = '%s|%s' % (self.tsXref, dbKey)
    #logger.debug('### dbKey : ' + dbKey)
    try:
      record = self._leveldb.Get(dbKey)
      if restore:
        return json.loads(record)
      return record
    except KeyError:
      return None
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

	#------------------------------------------------------------------#
	# append
	#------------------------------------------------------------------#
  def append(self, dbKey, value):
    self.method = 'append'
    try:
      record = self.getRecord(dbKey)
      dbKey = '%s|%s' % (self.tsXref, dbKey)    
      if not record:
        self._leveldb.Put(dbKey, json.dumps([value]))
      else:
        record.append(value)
        self._leveldb.Put(dbKey, json.dumps(record))
    except JsonMemberError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

	#------------------------------------------------------------------#
	# putRecord
	#------------------------------------------------------------------#
  def putRecord(self, dbKey, value):
    self.method = 'putRecord'
    try:
      dbKey = '%s|%s' % (self.tsXref, dbKey)
      if isinstance(value,basestring):
        self._leveldb.Put(dbKey, value)
      else:  
        self._leveldb.Put(dbKey, json.dumps(value))
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise JsonMemberError(ex)

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
  def __init__(self, leveldb, parent):
    super(JsonMemberRN1, self).__init__(leveldb)
    self.isRoot = True
    self.errcode = 'NA'

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
      raise JsonMemberError(ex)

#------------------------------------------------------------------#
# JsonMemberUKN1
#------------------------------------------------------------------#
class JsonMemberUKN1(JsonMember):
  '''
  JsonMemberJson, model - Unique Key Node 1
  ''' 
  def __init__(self, leveldb, parent):
    super(JsonMemberUKN1, self).__init__(leveldb, parent)

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
      raise JsonMemberError(ex)

#------------------------------------------------------------------#
# JsonMemberJsonFKN1
#------------------------------------------------------------------#
class JsonMemberFKN1(JsonMember):
  '''
  JsonMemberJson, model - Foreign Key Node 1
  ''' 
  def __init__(self, leveldb, parent):
    super(JsonMemberFKN1, self).__init__(leveldb, parent)

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
