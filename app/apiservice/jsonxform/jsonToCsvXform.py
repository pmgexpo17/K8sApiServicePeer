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
from apibase import logger
from apitools.jsonxform import XformMetaPrvdr
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
from threading import RLock
import csv
import math
import os, sys, time
import simplejson as json

def getFileTag(taskNum):
  tagOrd = int((taskNum-1) / 26)
  tagChr1 = chr(ord('a') + tagOrd)
  tagOrd = int((taskNum-1) % 26)
  tagChr2 = chr(ord('a') + tagOrd)
  return tagChr1 + tagChr2

#------------------------------------------------------------------#
# JsonNormaliser
#------------------------------------------------------------------#
class JsonNormaliser(object):

  def __init__(self, leveldb, caller):
    self._leveldb = leveldb
    self.caller = caller

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
      logger.info('### jsonToCsv workspace : ' + workSpace)
      logger.info('### split file name : ' + inputJsonFile)
      jsonFilePath = workSpace + '/' + inputJsonFile
      if not os.path.exists(jsonFilePath):
        errmsg = 'json split file %s does not exist in repo %s' \
                                          % (inputJsonFile, workSpace)
        self.sendMail('system error',errmsg)
        raise Exception(errmsg)

      JsonMember.start(self._leveldb, tsXref)
      with open(jsonFilePath) as jsfh:
        jsonDom = list(self.getJsDomAsQueue(taskNum))      
        recnum = 1
        for jsRecord in jsfh:
          self.normalise(jsRecord, jsonDom)
          recnum += 1
        logger.info('### JsonNormaliser split file %s rowcount : %d' % (fileTag, recnum))
    except Exception as ex:
      self.sendMail('system error',str(ex))
      raise

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):
    XformEmailPrvdr.newMail('JsonToCsv','ERR1',self.method,*args)

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self, taskNum):
    self.method = 'jsonToCsvXform.JsonNormaliser.getJsDomAsQueue'
    with JsonMember.lock:
      logger.info('task %d MemberFactory.getJsDomAsQueue' % taskNum)
      from collections import deque
      self.rootMember = JsonMember.getRoot(taskNum)
      domQueue = deque(self.rootMember.getMembers())
      while domQueue:
        nextMember = domQueue.popleft()
        #logger.info('### Next member : ' + nextMember.name)
        memberList = nextMember.getMembers()
        if memberList:
          domQueue.extendleft(memberList)        
        yield nextMember

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, jsRecord, jsonDom):
    self.method = 'jsonToCsvXform.JsonNormaliser.normalise'
    try:
      jsObject = json.loads(jsRecord, object_pairs_hook=OrderedDict)
    except ValueError as ex:
      raise Exception('Json decode error : ' + str(ex))
    self.rootMember.evalDataset(jsObject)
    for nextMember in jsonDom:
      nextMember.evalDataset()
    self.putDatasets(jsonDom)

	#------------------------------------------------------------------#
	# putDatasets
	#------------------------------------------------------------------#
  def putDatasets(self, jsonDom):
    self.method = 'jsonToCsvXform.JsonNormaliser.normalise'
    #print('putCsvObjects ...')
    hasObjects = True
    while hasObjects:
      hasObjects = self.rootMember.hasDataset
      for nextMember in jsonDom:
        hasObjects = hasObjects or nextMember.hasDataset
        if nextMember.isLeafNode:
          #print('Leaf node found !')
          self.putDsByMember(nextMember)
      self.rootMember.putDataset()
    
  def putDsByMember(self, nextMember):
    self.method = 'jsonToCsvXform.JsonNormaliser.putDsByMember'
    # child objects must be removed and put to db first before 
    # the parent object is put to db, so this means start at the
    # leaf node and traverse back up
    #print('putDatasets ...')
    while not nextMember.isRoot:
      nextMember.putDataset()
      #print('%s has dataset : %s' % (nextMember.name, str(nextMember.hasDataset)))      
      nextMember = nextMember.parent
    #print('%s has dataset : %s' % (nextMember.name, str(nextMember.hasDataset)))      

# -------------------------------------------------------------- #
# MemberFactory
# ---------------------------------------------------------------#
class MemberFactory(object):

  def __init__(self, leveldb, tsXref):
    self._leveldb = leveldb
    self.tsXref = tsXref
    self.lock = RLock()
    self.rootName = None
 
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
    self.taskNum = taskNum
    if self.rootName is None:
      dbKey = '%s|XFORM|rootname' % self.tsXref
      self.rootName = self._leveldb.Get(dbKey)
    return self._get(self.rootName, None)

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  def _get(self, nodeName, parent):
    self.method = 'jsonToCsvXform.MemberFactory._get'
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
      self.sendMail('system error',errmsg)
      raise
    memberObj = klass(self._leveldb, parent)
    memberObj.apply(self.tsXref, self.taskNum, xformMeta)
    return memberObj

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
    self.method = 'jsonToCsvXform.MemberFactory._getMembers'
    memberList = []
    for nodeName in parent.children:
      logger.info('%s member : %s ' % (parent.name, nodeName))
      memberList.append(self._get(nodeName, parent))
    return memberList

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):
    XformEmailPrvdr.sendMail(*args)

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

  # -------------------------------------------------------------- #
  # write
  # ---------------------------------------------------------------#
  @classmethod
  def write(cls, record):
    with cls.lock:
      cls.rowCount += 1
      record = ','.join(record)
      return cls.rowCount

#------------------------------------------------------------------#
# JsonMember
#------------------------------------------------------------------#
class JsonMember(object):
  __metaclass__ = ABCMeta
  lock = None
  factory = None

  def __init__(self, leveldb, parent=None):
    self._leveldb = leveldb
    self.parent = parent
    self.isRoot = False
    self.errcode = 'NA'

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
    logger.info('### factory session var, tsXref : %s' % JsonMember.factory.tsXref)

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
	# apply
	#------------------------------------------------------------------#
  def apply(self, tsXref, taskNum, xformMeta):
    self.tsXref = tsXref
    self.name = xformMeta['nodeName']
    self.ukeyName = '|'.join(xformMeta['ukey']) if xformMeta['ukey'] else None
    self.fkeyName = '|'.join(xformMeta['parent']['fkey']) if not self.isRoot else None
    self.ukeyType = xformMeta['parent']['ukeyType'] if not self.isRoot else None
    self.nullPolicy = xformMeta['nullPolicy']
    self.isLeafNode = xformMeta['children'] is None
    self.children = xformMeta['children']
    className = '%s%02d%s' % ('RowIndex',taskNum,xformMeta['tableTag'])
    try:
      self.rowIndex = getattr(sys.modules[__name__], className)
    except AttributeError:
      rindexClass = type(className,(RowIndex,),{})
      rindexClass.start(taskNum)
      self.rowIndex = rindexClass

	#------------------------------------------------------------------#
	# getMembers
	#------------------------------------------------------------------#
  def getMembers(self):
    if self.isLeafNode:
      return None
    memberList = self.factory.getMembers(self)
    if self.isRoot:  
      return memberList
    memberList.reverse()
    return memberList

	#------------------------------------------------------------------#
	# evalDataset
  # - for resolving headers
	#------------------------------------------------------------------#
  def evalDataset(self, csvDataset=None):
    self.hasDataset = False
    self.csvDataset = csvDataset
    try:
      self.csvDataset = self.getDataset()
      if self.hasDataset:
        self.putHeader()
    except Exception as ex:
      if self.errcode == 'JS_PARSE_01':
        raise
      if self.errcode == 'JS_PARSE_02':
        print(ex)

	#------------------------------------------------------------------#
	# getDataset
	#------------------------------------------------------------------#
  def getDataset(self):
    try:
      csvDataset = self._getDataset()
    except KeyError as ex:
      self.errcode = 'JS_PARSE_01'
      errmsg = '%s json component %s does not exist csv dataset ' \
                                              % (self.name, str(ex))
      raise Exception(errmsg)
    if not csvDataset: # empty list or dict
      self.errcode = 'JS_PARSE_02'
      errmsg = '%s json component is empty' % self.name
      raise Exception(errmsg)
    if not isinstance(csvDataset,list):
      csvDataset = [csvDataset]
    self.hasDataset = len(csvDataset) > 0
    return csvDataset    
    
	#------------------------------------------------------------------#
	# putHeader
	#------------------------------------------------------------------#
  def putHeader(self):
    csvObject = self.csvDataset[0]
    keyName = self.ukeyName if self.ukeyName else self.fkeyName
    keySet = set(keyName.split('|'))
    if not keySet.issubset(csvObject):
      errmsg = '%s key %s does not exist in json record' \
                                          % (self.name, keyName)
      raise Exception(errmsg)
    record = list(csvObject.keys())
    dbKey = self.name + '|header'
    self.putRecord(dbKey, record)

	#------------------------------------------------------------------#
	# putCsvObject
	#------------------------------------------------------------------#
  def putCsvObject(self, csvObject):
    # here self.incCount = klass.incCount
    rowIndex = self.rowIndex.incCount()
    record = list(csvObject.values())
    dbKey = '%s|%s' % (self.name, rowIndex)
    self.putRecord(dbKey, record)

	#------------------------------------------------------------------#
	# getRecord
	#------------------------------------------------------------------#
  def getRecord(self, dbKey, restore=True, verbose=False):
    dbKey = '%s|%s' % (self.tsXref, dbKey)
    if verbose:
      logger.info('### dbKey : ' + dbKey)
    try:
      record = self._leveldb.Get(dbKey)
      if restore:
        return json.loads(record)
      return record
    except KeyError:
      return None

	#------------------------------------------------------------------#
	# append
	#------------------------------------------------------------------#
  def append(self, dbKey, value):
    record = self.getRecord(dbKey)
    dbKey = '%s|%s' % (self.tsXref, dbKey)    
    if not record:
      self._leveldb.Put(dbKey, json.dumps([value]))
    else:
      record.append(value)
      self._leveldb.Put(dbKey, json.dumps(record))

	#------------------------------------------------------------------#
	# putRecord
	#------------------------------------------------------------------#
  def putRecord(self, dbKey, value):
    dbKey = '%s|%s' % (self.tsXref, dbKey)
    if isinstance(value,basestring):
      self._leveldb.Put(dbKey, value)
    else:  
      self._leveldb.Put(dbKey, json.dumps(value))

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
    if self.hasDataset:
      csvObject = self.csvDataset.pop(0)
      self.putCsvObject(csvObject)
      self.hasDataset = len(self.csvDataset) > 0

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
    if self.hasDataset:
      csvObject = self.csvDataset.pop(0)
      self.putCsvObject(csvObject)
      self.hasDataset = len(self.csvDataset) > 0

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
    if self.parent.hasDataset:
      csvDataset = self.parent.csvDataset[0].pop(self.name)
      for csvObject in csvDataset:
        self.putCsvObject(csvObject)
      self.hasDataset = False
