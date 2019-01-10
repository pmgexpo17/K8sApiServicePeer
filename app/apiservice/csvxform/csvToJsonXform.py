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
from apibase import logger
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
import csv
import os, sys, time
import simplejson as json

# -------------------------------------------------------------- #
# NormaliserFactory
# ---------------------------------------------------------------#
class NormaliserFactory(object):

  def __init__(self, leveldb, caller):
    self._leveldb = leveldb
    self.caller = caller

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self, tsXref, taskNum):
    self.method = 'csvToJsonXform.NormaliserFactory.__call__'
    try:
      dbKey = '%s|XFORM|META|%d' % (tsXref, taskNum)
      jsData = self._leveldb.Get(dbKey)
      xformMeta = json.loads(jsData)
      logger.info('csvToJsonXform.NormaliserFactory - name, classTag : %s, %s ' 
          % (xformMeta['tableName'], xformMeta['classTag']))
      try:
        className = 'Normalise' + xformMeta['classTag']
        klass = getattr(sys.modules[__name__], className)
      except AttributeError:
        errmsg = 'xformMeta class %s does not exist in %s' % (className, __name__)
        self.sendMail('system error',errmsg)
        raise
      dbKey = '%s|REPO|workspace' % tsXref
      workSpace = self._leveldb.Get(dbKey)
      csvFileName = '%s/%s.csv' % (workSpace, xformMeta['tableName'])
      if not os.path.exists(csvFileName):
        errmsg = 'csvFile %s does not exist in workspace %s' \
                          % (xformMeta['tableName'], workSpace)
        self.sendMail('system error',errmsg)
        raise Exception(errmsg)
      normalObj = klass(self._leveldb)
      normalObj.applyMeta(tsXref, xformMeta)
      normalObj.normalise(csvFileName)
    except Exception as ex:
      self.sendMail('system error',str(ex))
      raise

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):
    XformEmailPrvdr.sendMail('CsvToJson','ERR1',self.method,*args)

#------------------------------------------------------------------#
# Normaliser
#------------------------------------------------------------------#
class Normaliser(object):
  __metaclass__ = ABCMeta

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.isRoot = False

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
    try:
      with open(csvFilePath) as csvfh:
        csvReader = csv.reader(csvfh,quotechar='"', 
                                    doublequote=False, escapechar='\\')
        keys = next(csvReader)
        dbKey = '%s|%s|columns' % (self.tsXref, self.name)
        self._leveldb.Put(dbKey, json.dumps(keys))
        self.recnum = 0
        for values in csvReader:					
          self.recnum += 1
          self.putObjects(keys, values)
        if self.isRoot:
          dbKey = '%s|%s|rowcount' % (self.tsXref, self.name)
          self._leveldb.Put(dbKey, str(self.recnum))
        logger.info('#### %s rowcount : %d' % (self.name, self.recnum))
    except csv.Error as exc:
      errmsg = 'csv reader error, file: %s, line: %d, %s' \
                              % (self.name, csvReader.line_num, str(exc))
      print('@normalize, %s' % errmsg)
      raise Exception(errmsg)

	#------------------------------------------------------------------#
	# putObjects
	#------------------------------------------------------------------#
  def putObjects(self, keys, values):
    try:
      self._putObjects(keys, values)
    except KeyError as ex:
      print('KEYS : ' + str(keys))
      print('@putCsvRecord, ' + str(ex))
      raise

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
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, tsXref, xformMeta):
    self.tsXref = tsXref 
    self.name = xformMeta['nodeName']
    self.ukey = xformMeta['ukey']
    self.hasParent = xformMeta['parent'] is not None
    self.fkey = xformMeta['parent']['fkey'] if self.hasParent else None

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
	# getRecord
	#------------------------------------------------------------------#
  def getRecord(self, dbKey, restore=True):
    dbKey = '%s|%s' % (self.tsXref, dbKey)
    try:
      record = self._leveldb.Get(dbKey)
      if restore:
        return json.loads(record)
      return record
    except KeyError:
      return None

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
# NormaliseRN1
#------------------------------------------------------------------#
class NormaliseRN1(Normaliser):
  '''
  Normaliser, model - RootNode1
  ''' 
  def __init__(self, leveldb):
    super(NormaliseRN1, self).__init__(leveldb)
    self.isRoot = True

	#------------------------------------------------------------------#
	# _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    recordD = dict(zip(keys,values))
    ukvalue = self.getUkValue(recordD)
    recordL = list(zip(keys,values))
    self.putRecord(ukvalue, recordL)
    dbKey = '%s|%05d' % (self.name, self.recnum)
    self.putRecord(dbKey, ukvalue)

#------------------------------------------------------------------#
# NormaliseUKN1
#------------------------------------------------------------------#
class NormaliseUKN1(Normaliser):
  '''
  Normaliser, model - Unique Key Node 1
  ''' 
  def __init__(self, leveldb):
    super(NormaliseUKN1, self).__init__(leveldb)

	#------------------------------------------------------------------#
	# _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    recordD = dict(zip(keys,values))
    if self.ukey:
      ukvalue = self.getUkValue(recordD)
      recordL = list(zip(keys,values))
      self.putRecord(ukvalue, recordL)
      if self.hasParent:
        fkvalue = self.getFkValue(recordD)
        dbKey = '%s|%s' % (self.name,fkvalue)
        # store ukey value by fkey for retrieving the full record by ukey at compile time
        self.append(dbKey, ukvalue)

#------------------------------------------------------------------#
# NormaliseFKN1
#------------------------------------------------------------------#
class NormaliseFKN1(Normaliser):
  '''
  Normaliser, model - Foreign Key Node 1
  ''' 
  def __init__(self, leveldb):
    super(NormaliseFKN1, self).__init__(leveldb)

	#------------------------------------------------------------------#
	# _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    recordD = dict(zip(keys,values))
    fkvalue = self.getFkValue(recordD)
    dbKey = '%s|%s' % (self.name,fkvalue)
    recordL = list(zip(keys,values))
    self.append(dbKey, recordL)

# -------------------------------------------------------------- #
# CompilerFactory
# ---------------------------------------------------------------#
class CompilerFactory(object):
  
  def __init__(self, leveldb, tsXref):
    self._leveldb = leveldb
    self.tsXref = tsXref

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  def get(self, nodeName, parent):
    self.method = 'get'
    #logger.info('### hardhash dbPath : ' + tsXref)
    #db = HardHash.connect(tsXref)
    dbKey = '%s|XFORM|META|%s' % (self.tsXref, nodeName)
    #xformMeta = db[dbKey]
    metaData = self._leveldb.Get(dbKey)
    xformMeta = json.loads(metaData)
    logger.info('csvToJsonXform.CompilerFactory - name, classTag : %s, %s ' 
        % (xformMeta['nodeName'], xformMeta['classTag']))
    try:
      className = 'Compile' + xformMeta['classTag']
      klass = getattr(sys.modules[__name__], className)
    except AttributeError:
      errmsg = 'xformMeta class %s does not exist in %s' % (className, __name__)
      self.newMail('system error',errmsg)
      raise
    obj = klass(self._leveldb, parent)
    obj.applyMeta(self.tsXref, xformMeta)
    return obj

  # -------------------------------------------------------------- #
  # getMembers
  # ---------------------------------------------------------------#
  def getMembers(self, parent):
    self.method = 'getMembers'
    memberList = []
    for nodeName in parent.children:
      logger.info('%s member : %s ' % (parent.name, nodeName))
      memberList.append(self.get(nodeName, parent))
    return memberList

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    method = '%s.%s:%s' \
      % (self.__class__.__module__, self.__class__.__name__, self.method)
    XformEmailPrvdr.newMail('csvToJson',bodyKey,method,*args)

#------------------------------------------------------------------#
# Compiler
#------------------------------------------------------------------#
class Compiler(object):
  __metaclass__ = ABCMeta
  factory = None

  def __init__(self, leveldb, parent=None):
    self._leveldb = leveldb
    self.parent = parent
    self.isRoot = False
    self.ukeys = []
    self.fkeyMap = {}
    self.jsObject = {}

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  @staticmethod
  def start(leveldb, tsXref):
    Compiler.factory = CompilerFactory(leveldb, tsXref)

	#------------------------------------------------------------------#
	# getRootMember
	#------------------------------------------------------------------#
  @staticmethod
  def getRoot(rootName):
    return Compiler.factory.get(rootName, None)

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  @abstractmethod
  def compile(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # evalJsObject
  # -------------------------------------------------------------- #
  @abstractmethod
  def evalJsObject(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, tsXref, xformMeta):
    self.tsXref = tsXref
    self.name = xformMeta['nodeName']
    self.ukeyName = '|'.join(xformMeta['ukey']) if xformMeta['ukey'] else None
    self.fkeyName = '|'.join(xformMeta['parent']['fkey']) if not self.isRoot else None
    self.ukeyType = xformMeta['parent']['ukeyType'] if not self.isRoot else None
    self.nullPolicy = xformMeta['nullPolicy']
    self.isLeafNode = xformMeta['children'] is None
    self.children = xformMeta['children']

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
	# build
	#------------------------------------------------------------------#
  def build(self):
    for ukey in self.parent.ukeys:
      jsObject = self.parent.jsObject[ukey]
      if jsObject: # test if parent obj != {}
        jsObject[self.name] = self.getJsObject(ukey)
        self.parent.jsObject[ukey] = jsObject

	#------------------------------------------------------------------#
	# getEmptyObj
	#------------------------------------------------------------------#
  def getEmptyObj(self):
    if self.nullPolicy['IncEmptyObj']:
      dbKey = '%s|columns' % self.name
      columns = self.getRecord(dbKey)
      return OrderedDict([(colname, "") for colname in columns])
    return None

	#------------------------------------------------------------------#
	# getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, fkey):
    if fkey is None:
      if self.ukeyName:
        return {}
      return []
    return self.evalJsObject(fkey)

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
	# putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self):
    pass

#------------------------------------------------------------------#
# CompileRN1
#------------------------------------------------------------------#
class CompileRN1(Compiler):
  '''
  CompileJson, model - RootNode1
  ''' 
  def __init__(self, leveldb, parent):
    super(CompileRN1, self).__init__(leveldb)
    self.isRoot = True

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  def compile(self, dbKey, rootUkey):
    self.rownum = dbKey.split('|')[-1]
    self.ukeys = []
    self.fkeyMap = {}
    self.jsObject = {}
    self.fkeyMap[rootUkey] = [rootUkey]
    self.ukeys = [rootUkey]
    record = self.getRecord(rootUkey)
    self.jsObject[rootUkey] = OrderedDict(record)

	#------------------------------------------------------------------#
	# getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, fkey):
    return self.jsObject[fkey]

	#------------------------------------------------------------------#
	# evalJsObject
	#------------------------------------------------------------------#
  def evalJsObject(self, fkey):
    pass

	#------------------------------------------------------------------#
	# putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self, rootUkey):
    # special case : json object build is complete, now put it to db
    jsObject = {}
    jsObject[self.name] = self.jsObject[rootUkey]
    dbKey = '%s|%s' % (self.tsXref, self.rownum)
    self._leveldb.Put(dbKey, json.dumps(jsObject))

#------------------------------------------------------------------#
# CompileUKN1
#------------------------------------------------------------------#
class CompileUKN1(Compiler):
  '''
  CompileJson, model - Unique Key Node 1
  ''' 
  def __init__(self, xformMeta, parent):
    super(CompileUKN1, self).__init__(xformMeta, parent)

	#------------------------------------------------------------------#
	# compile
	#------------------------------------------------------------------#
  def compile(self):
    self.ukeys = []
    self.fkeyMap = {}
    self.jsObject = {}
    for fkey in self.parent.ukeys:
      if fkey is None:
        return
      dbKey = '%s|%s' % (self.name,fkey)
      fkRecord = self.getRecord(dbKey)
      if not fkRecord:
        # 0 child objects exist 
        self.fkeyMap[fkey] = [None]
        self.ukeys = [None]
        self.jsObject[None] = self.getEmptyObj()
        continue
      self.fkeyMap[fkey] = []
      for jsData in fkRecord:
        ukey = jsData
        self.fkeyMap[fkey] += [ukey]
        self.ukeys += [ukey]        
        self.jsObject[ukey] = OrderedDict(self.getRecord(ukey))

	#------------------------------------------------------------------#
	# evalJsObject
	#------------------------------------------------------------------#
  def evalJsObject(self, fkey):
    jsObject = []
    ukeys = self.fkeyMap[fkey]
    for ukey in ukeys:
      jsObject += [self.jsObject[ukey]]
    if len(jsObject) == 1:
      # don't include null objects, ie, when IncEmptyObj is false
      if jsObject == [None]:
        if self.ukeyType == 'OneToOne':
          return {}
        return []
      elif self.ukeyType == 'OneToOne':
        return jsObject[0]
    return jsObject

#------------------------------------------------------------------#
# CompileJsonFKN1
#------------------------------------------------------------------#
class CompileFKN1(Compiler):
  '''
  CompileJson, model - Foreign Key Node 1
  ''' 
  def __init__(self, xformMeta, parent):
    super(CompileFKN1, self).__init__(xformMeta, parent)

	#------------------------------------------------------------------#
	# compile
	#------------------------------------------------------------------#
  def compile(self):
    self.ukeys = []
    self.fkeyMap = {}
    self.jsObject = {}
    for fkey in self.parent.ukeys:
      if fkey is None:
        return
      dbKey = '%s|%s' % (self.name,fkey)
      fkRecord = self.getRecord(dbKey)
      if not fkRecord:
        # 0 child objects exist 
        self.fkeyMap[fkey] = fkey
        self.jsObject[fkey] = [self.getEmptyObj()]
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

	#------------------------------------------------------------------#
	# evalJsObject
	#------------------------------------------------------------------#
  def evalJsObject(self, fkey):
    fkey = self.fkeyMap[fkey]
    jsObject = self.jsObject[fkey]
    if jsObject == [None]: # ensure an empty list when IncEmptyObj is false
      return []
    return jsObject
