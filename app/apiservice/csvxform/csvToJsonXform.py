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
from apitools.mailer import XformEmailPrvdr
from collections import deque, OrderedDict
import csv
import logging
import os, sys, time
import simplejson as json

logger = logging.getLogger('apiservice.async')

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
      normalObj = klass(self._leveldb)
      normalObj.applyMeta(tsXref, xformMeta)
      normalObj.normalise(csvFilePath)
    # apscheduler will only catch BaseException, EVENT_JOB_ERROR will not fire otherwise      
    except NormaliseError:
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

  def __init__(self, leveldb):
    self._leveldb = leveldb
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
    except NormaliseError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

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
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise NormaliseError(ex)

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
  def get(self, nodeName, parent):
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
      obj = klass(self._leveldb, parent)
      obj.applyMeta(self.tsXref, xformMeta)
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
  factory = None

  def __init__(self, leveldb, parent=None):
    self._leveldb = leveldb
    self.parent = parent
    self.isRoot = False
    self.ukeys = []
    self.fkeyMap = {}
    self.jsObject = {}
    self.method = '__init__'

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
  # getJsObject
  # -------------------------------------------------------------- #
  @abstractmethod
  def getJsObject(self, *args, **kwargs):
    pass

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
        columns = self.getRecord(dbKey)
        return OrderedDict([(colname, "") for colname in columns])
      return {}
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
      raise CompileError(ex)

	#------------------------------------------------------------------#
	# getRecord
	#------------------------------------------------------------------#
  def getRecord(self, dbKey, restore=True):
    self.method = 'getRecord'
    dbKey = '%s|%s' % (self.tsXref, dbKey)
    logger.debug('### dbKey : ' + dbKey)
    try:
      record = self._leveldb.Get(dbKey)
      if restore:
        return json.loads(record)
      return record
    except KeyError:
      return None
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
  def __init__(self, leveldb, parent):
    super(CompileRN1, self).__init__(leveldb)
    self.isRoot = True

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  def compile(self, dbKey, rootUkey):
    self.method = 'compile'
    try:
      self.rownum = dbKey.split('|')[-1]
      self.ukeys = []
      self.fkeyMap = {}
      self.jsObject = {}
      self.fkeyMap[rootUkey] = [rootUkey]
      self.ukeys = [rootUkey]
      record = self.getRecord(rootUkey)
      self.jsObject[rootUkey] = OrderedDict(record)
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
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
	# putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self, rootUkey):
    self.method = 'putJsObject'    
    # special case : json object build is complete, now put it to db
    try:
      jsObject = {}
      jsObject[self.name] = self.jsObject[rootUkey]
      dbKey = '%s|%s' % (self.tsXref, self.rownum)
      self._leveldb.Put(dbKey, json.dumps(jsObject))
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
  def __init__(self, xformMeta, parent):
    super(CompileUKN1, self).__init__(xformMeta, parent)

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
        fkRecord = self.getRecord(dbKey)
        if not fkRecord:
          # 0 child objects exist 
          self.fkeyMap[fkey] = None
          self.jsObject[fkey] = [] if self.subType == 'LIST' else self.getEmptyObj()
          continue
        self.fkeyMap[fkey] = []
        for ukey in fkRecord:
          self.fkeyMap[fkey] += [ukey]
          self.ukeys += [ukey]        
          self.jsObject[ukey] = OrderedDict(self.getRecord(ukey))
    except CompileError:
      raise
    except Exception as ex:
      self.newMail('ERR2','system error',str(ex))
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
  def __init__(self, xformMeta, parent):
    super(CompileFKN1, self).__init__(xformMeta, parent)

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
        fkRecord = self.getRecord(dbKey)
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
      self.newMail('ERR2','system error',str(ex))
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
