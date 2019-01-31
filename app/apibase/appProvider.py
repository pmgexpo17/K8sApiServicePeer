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
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from jobstore import LeveldbJobStore
from pytz import utc
from threading import RLock
import simplejson as json
import logging
import leveldb
import os, sys
import uuid

# ---------------------------------------------------------------------------#
# AppProvider
#
# The design intention of a smart job is to enable a group of actors to each
# run a state machine as a subprogram of an integrated super program
# The wikipedia (https://en.wikipedia.org/wiki/Actor_model) software actor 
# description says :
# In response to a message that it receives, an actor can : make local decisions, 
# create more actors, send more messages, and determine how to respond to the 
# next message received. Actors may modify their own private state, but can only 
# affect each other through messages (avoiding the need for any locks).
# ---------------------------------------------------------------------------#    
class AppProvider(object):
  _singleton = None
  _lock = RLock()
  
  @staticmethod
  def connect(dbPath=None):
    with AppProvider._lock:
      if AppProvider._singleton is not None:
        return AppProvider._singleton
      if not dbPath:
        raise Exception("dbPath must be defined for AppProvider startup")
      return AppProvider.start(dbPath)
  
  @staticmethod
  def start(dbPath):
    global logger
    logger = logging.getLogger('apiservice.smart')
    
    logger.info('### AppProvider is starting ... ###')
    if not os.path.isdir(dbPath):
      raise Exception("dbPath is not a directory : " + dbPath)
    appPrvdr = AppProvider()
    _leveldb = leveldb.LevelDB(dbPath)
    appPrvdr.db = _leveldb
    jobstore = LeveldbJobStore(_leveldb)
    jobstores = { 'default': jobstore } 
    scheduler = BackgroundScheduler(jobstores=jobstores,timezone=utc)
    scheduler.start()
    appPrvdr.scheduler = scheduler
    appPrvdr.registry = ServiceRegister()
    appPrvdr._job = {}
    AppProvider._singleton = appPrvdr
    return AppProvider._singleton
    
  def __init__(self):
    self.lock = RLock()
    
  # -------------------------------------------------------------- #
  # addActorGroup
  # ---------------------------------------------------------------#
  def addActorGroup(self, params, jobRange):

    director = self._job[params.id]
    module, className = self.registry.getClassName(params.service)

    jobs = director.listener.register(jobRange)
    params.args.append(0)
    for jobNum in jobRange:
      actorId = jobs.pop(0)
      # leveldb, actorId constructor params are fixed by protocol
      delegate = getattr(module, className)(self.db, actorId, params.id)
      # ensure delegate._type attribute is set
      delegate._type = 'delegate'
      self._job[actorId] = delegate
      params.args[-1] = jobNum
      self.runActor(params,actorId)
      # append the job to remake the original list
      jobs.append(actorId)
    return jobs

  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  def runActor(self, params, actorId):

    logger.info('appProvider.runActor job args : ' + str(params.args))
    args = [actorId] + params.args
    self.scheduler.add_job('apibase:dispatch',id=actorId,args=args,kwargs=params.kwargs,misfire_grace_time=3600)
    return actorId

  # -------------------------------------------------------------- #
  # addActor
  # ---------------------------------------------------------------#
  def addActor(self, params, actorId):

    module, className = self.registry.getClassName(params.service)
    # must be an AppDirector derivative, leveldb and actorId params are fixed by protocol
    if params.caller:
      delegate = getattr(module, className)(self.db, actorId, params.caller)
    else:
      delegate = getattr(module, className)(self.db, actorId)
    if hasattr(params, 'listener'):
      # must be an AppListener derivative, leveldb param is fixed by protocol
      module, className = self.registry.getClassName(params.listener)
      if params.callerHost:
        listener = getattr(module, className)(self.db, actorId, params.callerHost)
      else:
        listener = getattr(module, className)(self.db, actorId)
      listener.state = delegate.state
      delegate.listener = listener
      self.scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    self._job[actorId] = delegate
    self.runActor(params,actorId)
    return actorId

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, _params, actorId=None, jobRange=None):

    params = Params(_params)
    with self.lock:
      try:
        params.id
      except AttributeError:
        raise Exception("required param 'id' not found")
      logger.info('job service, id : %s, %s' % (params.service, params.id))
      if params.id:
        try:
          self._job[params.id]
        except KeyError:
          raise Exception('actorId not found in job register : ' + params.id)
        if params.type == 'delegate':
          # a live director is delegating an actor group
          if '-' in jobRange:
            a,b = list(map(int,jobRange.split('-')))
            _range = range(a,b)
          else:
            b = int(jobRange) + 1
            _range = range(1,b)
          return self.addActorGroup(params, _range)
        else:
          # a live director program is promoted, ie, state machine is promoted
          return self.runActor(params, params.id)
      elif not actorId:
        actorId = str(uuid.uuid4())
        logger.info('new job id : ' + actorId)        
      # a new program, either a sync director or async delegate
      return self.addActor(params, actorId)

  # -------------------------------------------------------------- #
  # resolve
  # ---------------------------------------------------------------#
  def resolve(self, _params):
    
    params = Params(_params)
    module, className = self.registry.getClassName(params.service)    
    actor = getattr(module, className)(self.db)
    with self.lock:
      try:
        if params.kwargs:
          return actor(*params.args, **params.kwargs)
        return actor(*params.args)
      except Exception as ex:
        logger.error('sync process failed : ' + str(ex))
        raise

  # -------------------------------------------------------------- #
  # evalComplete
  # ---------------------------------------------------------------#
  def evalComplete(self, actor, actorId):    
    with self.lock:
      try:
        actor.state
      except AttributeError:
        # only stateful jobs are retained
        del(self._job[actorId])
      else:
        if not actor.state.complete:
          return
        logMsg = '### %s director %s is complete, removing it now ...'
        if actor.state.failed:
          logMsg = '### %s director %s has failed, removing it now ...'
        logger.info(logMsg, actor.__class__.__name__,actorId)
        if actor._type == 'director':
          self.removeMeta(actorId)
        if hasattr(actor, 'listener'):
          self.scheduler.remove_listener(actor.listener)
        del(self._job[actorId])

  # -------------------------------------------------------------- #
  # handleError
  # ---------------------------------------------------------------#
  def handleError(self, delegate, ex):
    with self.lock:
      try:
        caller = self._job[delegate.caller]
        caller.listener
      except (AttributeError, KeyError):
        pass
      else:
        logMsg = 'delegate %s failed : %s' % (delegate.__class__.__name__, str(ex))
        logger.error(logMsg)
        raise Exception(ex)

  # -------------------------------------------------------------- #
  # removeMeta
  # ---------------------------------------------------------------#
  def removeMeta(self, actorId):
    dbKey = 'PMETA|' + actorId
    self.db.Delete(dbKey)

  # -------------------------------------------------------------- #
  # getLoadStatus
  # ---------------------------------------------------------------#
  def getLoadStatus(self, serviceName):
    with self.lock:
      if self.registry.isLoaded(serviceName):
        return {'status':200,'loaded':True}
      return {'status':200,'loaded':False}

  # -------------------------------------------------------------- #
  # loadService
  # ---------------------------------------------------------------#
  def loadService(self, serviceName, serviceRef):
    with self.lock:
      self.registry.loadModules(serviceName, serviceRef)

  # -------------------------------------------------------------- #
  # reloadModule
  # ---------------------------------------------------------------#
  def reloadModule(self, serviceName, moduleName):
    with self.lock:
      return self.registry.reloadModule(serviceName, moduleName)

# -------------------------------------------------------------- #
# ServiceRegister
# ---------------------------------------------------------------#
class ServiceRegister(object):
	
  def __init__(self):
    self._modules = {}
    self._services = {}
    
  # ------------------------------------------------------------ #
  # isLoaded
  # -------------------------------------------------------------#
  def isLoaded(self, serviceName):
    try:
      self._services[serviceName]
    except KeyError:
      return False
    else:
      return True

  # ------------------------------------------------------------ #
  # loadModules
  # -------------------------------------------------------------#
  def loadModules(self, serviceName, serviceRef):
    self._services[serviceName] = {}
    for module in serviceRef:
      self.loadModule(serviceName, module['name'], module['fromList'])
	
  # ------------------------------------------------------------ #
  # _loadModule : wrap load module
  # -------------------------------------------------------------#
  def loadModule(self, serviceName, moduleName, fromList):
    self._services[serviceName][moduleName] = fromList
    try:
      self._modules[moduleName]
    except KeyError:
      self._loadModule(moduleName, fromList)
		
  # ------------------------------------------------------------ #
  # _loadModule : execute load module
  # -------------------------------------------------------------#
  def _loadModule(self, moduleName, fromList):    
    # reduce moduleName to the related fileName for storage
    _module = '.'.join(moduleName.split('.')[-2:])
    logger.info('%s is loaded as : %s' % (moduleName, _module))
    self._modules[_module] = __import__(moduleName, fromlist=[fromList])

  # ------------------------------------------------------------ #
  # reloadHelpModule
  # -------------------------------------------------------------#
  def reloadHelpModule(self, serviceName, helpModule):
    try:
      serviceRef = self._services[serviceName]
    except KeyError as ex:
      return ({'status':400,'errdesc':'KeyError','error':str(ex)}, 400)
    for moduleName in serviceRef:
      #_module = moduleName.split('.')[-1]
      _module = '.'.join(moduleName.split('.')[-2:])
      self._modules[_module] = None
    if helpModule not in sys.modules:
      warnmsg = '### support module %s does not exist in sys.modules'
      logger.warn(warnmsg % helpModule)
    else:
      reload(sys.modules[helpModule])
    for moduleName, fromList in serviceRef.items():
      self._loadModule(moduleName, fromList)
    logger.info('support module is reloaded : ' + helpModule)
    return {'status':201,'service':serviceName,'module':helpModule}
	
  # ------------------------------------------------------------ #
  # reloadModule
  # -------------------------------------------------------------#
  def reloadModule(self, serviceName, moduleName):
    try:
      serviceRef = self._services[serviceName]
      fromList = serviceRef[moduleName]
    except KeyError:
      return self.reloadHelpModule(serviceName,moduleName)

    # reduce moduleName to the related fileName for storage
    #_module = moduleName.split('.')[-1]
    _module = '.'.join(moduleName.split('.')[-2:])
    self._modules[_module] = None
    reload(sys.modules[moduleName])    
    logger.info('%s is reloaded as : %s' % (moduleName, _module))
    self._modules[_module] = __import__(moduleName, fromlist=[fromList])
    return {'status':201,'service':serviceName,'module':moduleName}
	
  # ------------------------------------------------------------ #
  # getClassName
  # -------------------------------------------------------------#
  def getClassName(self, classRef):

    if ':' not in classRef:
      raise ValueError('Invalid classRef %s, expecting module:className' % classRef)
    moduleName, className = classRef.split(':')

    try:
      module = self._modules[moduleName]
    except KeyError:
      raise Exception('Service module name not found in register : ' + moduleName)

    if not hasattr(module, className):
      raise Exception('Service classname not found in service register : ' + className)
    
    return (module, className)

# -------------------------------------------------------------- #
# Params
# ---------------------------------------------------------------#
class Params(object):
  def __init__(self, params):
    if not hasattr(params,'args'):
      self.args = []
    if not hasattr(params,'kwargs'):
      self.kwargs = None
    if not hasattr(params,'caller'):
      self.caller = None
    try:
      self.__dict__.update(params)
    except:
      raise Exception('params is not a dict')     
