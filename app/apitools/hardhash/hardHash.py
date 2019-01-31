from collections import MutableMapping
from threading import Thread, RLock, Event
import logging
import simplejson as json
import os, sys, time
import zmq

curDir = os.path.dirname(os.path.realpath(__file__)) + '/'
logBase = curDir.split('/app/')[0] + '/app/log'
logger = logging.getLogger('apipeer.hardhash')
logFormat = '%(levelname)s:%(asctime)s %(message)s'
logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
logfile = '%s/hardhash.log' % logBase
fileHandler = logging.FileHandler(logfile)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logFormatter)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

#----------------------------------------------------------------#
# MessageTimeout
#----------------------------------------------------------------#		
class MessageTimeout(Exception):
  pass

#----------------------------------------------------------------#
# Messenger
#----------------------------------------------------------------#		
class Messenger(object):
  def __init__(self):
    self.active = Event()
    self.retry = 300
    self.sock = None

  #----------------------------------------------------------------#
  # apply
  #----------------------------------------------------------------#		
  def apply(self, params):
    self.__dict__.update(params)

  #----------------------------------------------------------------#
  # _make
  #----------------------------------------------------------------#		
  def _make(self, context, sockType, sockAddr, hwm=100):
    self.sock = context.socket(sockType)
    self.sock.set_hwm(hwm)
    self.sock.connect(sockAddr)
    self.active.set()

  #----------------------------------------------------------------#
  # putPacket
  #----------------------------------------------------------------#		
  def putPacket(self, packet):
    for msg in packet[:-1]:
      self.sock.send(str(msg), zmq.SNDMORE)
    self.sock.send(str(packet[-1]))

  #----------------------------------------------------------------#
  # _getPacket
  #----------------------------------------------------------------#		
  def _getPacket(self):
    retry = self.retry
    while self.active.is_set() and retry > 0:
      if self.sock.poll(timeout=100,flags=zmq.POLLIN):
        return self.sock.recv()
      retry -= 1
    if self.active.is_set():
      raise MessageTimeout('failed to get response after approx 30 sec')

#----------------------------------------------------------------#
# LeveldbClient
#----------------------------------------------------------------#		
class LeveldbClient(Messenger):
  def __init__(self, prefix):
    super(LeveldbClient, self).__init__()
    self._prefix = prefix
    self._debug = False
    self.restoreMode = 'PYOBJ'
    self.lock = RLock()

  #----------------------------------------------------------------#
  # make
  # must provide a context to avoid premature garbage collection
  #----------------------------------------------------------------#		
  @staticmethod
  def make(context, routerAddr, prefix=None, **kwargs):
    logger.info('### creating HardHash client, routerAddr : ' + routerAddr)
    client = LeveldbClient(prefix)
    client._make(context, zmq.REQ, routerAddr, **kwargs)
    return client

  #----------------------------------------------------------------#
  # _make
  #----------------------------------------------------------------#		
  def _make(self, context, sockType, sockAddr, hwm=100):    
    self.sock = context.socket(sockType)
    self.sock.set_hwm(hwm)
    self.sock.connect(sockAddr)
    self.active.set()

  #----------------------------------------------------------------#
  # setRestoreMode
  #----------------------------------------------------------------#		
  def setRestoreMode(self, mode):
    self.restoreMode = mode

  #----------------------------------------------------------------#
  # _getPacket
  #----------------------------------------------------------------#		
  def _getPacket(self):
    retry = self.retry
    while self.active.is_set() and retry > 0:
      if self.sock.poll(timeout=100,flags=zmq.POLLIN):
        return self.sock.recv()
      retry -= 1
    if self.active:
      raise MessageTimeout('failed to get response after approx 30 sec')

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    with self.lock:
      if self.active.is_set():
        try:
          logger.info('### closing client socket ...')
          self.active.clear()
          self.sock.setsockopt(zmq.LINGER, 0)
          self.sock.close()
          logger.info('### client socket is closed')
        except zmq.ZMQError as ex:
          logger.error('### hardhash client closure failed : ' + str(ex))

  #----------------------------------------------------------------#
  # MutableMapping methods for emulating a dict
  #----------------------------------------------------------------#		

  #----------------------------------------------------------------#
  # __getitem__
  #----------------------------------------------------------------#		
  def __getitem__(self, key):
    with self.lock:
      try:
        key = '%s%s' % (self._prefix, key) if self._prefix else key
        self.putPacket(['GET',key])
        return self.getPacket('GET')
      except Exception as ex:
        logger.error('GET failed : ' + str(ex))
        return None

  #----------------------------------------------------------------#
  # __setitem__
  #----------------------------------------------------------------#		
  def __setitem__(self, key, value):
    with self.lock:    
      try:
        _value = value if isinstance(value, basestring) else json.dumps(value)
        key = '%s%s' % (self._prefix, key) if self._prefix else key
        self.putPacket(['PUT',key,_value])
        self.getPacket('PUT')
      except Exception as ex:
        logger.error('PUT failed : ' + str(ex))
        raise

  #----------------------------------------------------------------#
  # __delitem__
  #----------------------------------------------------------------#		
  def __delitem__(self, key):
    with self.lock:    
      try:
        key = '%s%s' % (self._prefix, key) if self._prefix else key      
        self.putPacket(['DELETE',key])
        self.getPacket('DELETE')
      except Exception as ex:
        logger.error('DELETE failed : ' + str(ex))
        raise

  #----------------------------------------------------------------#
  # getPacket
  #----------------------------------------------------------------#		
  def getPacket(self, action):
    value = self.restore()
    if value == '|0|':
      self.handleError(action)
    return value

  #----------------------------------------------------------------#
  # restore
  #----------------------------------------------------------------#		
  def restore(self):
    value = self._getPacket()
    try:
      if self.restoreMode == 'JSON':
        return value
      return json.loads(value)
    except ValueError:
      return value

  #----------------------------------------------------------------#
  # getGenerator
  #----------------------------------------------------------------#		
  def getGenerator(self):
    value = self.restore()
    if value == '|0|':
      self.handleError('SELECT')
    yield value    
    while self.sock.getsockopt(zmq.RCVMORE):
      yield self.restore()

  #----------------------------------------------------------------#
  # append
  #----------------------------------------------------------------#		
  def append(self, key, value):
    with self.lock:
      try:
        _value = value if isinstance(value, basestring) else json.dumps(value)
        key = '%s%s' % (self._prefix, key) if self._prefix else key      
        self.putPacket(['APPEND',key,_value])
        self.getPacket('APPEND')
      except Exception as ex:
        logger.error('APPEND failed : ' + str(ex))
        raise

  #----------------------------------------------------------------#
  # select
  # - use with caution - because the related socket is bound to the
  # - output generator, the socket is unusable until generator is complete
  # - This means using select requires creating 1 or more extra clients
  # - to do get/put using selection values.
  #----------------------------------------------------------------#		
  def select(self, keyLow, keyHigh, keysOnly=False):
    with self.lock:
      try:
        keyLow = self._prefix + keyLow if self._prefix else keyLow
        keyHigh = self._prefix + keyHigh if self._prefix else keyHigh      
        logger.info('### SELECT keyLow, keyHigh : %s, %s' % (keyLow, keyHigh))
        self.putPacket(['SELECT',keyLow,keyHigh,int(keysOnly)])
        return self.getGenerator()
      except Exception as ex:
        logger.error('SELECT failed : ' + str(ex))
        raise

  #----------------------------------------------------------------#
  # maxKey
  #----------------------------------------------------------------#		
  def maxKey(self, keyLow, keyHigh):
    with self.lock:
      try:
        keyLow = self._prefix + keyLow if self._prefix else keyLow
        keyHigh = self._prefix + keyHigh if self._prefix else keyHigh      
        logger.info('### MAXKEY keyLow, keyHigh : %s, %s' % (keyLow, keyHigh))
        self.putPacket(['MAXKEY',keyLow,keyHigh])
        return self.getPacket('MAXKEY')
      except Exception as ex:
        logger.error('MAXKEY failed : ' + str(ex))
        raise

  #----------------------------------------------------------------#
  # handleError
  #----------------------------------------------------------------#		
  def handleError(self, action):
    logger.error('!!! %s FAILED' % action)
    if self._debug:
      errmsg = self._getPacket()
      raise Exception('%s failed : %s' % (action, errmsg))
    raise Exception('%s failed' % action)
    
  #----------------------------------------------------------------#
  # redundant methods
  #----------------------------------------------------------------#		

  def __iter__(self):
    raise Exception('Not implemented')

  def __len__(self):
    raise Exception('Not implemented')

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    pass

  def __contains__(self, key):
    raise Exception('Not implemented')
