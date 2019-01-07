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
from flask import Response
import json
import logging
import leveldb
import os

logger = logging.getLogger('apscheduler')

# -------------------------------------------------------------- #
# PurgeDataSet
# ---------------------------------------------------------------#
class PurgeDataSet(object):

  def __init__(self, leveldb, *argv):
    self._leveldb = leveldb

  def __call__(self, startKey, endKey):
    logger.info('!! Delete All Keys : %s, %s !!' % (startKey, endKey))
    batch = leveldb.WriteBatch()
    keyIter = self._leveldb.RangeIter(startKey, endKey, include_value=False)
    try:
      while True:
        key = keyIter.next()
        batch.Delete(key)
    except StopIteration:
      self._leveldb.Write(batch, sync=True)
    

# -------------------------------------------------------------- #
# DlmrStreamPrvdr
# ---------------------------------------------------------------#
class DlmrStreamPrvdr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  def evalStream(self, dlm, itemIter):
    hasNext = True
    try:
      key = None
      while hasNext:
        key, item = itemIter.next()
        row = dlm.join(json.loads(item))
        yield row + '\n'
    except StopIteration:
      if key is None:
        raise Exception('range query returned an empty result set')

	# -------------------------------------------------------------- #
	# render a stream generator
	# ---------------------------------------------------------------#
  def __call__(self, dlm, startKey, endKey):
    logger.info('!! Render Stream : %s, %s !!' % (startKey, endKey))
    itemIter = self._leveldb.RangeIter(startKey, endKey)
    try:
     dstream = self.evalStream(dlm, itemIter)
    except Exception as ex:
      jsonTxt = '{"status":400,"error":"%s"}' % str(ex)
      return Response(jsonTxt, status=400, mimetype='text/html')
    return Response(dstream, status=201, mimetype='text/html')

# -------------------------------------------------------------- #
# TxtStreamPrvdr
# ---------------------------------------------------------------#
class TxtStreamPrvdr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  def evalStream(self, itemIter):
    hasNext = True
    try:
      key = None
      while hasNext:
        key, item = itemIter.next()
        yield item + '\n'
    except StopIteration:
      if key is None:
        raise Exception('range query returned an empty result set')

	# -------------------------------------------------------------- #
	# render a stream generator
	# ---------------------------------------------------------------#
  def __call__(self, startKey, endKey):
    logger.info('!! Render Stream : %s, %s !!' % (startKey, endKey))
    itemIter = self._leveldb.RangeIter(startKey, endKey)
    try:
     dstream = self.evalStream(itemIter)
    except Exception as ex:
      jsonTxt = '{"status":400,"error":"%s"}' % str(ex)
      return Response(jsonTxt, status=400, mimetype='text/html')
    return Response(dstream, status=201, mimetype='text/html')

# -------------------------------------------------------------- #
# TxtFileStreamPrvdr
# ---------------------------------------------------------------#
class TxtFileStreamPrvdr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  def evalStream(self, txtFilePath):

    with open(txtFilePath, 'r') as fhr:
      for line in fhr:
        yield line

	# -------------------------------------------------------------- #
	# render a text file stream generator
	# ---------------------------------------------------------------#
  def __call__(self, tsXref, txtFileName):
    logger.info('!! Render Text File Stream, tsXref : %s !!' % tsXref)
    try:
      dbKey = '%s|REPO|workspace' % tsXref
      self.workSpace = self._leveldb.Get(dbKey)
    except KeyError:
      jsonTxt = '{"status":400,"error":"db timestamp key failed to return a value"}'
      return Response(jsonTxt, status=400, mimetype='text/html')

    txtFilePath = '%s/%s' % (self.workSpace, txtFileName)
    if not os.path.exists(txtFilePath):
      jsonTxt = '{"status":400,"error":"file does not exist : %s"}' % txtFilePath
      return Response(jsonTxt, status=400, mimetype='text/html')

    try:
     dstream = self.evalStream(txtFilePath)
    except Exception as ex:
      jsonTxt = '{"status":500,"error":"%s"}' % str(ex)
      return Response(jsonTxt, status=500, mimetype='text/html')
    return Response(dstream, status=201, mimetype='application/octet-stream')

# -------------------------------------------------------------- #
# BinryFileStreamPrvdr
# ---------------------------------------------------------------#
class BinryFileStreamPrvdr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  def evalStream(self, binryFilePath):

    with open(binryFilePath, 'rb') as fhrb:
      while True:        
        chunk = fhrb.read(1024)
        if not chunk:
          break
        yield chunk

	# -------------------------------------------------------------- #
	# render a binary file stream generator
	# ---------------------------------------------------------------#
  def __call__(self, tsXref, binryFileName):
    logger.info('!! Render Binary File Stream, tsXref : %s !!' % tsXref)
    try:
      dbKey = '%s|REPO|workspace' % tsXref
      self.workSpace = self._leveldb.Get(dbKey)
    except KeyError:
      jsonTxt = '{"status":400,"error":"db timestamp key failed to return a value"}'
      return Response(jsonTxt, status=400, mimetype='text/html')

    binryFilePath = '%s/%s' % (self.workSpace, binryFileName)
    if not os.path.exists(binryFilePath):
      jsonTxt = '{"status":400,"error":"file does not exist : %s"}' % binryFilePath
      return Response(jsonTxt, status=400, mimetype='text/html')

    try:
     dstream = self.evalStream(binryFilePath)
    except Exception as ex:
      jsonTxt = '{"status":500,"error":"%s"}' % str(ex)
      return Response(jsonTxt, status=500, mimetype='text/html')
    return Response(dstream, status=201, mimetype='application/octet-stream')
