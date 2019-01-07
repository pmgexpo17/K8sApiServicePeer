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
import os

logger = logging.getLogger('apscheduler')

# -------------------------------------------------------------- #
# SaasEventMngr
# ---------------------------------------------------------------#
class SaasEventMngr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

  def __call__(self, owner, product, category, eventType, itemKey=None):
    dbKey = 'SAAS|%s|%s|%s|%s' % (eventType, owner, product, category)
    logger.info('get saas event contract for key : ' + dbKey)
    try:
      result = self._leveldb.Get(dbKey)
      if itemKey:
        logger.info('### event meta key : ' + itemKey)
        _result = json.loads(result)[itemKey]
        result = json.dumps(_result)
    except KeyError:
      errmsg = {'status':400,'error':'Saas EVENT data not found : ' + dbKey}
      raise Exception(json.dumps(errmsg))
    else:
      logger.info('SaasEventMngr, delivering EVENT info ... ')
      # since the result is already serialized json, send mimetype=text
      return Response(result,status=200, mimetype='text/html')

# -------------------------------------------------------------- #
# SaasRepoMngr
# ---------------------------------------------------------------#
class SaasRepoMngr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

  def __call__(self, owner, product, category, itemKey=None):
    logger.info('get saas repo contract for domain : %s/%s/%s' \
                                          % (owner, product, category))
    dbKey = 'SAAS|REPO|%s|%s|%s' % (owner, product, category)
    try:
      result = self._leveldb.Get(dbKey)
      if itemKey:
        logger.info('### repo meta key : ' + itemKey)
        _result = json.loads(result)[itemKey]
        result = json.dumps(_result)
    except KeyError:
      errmsg = {'status':400,'error':'Saas REPO data not found : ' + dbKey}
      raise Exception(json.dumps(errmsg))
    else:
      logger.info('SaasRepoMngr, delivering REPO info ... ')
      # since the result is already serialized json, send mimetype=text
      return Response(result,status=200, mimetype='text/html')

# -------------------------------------------------------------- #
# SaasXformMngr
# ---------------------------------------------------------------#
class SaasXformMngr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

  def __call__(self, owner, product, category, itemKey=None):
    logger.info('get saas xform contract for domain : %s/%s/%s' \
                                          % (owner, product, category))
    dbKey = 'SAAS|XFORM|%s|%s|%s' % (owner, product, category)
    try:
      result = self._leveldb.Get(dbKey)
      if itemKey:
        logger.info('### xform meta key : ' + itemKey)
        _result = json.loads(result)[itemKey]
        result = json.dumps(_result)
    except KeyError:
      errmsg = {'status':400,'error':'Saas XFORM data not found : ' + dbKey}
      raise Exception(json.dumps(errmsg))
    else:
      logger.info('SaasXformMngr, delivering XFORM info ... ')
      # since the result is already serialized json, send mimetype=text
      return Response(result,status=200, mimetype='text/html')
