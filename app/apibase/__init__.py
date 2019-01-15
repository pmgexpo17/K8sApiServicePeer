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
from appProvider import AppProvider
from apiPeer import ApiPeer as ApiPeer
from appDirector import (AppDirector, AppState, AppResolvar, AppListener,
  MetaReader, SysCmdUnit, logger)

# -------------------------------------------------------------- #
# dispatch
# ---------------------------------------------------------------#
def dispatch(jobId, *argv, **kwargs):

  appPrvdr = AppProvider.connect()
  try:
    delegate = appPrvdr._job[jobId]
  except KeyError:
    logger.error('jobId not found in job register : ' + jobId)
    return
  try:
    delegate(*argv, **kwargs)
  except Exception as ex:
    if delegate._type == 'delegate':
      appPrvdr.handleError(delegate, ex)
  else:
    appPrvdr.evalComplete(delegate, jobId)

