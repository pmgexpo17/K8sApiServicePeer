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
from apibase import logger
import os
import simplejson as json

# parent module : jsonxform
class XformMetaPrvdr(object):

  # -------------------------------------------------------------- #
  # load
  # ---------------------------------------------------------------#
  def load(self, metaFilePath):

    with open(metaFilePath,'r') as fhr:
      try:
        self.xformMeta = json.load(fhr)
      except ValueError as ex:
        errdesc = 'json load error: ' + str(ex) 
        raise Exception(errdesc)

	#------------------------------------------------------------------#
	# getRootName
	#------------------------------------------------------------------#
  def getRootName(self):
    return self.rootName

	#------------------------------------------------------------------#
	# getCsvFiles
	#------------------------------------------------------------------#
  def getCsvFiles(self):
    return ['%s.csv' % item['tableName'] for key, item in self.xformMeta.items()]

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  def get(self, nodeName=None, attrName=None):
    if not nodeName:
      if attrName:
        return [item[attrName] for key, item in self.xformMeta.items()]
      return self.xformMeta.keys()
    if nodeName == 'root':
      return self.get(self.rootName, attrName)
    metaItem = self.xformMeta[nodeName]
    if attrName:
      return metaItem[attrName]
    tableName = metaItem['tableName']
    metaItem['nodeName'] = nodeName
    metaItem['tableName'] = tableName
    metaItem['tableTag'] = self.tableTag[tableName]
    return metaItem

	#------------------------------------------------------------------#
	# validate metafile content
	#------------------------------------------------------------------#
  def validate(self, modeKey):
    try:
      meta = self.xformMeta[modeKey]
      meta['csvDelimiter']
      meta['tableTag']
      jsonDom = meta['jsonDOM']
      rootNode = jsonDom['rootNode']
      subNodes = jsonDom['subNodes']
    except KeyError as ex:
      errmsg = 'Required meta item %s does not exist' % str(ex)
      raise Exception(errmsg)

    # fill jsNodeList to test it matches data['CsvFileMap'].keys()
    try:
      rootName = rootNode['nodeName']
      rootNode['ukey']
      rootNode['parent']
      rootNode['nullPolicy']      
      rootNode['children']
      rootNode['classTag']
    except KeyError as ex:
      errmsg = 'Required meta item %s does not exist in %s' % (str(ex), rootName)
      raise Exception(errmsg)

#    nodeNames = [rootName] + subNodes.keys()
#    _nodeNames = meta['csvFileMap'].keys()
#    if set(nodeNames) != set(_nodeNames):
#      raise Exception('All JsonDOM node names must exist in CsvFileMap keys')

    print('validateSubNodes ...')
    try:
      for nodeName, subNode in subNodes.items():
        print('SubNode : ' + nodeName)
        self.validateSubNode(nodeName, subNode)
    except KeyError as ex:
      errmsg = 'Required meta item %s does not exist in %s' % (str(ex), nodeName)
      raise Exception(errmsg)
    
    print('validateFKeys ...')
    self.xformMeta = subNodes
    self.xformMeta[rootName] = rootNode
    tableName = []
    for nodeName, subNode in subNodes.items():
      tableName.append(subNode['tableName'])
      print('SubNode : ' + nodeName)
      if nodeName != rootName:
        self.validateFKey(nodeName, subNode['parent'])

    tableTag = meta['tableTag'].keys()
    if set(tableName) != set(tableTag):
      raise Exception('For each node, the tableName attrbute must exist in the tableTag key set')

    logger.info('#### %s meta file is valid, root name : %s' % (modeKey, rootName))
    self.rootName = rootName
    self.tableTag = meta['tableTag']

	#------------------------------------------------------------------#
	# validateFkey - validate foreign key items in parent unique key
	#------------------------------------------------------------------#
  def validateFKey(self, nodeName, parentItem):
    parentName = parentItem['nodeName']
    parentNode = self.xformMeta[parentName]
    if set(parentItem['fkey']) != set(parentNode['ukey']):
      errmsg = '%s FKey does not match parent UKey' % nodeName
      raise Exception(errmsg)

	#------------------------------------------------------------------#
	# validateSubNode - validate JsonDOM elements
	#------------------------------------------------------------------#
  def validateSubNode(self, nodeName, metaNode):

    errtxt = 'item'
    # test requried keys exist
    metaNode['tableName']
    metaNode['nullPolicy']
    metaNode['classTag']
    for metaKey in ['ukey','parent','children']:
      metaItem = metaNode[metaKey]
      if metaKey == 'parent':
        metaItem['nodeName']
        metaItem['ukeyType']
        metaItem = metaItem['fkey']
        errtxt = 'FKey item'
      if metaItem and not isinstance(metaItem, list):
        errmsg = 'metafile list value expected for %s %s in %s' % (metaKey, errtxt, nodeName)
        raise Exception(errmsg)

