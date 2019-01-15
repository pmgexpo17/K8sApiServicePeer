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
from apibase import AppResolvar
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from email.mime.text import MIMEText
from threading import RLock
import logging
import smtplib
import sys, yaml

logger1 = logging.getLogger('apiservice.smart')
logger2 = logging.getLogger('apiservice.async')

# -------------------------------------------------------------- #
# EmailPacket
# ---------------------------------------------------------------#
class EmailPacket(object):
  
  def __init__(self):
    self.attachment = None
    self.mailBody = ''
    self.scope = ''    
    self.subject = ''
    self.multiPart = []

  # ------------------------------------------------------------ #
  # - setAttachment
  # -------------------------------------------------------------#
  def setAttachment(self, bindFile, fileName):
    
    with open(bindFile, "r") as fhr:
      mailPart = MIMEApplication(fhr.read(),Name=fileName)
    mailPart['Content-Disposition'] = 'attachment; filename=' + fileName
    self.attachment = mailPart

  # ------------------------------------------------------------ #
  # - wrap
  # -------------------------------------------------------------#
  def wrap(self, mailBody, bindFile=None,fileName=None):
    self.mailBody = MIMEText(mailBody)
    if bindFile:
      if not fileName:
        fileName = bindFile.split('/')[-1]
      self.setAttachment(bindFile,fileName)
    
# -------------------------------------------------------------- #
# XformEmailPrvdr
# ---------------------------------------------------------------#
class XformEmailPrvdr(AppResolvar):
  _lock = RLock()
  mailer = None
  
  def __init__(self):
    self.mailbox = {}
    self.__dict__['ERR1'] = self.setBodyERR1
    self.__dict__['ERR2'] = self.setBodyERR2
    # add default params for _start error message handling
    self.jobTitle = 'XformEmailPrvdr._start'
    self._to = ['pmg7670@gmail.com']
    self._from = 'pmg7670@hotmail.com'
    self.passwd = '506d672a326e644164616d'
    
    self._params = {}
    # put default error body for unmanaged exceptions
    self.meta = {'ERR1':['','']}
    self.meta['ERR1'][0] = '%s job %s has reported an error'
    self.meta['ERR1'][1] = '''
  Hi csvXform client,

    CsvXform has reported an error
    Error details :
      Source : %s
      Description : %s
      Message : %s

  Regards,
  cirrus cloud apps team
'''
    self.meta = {'EOP1':['','']}
    self.meta['EOP1'][0] = '%s job %s has completed successfully'
    self.meta['EOP1'][1] = '''
  Hi csvXform client,

    CsvXform job %s has completed successfully !

  Regards,
  cirrus cloud apps team
'''

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#  
  def _start(self, mailKey, mailParams):
    self._params[mailKey] = mailParams

  # ------------------------------------------------------------ #
  # - _newMail
  # -------------------------------------------------------------#
  def _newMail(self, mailKey, bodyKey, *args):
    self.packet = self.mailbox[mailKey]
    self.mailbox[mailKey] = self.wrapMail(bodyKey, *args)

  # ------------------------------------------------------------ #
  # - wrapMail
  # -------------------------------------------------------------#
  def wrapMail(self, bodyKey, *args):
    return self[bodyKey](*args)
    
  # ------------------------------------------------------------ #
  # - setBodyERR1
  # -------------------------------------------------------------#
  def setBodyERR1(self, msgScope, desc, message):    
    mailPart = '%s:%s,%s' % (msgScope,desc,message)
    logger1.error(mailPart)
#    self.packet.multiPart.append(mailPart)
#    mailBody = '\n'.join(self.packet.multiPart)
#    self.packet.wrap(mailBody)
    return self.packet

  # ------------------------------------------------------------ #
  # - setBodyERR2
  # -------------------------------------------------------------#
  def setBodyERR2(self, msgScope, desc, message):
    mailPart = '%s:%s,%s' % (msgScope,desc,message)
    logger2.error(mailPart)
#    self.packet.multiPart.append(mailPart)
#    mailBody = '\n'.join(self.packet.multiPart)
#    self.packet.wrap(mailBody)
    return self.packet

  # ------------------------------------------------------------ #
  # - setBodyERR3
  # -------------------------------------------------------------#
  def setBodyERR3(self, msgScope, scriptName, logFile, progLib):

    logger1.error('%s.sas has errored. refer %s' % (scriptName, logFile))
    packet = EmailPacket()
    packet.subject = self.meta['ERR2'][0] \
                    % (self._params['Label'],self._params['JobRef'])
    mailBody = self.meta['ERR2'][1]
    mailBody = mailBody % (scriptName,scriptName,progLib)
    logFile = progLib + '/' + logFile
    packet.wrap(mailBody,bindFile=logFile)
    return packet

  # ------------------------------------------------------------ #
  # - setBodyERR4
  # -------------------------------------------------------------#
  def setBodyERR4(self, msgScope, scriptName):

    logFile = '%s/log/%s.log' % (self.progLib, scriptName)    
    logger1.error('%s.sas has errored. refer %s' % (scriptName, logFile))
    packet = EmailPacket()
    # the email template is exactly the same as ERR2
    packet.subject = self.meta['ERR2'][0] \
                  % (self._params['Label'],self._params['JobRef'])
    mailBody = self.meta['ERR2'][1]
    mailBody = mailBody % (scriptName,scriptName,self.workSpace)
    packet.wrap(mailBody,bindFile=logFile)
    return packet

  # ------------------------------------------------------------ #
  # - setBodyEOP1
  # -------------------------------------------------------------#
  def setBodyEOP1(self, label, jobRef):
    
    packet = EmailPacket()
    packet.subject = self.meta['EOP1'][0] % (label, jobRef)
    mailBody = self.meta['EOP1'][1]
    packet.wrap(mailBody)
    return packet

  # -------------------------------------------------------------- #
  # _sendMail
  # ---------------------------------------------------------------#
  def _sendMail(self, mailKey):
    packet = self.mailbox[mailKey]
    msg = MIMEMultipart()
    msg['Subject'] = packet.subject
    msg['From'] = self._from
    logger1.debug('### mail from : ' + str(msg['From']))
    msg['To'] = COMMASPACE.join(self._to)
    logger1.debug('### mail to : ' + str(msg['To']))
    msg['Date'] = formatdate(localtime=True)
    
    msg.attach(packet.mailBody)
    if packet.attachment:
      msg.attach(packet.attachment)
    
    #server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    #server.ehlo()
    #server.login('pmg7670@gmail.com',self.passwd.decode('hex'))
    #server.sendmail(self._from, self._to, msg.as_string())
    #server.close()
    self.mailbox[mailKey] = EmailPacket()

  # -------------------------------------------------------------- #
  # hasMailReady
  # ---------------------------------------------------------------#  
  @staticmethod
  def hasMailReady(mailKey):
    with XformEmailPrvdr._lock:
      mailer = XformEmailPrvdr.mailer
      if not mailer or mailKey not in mailer.mailbox:
        return False
      return mailer.mailbox[mailKey] is not None

  # ------------------------------------------------------------ #
  # - newMail
  # -------------------------------------------------------------#
  @staticmethod
  def newMail(mailKey, *args):
    with XformEmailPrvdr._lock:
      XformEmailPrvdr.mailer._newMail(mailKey, *args)

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  @staticmethod
  def sendMail(mailKey, *args):
    with XformEmailPrvdr._lock:
      mailer = XformEmailPrvdr.mailer
      if not XformEmailPrvdr.hasMailReady(mailKey):
        mailer._newMail(mailKey,*args)
      mailer._sendMail(mailKey)

  # -------------------------------------------------------------- #
  # subcribe
  # ---------------------------------------------------------------#  
  @staticmethod
  def subscribe(mailKey):
    with XformEmailPrvdr._lock:
      mailer = XformEmailPrvdr.mailer
      if not mailer:
        mailer = XformEmailPrvdr()
        mailer.mailbox[mailKey] = EmailPacket()
        XformEmailPrvdr.mailer = mailer
      elif mailKey not in mailer.mailbox:
        mailer.mailbox[mailKey] = EmailPacket()

  # -------------------------------------------------------------- #
  # init
  # ---------------------------------------------------------------#  
  @staticmethod
  def init(mailKey):
    # TODO : import mail template yaml file before subscribe
    XformEmailPrvdr.subscribe(mailKey)

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#  
  @staticmethod
  def start(mailKey, mailParams):
    #XformEmailPrvdr.mailer._start(mailKey, mailParams)
    pass
