
import smtplib, htmllib, formatter, time

from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.Utils import formatdate

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def extractEmailAddress(address_string):
    left = address_string.find('<')
    if left > -1:
        right = address_string.rfind('>')
        return address_string[left+1:right]
    return address_string

def untagleEmailAddress(address_string):
    if isinstance(address_string, basestring):
        return address_string.split(',')
    elif isinstance(address_string, (list,tuple)):
        return list(address_string)
    else:
        errmsg = "Unsupported type for email address : %s"
        raise TypeError, errmsg % str(type(address_string))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SmtpMailer(object):

    def __init__(self, smtp_host, smtp_port=None):
        self.smtp_server = None
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.start(smtp_host, smtp_port)

    def __del__(self):
        self.stop()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def start(self, smtp_host_=None, smtp_port_=None):
        if self.smtp_server is not None: self.stop()
        if smtp_host_ is not None: self.smtp_host = smtp_host_
        if smtp_port_ is not None: self.smtp_port = smtp_port_

        if self.smtp_port is not None:
            self.smtp_server = smtplib.SMTP(self.smtp_host, self.smtp_port)
        else: self.smtp_server = smtplib.SMTP(self.smtp_host)
        #print 'created smtp_server :', self.smtp_host, self.smtp_server

    def stop(self):
        if self.smtp_server is not None:
            self.smtp_server.quit()
            del self.smtp_server
            self.smtp_server = None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def extractHeader(self, sender, mail_to, subject, **kwargs):
        """ create the message header
        """
        debug = kwargs.get('debug', kwargs.get('test',False))

        header = { }
        # add the header info
        if isinstance(subject, basestring):
            header['Subject'] = subject
            if debug: print "header['subject'] =", subject
        else:
            errmsg = "Unsupported type for 'subject' argument : %s"
            raise TypeError, errmsg % str(type(subject))

        if isinstance(sender, basestring):
            header['From'] = sender
            sent_by = extractEmailAddress(sender)
            if debug:
                print "header['From'] =", sender
                print "sent_by =", sent_by
        else:
            errmsg = "Unsupported type for 'from_address' argument : %s"
            raise TypeError, errmsg % str(type(from_address))

        if mail_to is not None:
            _mail_to_ = untagleEmailAddress(mail_to)
            header['To'] = ','.join(_mail_to_)
            send_to = [extractEmailAddress(addr) for addr in _mail_to_]
        else:
            header['To'] = 'Test mode, no mail sent.'
            send_to = [ ]
        if debug:
            print "header['To'] =", header['To']
            print "send_to =", send_to

        cc = kwargs.get('cc',kwargs.get('Cc',kwargs.get('CC',None)))
        if cc is not None:
            cc = untagleEmailAddress(cc)
            header['Cc'] = ', '.join(cc)
            send_to.extend([extractEmailAddress(addr) for addr in cc])
            if debug:
                print "header['Cc'] =", header['Cc']
                print "send_to =", send_to

        bcc = kwargs.get('bcc',None)
        if bcc is not None:
            bcc = untagleEmailAddress(bcc)
            send_to.extend([extractEmailAddress(addr) for addr in bcc])
            if debug: print "send_to =", send_to
 
        reply_to = kwargs.get('reply_to',None)
        if reply_to is not None:
            header['Reply-To'] = ','.join(untagleEmailAddress(reply_to))

        header['Date'] = formatdate(time.mktime(datetime.now().timetuple()))

        return header, sent_by, send_to

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sendMessage(self, sender, mail_to, subject, message, **kwargs):
        """ send a simple text message via SMTP
        """
        test_ = kwargs.get('test',False)
        debug = kwargs.get('debug',test_)

        # Create a MIME message from the plain text message
        if isinstance(message, basestring):
            msg = MIMEText(message)
        elif isinstance(message,(list,tuple)):
            msg = MIMEText('\n'.join(message))
        else:
            errmsg = "Unsupported type for 'message' argument : %s"
            raise TypeError, errmsg % str(type(message))

        header, sent_by, send_to =\
        self.extractHeader(sender, mail_to, subject, **kwargs)

        for key, value in header.items():
            msg[key] = value

        if not test_ and mail_to is not None: 
            result = self.smtp_server.sendmail(sent_by,send_to,msg.as_string())
            print 'SmtpMailer.sendMessage', result
        return msg

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SmtpHtmlMailer(SmtpMailer):

    def sendMessage(self, sender, mail_to, subject, message, **kwargs):
        test_ = kwargs.get('test',False)
        debug = kwargs.get('debug',test_)

        text = kwargs.get('text',None)
        if text is None:
            text = self.textFromHtml(message)

        body = MIMEMultipart('alternative')
        body.attach(MIMEText(text, 'plain'))
        body.attach(MIMEText(message, 'html'))

        msg = MIMEMultipart()
        msg.preamble = 'This is a multi-part message in MIME format.\n' 
        msg.attach(body)

        header, sent_by, send_to =\
        self.extractHeader(sender, mail_to, subject, **kwargs)
        for key, value in header.items():
            msg[key] = value

        if not test_ and mail_to is not None: 
            result = self.smtp_server.sendmail(sent_by, send_to, msg.as_string())
            if result: print 'SmtpHtmlMailer.sendMessage result :\n', result

        return msg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def textFromHtml(self, html):
        textout = cStringIO.StringIO()
        formtext = formatter.AbstractFormatter(formatter.DumbWriter(textout))
        parser = htmllib.HTMLParser(formtext)
        parser.feed(html)
        parser.close()
        text = textout.getvalue()
        del textout, formtext, parser
        return text

