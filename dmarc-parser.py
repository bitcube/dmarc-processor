#!/usr/bin/python

import re
import sys
import os
import imaplib
import datetime
import email
import argparse
import zlib
import zipfile
import StringIO
import xml.etree.ElementTree as ET
import unicodedata
import time

DEBUG = 3
VERBOSE = 2
INFO = 1
ERROR = 0
PRINTLEVEL = INFO

def info(msg, level = INFO, newline = True):
  if level <= PRINTLEVEL:
    if newline:
      print datetime.datetime.now().strftime("%b %-d %H:%M:%S") + " " + os.uname()[1].split(".")[0] + " [" + str(os.getpid()) + "]: " + msg
    else:
      sys.stdout.write(datetime.datetime.now().strftime("%b %-d %H:%M:%S") + " " + os.uname()[1].split(".")[0] + " [" + str(os.getpid()) + "]: " + msg)
      sys.stdout.flush()

def connect(hostname, username, password, readonly = False, folder = None):
  try:
    info("Connecting to " + hostname, DEBUG)
    mailbox = imaplib.IMAP4_SSL(hostname)
    info("Connected to " + hostname, DEBUG)
  except:
    print "Error: Cannot connect to " + hostname
    sys.exit(1)

  try:
    info("Login in as " + username, DEBUG)
    mailbox.login(username, password)
    info("Logged in as " + username, DEBUG)
  except:
    print "Error: Incorrect username/password"
    sys.exit(2)

  if args.listfolders:
    print "Folders found:"
    for item in mailbox.list()[1]:
      print "  " + item
    sys.exit(0)

  if folder != None:
    info("Selecting folder " + folder, DEBUG)
    res, data = mailbox.select(folder, readonly)
  else:
    info("Select root folder", DEBUG)
    res, data = mailbox.select(readonly = readonly)
  info("Folder selected, result: " + res, DEBUG);

  return mailbox

def extract_gzip(payload):
  info("      Extracting using ZLIB", DEBUG)
  data = zlib.decompress(payload, 16+zlib.MAX_WBITS)
  return data
  

def extract_zip(payload):
  info("      Extracting using ZIP", DEBUG)
  memZip = StringIO.StringIO()
  memZip.write(payload)
  zf = zipfile.ZipFile(memZip)
  files = {name: zf.read(name) for name in zf.namelist()}
  for name in files:
    return files[name]

def gettext(xml, xpath):
  try:
    return xml.find(xpath).text
  except:
    return ""

def getrecords(xml, msg):
  tree = ET.fromstring(xml)
  records = []
  if "Message-Id" in msg:
    msgid = msg['Message-Id']
  else:
    msgid = "<Undefined>"
  for xmlrecord in tree.findall("record"):
    record = {
      "orgname":                gettext(tree, "report_metadata/org_name"),
      "orgemail":               gettext(tree, "report_metadata/email"),
      "extra_contact_info":     gettext(tree, "report_metadata/extra_contact_info"),
      "report_id":              gettext(tree, "report_metadata/report_id"),
      "date_epoch_begin":       gettext(tree, "report_metadata/date_range/begin"),
      "date_epoch_end":         gettext(tree, "report_metadata/date_range/end"),
      "date_human_begin":       time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(gettext(tree, "report_metadata/date_range/begin")))),
      "date_human_end":         time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(gettext(tree, "report_metadata/date_range/end")))),
      "error":                  gettext(tree, "report_metadata/error"),
      "mail_to":                msg['To'],
      "mail_from":              msg['From'],
      "mail_date":              msg['Date'],
      "mail_msgid":             msgid,
      "policy_domain":          gettext(tree, "policy_published/domain"),
      "policy_adkim":           gettext(tree, "policy_published/adkim"),
      "policy_aspf":            gettext(tree, "policy_published/aspf"),
      "policy_p":               gettext(tree, "policy_published/p"),
      "policy_sp":              gettext(tree, "policy_published/sp"),
      "policy_pct":             gettext(tree, "policy_published/pct"),
      "source_ip":              gettext(xmlrecord, "row/source_ip"),
      "evaluated_disposition":  gettext(xmlrecord, "row/policy_evaluated/disposition"),
      "evaluated_dkim":         gettext(xmlrecord, "row/policy_evaluated/dkim").lower(),
      "evaluated_spf":          gettext(xmlrecord, "row/policy_evaluated/spf").lower(),
      "evaluated_reason_type":  gettext(xmlrecord, "row/policy_evaluated/reason/type"),
      "evaluated_reason_comm":  gettext(xmlrecord, "row/policy_evaluated/reason/comment"),
      "envelope_to":            gettext(xmlrecord, "identifiers/envelope_to"),
      "header_from":            gettext(xmlrecord, "identifiers/header_from"),
      "auth_dkim_domain":       gettext(xmlrecord, "auth_results/dkim/domain"),
      "auth_dkim_result":       gettext(xmlrecord, "auth_results/dkim/result").lower(),
      "auth_dkim_human_result": gettext(xmlrecord, "auth_results/dkim/human_result"),
      "auth_spf_domain":        gettext(xmlrecord, "auth_results/spf/domain"),
      "auth_spf_result":        gettext(xmlrecord, "auth_results/spf/result").lower(),
    }
    count = int(gettext(xmlrecord, "row/count"))
    info("    Adding records " + str(count) + " times", DEBUG)
    for i in range(count):
      records.append(record)

  return records
  
def processattachment(filename, payload, msg):
  ext = os.path.splitext(filename)
  info("    Attachment extension: " + ext[1], DEBUG)
  if ext[1] == ".gz" or ext[1] == ".gzip":
    data = extract_gzip(payload)
  elif ext[1] == ".zip":
    data = extract_zip(payload)
  else:
    info("    ERROR: Unknown extension: " + ext[1], ERROR)
    return False
  records = getrecords(data, msg)
  if "Message-Id" in msg:
    msgid = msg['From'] + " - " + msg['Message-Id']
  else:
    msgid = msg['From'] + "-" + msg['Date']

  info("    Mail message id: " + msgid, DEBUG)
  basename = unicodedata.normalize('NFKD', unicode(msgid)).encode('ascii', 'ignore')
  basename = unicode(re.sub('[^@.\w\s-]', '', basename).strip().lower())
  basename = unicode(re.sub('[-\s]+', '-', basename))

  duplicate = 0
  filename = os.path.join(args.output, basename + ".csv")
  while os.path.isfile(filename):
    duplicate += 1
    filename = os.path.join(args.output, basename + "-" + str(duplicate) + ".csv")
  info("    Save CSV as: " + filename, VERBOSE)
  line = '"Organization Name","Organization Email","Extra Contact Info","Report ID","Date Begin (EPOCH)","Date End (EPOCH)","Date Begin (Human)","Date End (Human)","Error","Mail Recipient","Mail Sender","Mail Date","Mail MessageID","Policy: Domain","Policy: ADKIM","Policy: ASPF","Policy: P","Policy: SP","Policy: PCT","Source IP","Evaluated Disposition","Evaluated DKIM","Evaluated SPF","Evaluated Reason","Evaluated Comment","Envelope To","Header From","Auth DKIM Domain","Auth Result DKIM","Auth Human Result DKIM","Auth SPF Domain","Auth Result SPF"\n'

  if not args.nosave:
    fp = open(filename, "w")
    fp.write(line)
  
  for record in records:
    line = '"' + record['orgname'] + '",'
    line+= '"' + record['orgemail'] + '",'
    line+= '"' + record['extra_contact_info'] + '",'
    line+= '"' + record['report_id'] + '",'
    line+= '"' + record['date_epoch_begin'] + '",'
    line+= '"' + record['date_epoch_end'] + '",'
    line+= '"' + record['date_human_begin'] + '",'
    line+= '"' + record['date_human_end'] + '",'
    line+= '"' + record['error'] + '",'
    line+= '"' + record['mail_to'] + '",'
    line+= '"' + record['mail_from'] + '",'
    line+= '"' + record['mail_date'] + '",'
    line+= '"' + record['mail_msgid'] + '",'
    line+= '"' + record['policy_domain'] + '",'
    line+= '"' + record['policy_adkim'] + '",'
    line+= '"' + record['policy_aspf'] + '",'
    line+= '"' + record['policy_p'] + '",'
    line+= '"' + record['policy_sp'] + '",'
    line+= '"' + record['policy_pct'] + '",'
    line+= '"' + record['source_ip'] + '",'
    line+= '"' + record['evaluated_disposition'] + '",'
    line+= '"' + record['evaluated_dkim'] + '",'
    line+= '"' + record['evaluated_spf'] + '",'
    line+= '"' + record['evaluated_reason_type'] + '",'
    line+= '"' + record['evaluated_reason_comm'] + '",'
    line+= '"' + record['envelope_to'] + '",'
    line+= '"' + record['header_from'] + '",'
    line+= '"' + record['auth_dkim_domain'] + '",'
    line+= '"' + record['auth_dkim_result'] + '",'
    line+= '"' + record['auth_dkim_human_result'] + '",'
    line+= '"' + record['auth_spf_domain'] + '",'
    line+= '"' + record['auth_spf_result'] + '"'
    if not args.nosave:
      fp.write(line + "\n")
    else:
      print line
  
  if not args.nosave:
    fp.close()

def processmessage(msg, to):
  if 'To' not in msg:
    info("No to header found in email", ERROR)
    return False

  if 'Subject' not in msg:
    info("No subject header found in email", ERROR)
    return False

  if 'Date' not in msg:
    info("No date header found in email", ERROR)
    return False

  info("  Message is sent to " + msg['To'], VERBOSE)
  info("  Subject of message: " + msg['Subject'], DEBUG)
  info("  Date of message: " + msg['Date'], DEBUG)
  info("  Mail content type:" + msg.get_content_maintype(), DEBUG)

  if to != None:
    if msg['To'] != to and msg['To'] != "<" + to + ">":
      info("  - Skipping message, not right recipient", DEBUG)
      return False

  for part in msg.walk():
    info("  Processing part: " + part.get_content_maintype(), DEBUG)
    if part.get_content_maintype() == 'multipart':
      info ("    Skipping because of maintype", DEBUG)
      continue
    if part.get('Content-Disposition') is None:
      info ("    Skipping because of Content-Disposition", DEBUG)
      continue
    filename = part.get_filename()
    if filename == None:
      info("  No filename given, skipping this attachment", VERBOSE)
      continue
    info("    Found attachment: " + filename, VERBOSE)
    data = part.get_payload(decode=True)
    if not data:
      info("      No data found in attachment", VERBOSE)
      continue
    processattachment(filename, data, msg)


def processmailbox(mailbox, to = None, all = False):

  if all:
    info("Searching ALL messages", DEBUG)
    res, data = mailbox.search(None, "ALL")
  else:
    info("Searching all UNSEEN messages", DEBUG)
    res, data = mailbox.search(None, "UNSEEN")
  info("Result of search: " + res, DEBUG);

  if len(data[0]) == 0:
    info("No (new) messages found", INFO)
    return True

  msgids = data[0].split(' ');
  info("Found " + str(len(msgids)) + " messages", VERBOSE);
  counter = 0
  for msgid in msgids:
    counter += 1
    process = int(float(float(counter) / float(len(msgids))) * 100)
    if PRINTLEVEL == INFO:
      if process % 5 == 0:
        info("Processed " + str(counter) + "/" + str(len(msgids)) + " - "  + str(process) + "%", INFO)
    info("Retrieving message " + msgid + "/" + str(len(msgids)) + " - " + str(process) + "%", VERBOSE)
    res, data = mailbox.fetch(msgid, '(RFC822)')
    info("Result of retrieving: " + res, DEBUG)
    msg = email.message_from_string(data[0][1])
    processmessage(msg, to)


parser = argparse.ArgumentParser(description="Process a mailbox for DMARC reports and save them as CSV files")
parser.add_argument('--server',   '-s', required=True,  help="Hostname of the IMAP server")
parser.add_argument('--username', '-u', required=True,  help="Username of the IMAP account")
parser.add_argument('--password', '-p', required=False, help="Password of the IMAP account; unsafe use environment variable IMAPPW for this")
parser.add_argument('--to',       '-t', required=False, help="The to-address to filter out message")
parser.add_argument('--folder',   '-f', required=False, help="The folder")
parser.add_argument('--listfolders',    required=False, help="List folders", action='store_true')
parser.add_argument('--output',   '-o', required=True,  help="The directory to save the CVS files")
parser.add_argument('--verbose',  '-v', required=False, help="Print verbose messages", action='store_true')
parser.add_argument('--debug',    '-d', required=False, help="Print debug messages", action='store_true')
parser.add_argument('--readonly', '-r', required=False, help="Connect readonly to the IMAP server", action='store_true')
parser.add_argument('--all',      '-a', required=False, help="Instead of processing only unread messages, process all messages", action='store_true')
parser.add_argument('--nosave',         required=False, help="Print instead of save to csv", action='store_true')
args = parser.parse_args()

if args.password == None:
  if 'IMAPPW' not in os.environ:
    info("ERROR: No imap password defined (use IMAPPW='mypassword' ./dmarc-parser.py", ERROR)
    sys.exit(2)
  else:
    args.password = os.environ['IMAPPW']

if args.verbose:
  PRINTLEVEL = VERBOSE

if args.debug:
  PRINTLEVEL = DEBUG

processmailbox(mailbox = connect(args.server, args.username, args.password, readonly = args.readonly, folder = args.folder), all = args.all, to = args.to)
