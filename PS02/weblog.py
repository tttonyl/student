#!/usr/bin/env python2
#
# Demonstrates how to parse URLs with Python
# This version creates a Weblog object. For details review 
# http://stackoverflow.com/questions/472000/python-slots

import re              # bring in regular expression package
from dateutil import parser

class Weblog(object):
    __slots__ = ['ip','timestamp','request','result','user','agent',
                 'referrer','url','date']
    clf_regex = '([(\d\.)]+) [^ ]+ [^ ]+ \[(.*)\] "(.*)" (\d+) [^ ]+ ("(.*)")? ("(.*)")?'
    clf_parser = re.compile(clf_regex)
    def __init__(self,line):
        m = self.clf_parser.match(line)
        if not m:
            raise ValueError("invalid logfile line: "+line)
        self.ip = m.group(1)
        self.timestamp = m.group(2)
        self.request = m.group(3)
        self.result = m.group(4)
        self.user = m.group(5)
        self.agent = m.group(6)
        self.referrer = m.group(7)
        self.url = self.request.split(" ")[1]
        self.date = parser.parse(self.timestamp.replace(":", " ", 1)).isoformat()
        
# test the parser
if __name__=="__main__":
    line = '172.16.0.3 - - [25/Sep/2002:14:04:19 +0200] "GET /hello.html HTTP/1.1" 401 - "" "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.1) Gecko/20020827"'
    a = WeblogParser(line)
    print("url: %s  date: %s" % (a.url,a.date))
