#!/usr/bin/env python2
#
# Demonstrates how to parse URLs with Python
# This version creates a Weblog object. For details review 
# http://stackoverflow.com/questions/472000/python-slots

import re              # bring in regular expression package
from dateutil import parser
# Import pytest if we have it available
try:
    import pytest
except ImportError as e:
    pass


class Weblog(object):
    __slots__ = ['ipaddr','timestamp','request','result','user','agent',
                 'referrer','url','date','time','datetime']
    clf_regex = '([(\d\.)]+) [^ ]+ [^ ]+ \[(.*)\] "(.*)" (\d+) [^ ]+ ("(.*)")? ("(.*)")?'
    clf_parser = re.compile(clf_regex)
    def __init__(self,line):
        m = self.clf_parser.match(line)
        if not m:
            raise ValueError("invalid logfile line: "+line)
        self.ipaddr = m.group(1)
        self.timestamp = m.group(2)
        self.request = m.group(3)
        self.result = int(m.group(4))
        self.user = m.group(5)
        self.referrer = m.group(6)
        self.agent = m.group(7)
        if self.agent:
            self.agent = self.agent.replace('"','')
        request_fields = self.request.split(" ")
        self.url = request_fields[2] if len(request_fields)>2 else ""
        self.datetime = parser.parse(self.timestamp.replace(":", " ", 1)).isoformat()
        self.date = self.datetime[0:10]
        self.time = self.datetime[11:]
        
# Tests
# test with pytest
demo_line1 = '172.16.0.3 - - [25/Sep/2002:14:04:19 +0200] "GET /hello.html HTTP/1.1" 401 - "" "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.1) Gecko/20020827"'
def test_weblog():
    obj = Weblog(demo_line1)
    assert obj.ipaddr=="172.16.0.3"
    assert obj.datetime==parser.parse("25-Sep-2002 14:04:19 +0200").isoformat()
    assert obj.request=="GET /hello.html HTTP/1.1"
    assert obj.result==401
    assert obj.referrer==""
    assert obj.agent=="Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.1) Gecko/20020827"
        

# test the parser
if __name__=="__main__":
    a = Weblog(demo_line1)
    print("url: %s  date: %s  time: %s  datetime: %s" % (a.url,a.date,a.time,a.datetime))
