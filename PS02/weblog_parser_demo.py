#!/usr/bin/env python2
#
# Demonstrates how to parse URLs with Python

import re              # bring in regular expression package
from dateutil import parser

# regular expression to parse common log file:
clf_regex = '([(\d\.)]+) [^ ]+ [^ ]+ \[(.*?)\] "(.*?)" (\d+) [^ ]+ "(.*?)" "(.*?)"'

clf_parser = re.compile(clf_regex)

# Now, demonstrate the parser. Here is a line from an actual log file:
log_line = '172.16.0.3 - - [25/Sep/2002:14:04:19 +0200] "GET /hello.html HTTP/1.1" 401 - "" "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.1) Gecko/20020827"'

# parse it:
(ip,timestamp,verb,result,user,agent) = clf_parser.match(log_line).groups()

# extract the URL and the date:
url = verb.split(" ")[1]
date = parser.parse(timestamp.replace(":", " ", 1))

# and print it
print("url: %s  date: %s" % (url,date))
