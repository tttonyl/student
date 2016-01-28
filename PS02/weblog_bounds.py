#!/usr/bin/env python2

# This is the final wordcount program.
# the mapper outputs <word,1>
# the reducer receives <word,(1,1,1,...)> and outputs <word,COUNT>

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os

class WeblogBounds(MRJob):
    def mapper(self, _, line):
        # Get the name of the input file, per mrjob v0.4.6 documentation
        # https://pythonhosted.org/mrjob/utils-compat.html
        filename = mrjob.compat.jobconf_from_env("map.input.file")

        # parse the weblog input line
        log = Weblog(line)      
        
        # output <filename,date>
        yield filename,log.date


    def reducer(self, key, values):
        # find the minimum and the maximum date for each key
        # notice that we can't simply say min(values) and max(values), because we need to compute
        # both at the same time (we don't want to consume the values)
        vmin = None
        vmax = None
        for v in values:
            if v<vmin or not vmin: vmin=v
            if v>vmax or not vmax: vmax=v
        yield (key,(vmin,vmax))


if __name__=="__main__":
    WeblogBounds.run()
