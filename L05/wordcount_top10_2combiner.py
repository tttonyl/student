#!/usr/bin/env python2

# This is the final wordcount program.
# Job #1:
# the mapper outputs <word,1>
# the reducer receives <word,(1,1,1,...)> and outputs <word,COUNT>
# Job #2:
# The mapper outputs ("TopN",(word,COUNT))
# The reducer outputs the TopN

import mrjob,os
from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

TOPN=10
class WordCountTopN(MRJob):
    # Strip the input into <word, 1>
    def mapper(self, _, line):
        for word in line.strip().lower().split():
            yield filter(str.isalpha,word),1
        
    def combiner(self, word, counts):
        yield word, sum(counts)

    # This reducer both sums and maintains a top-N list
    def reducer_init(self):
        self.heap = []

    def reducer(self, word, counts):
        heapq.heappush(self.heap,(sum(counts),word))
        if len(self.heap) > TOPN:
            heapq.heappop(self.heap)

    def reducer_final(self):
        for (count,word) in self.heap:
            yield (word,count)

    # The globalTopN mapper maintains a global topN list.
    # It uses a single Key to force all values to go through
    # a single reducer
    def globalTopN_mapper(self,word,count):
        yield "Top"+str(TOPN), (count,word)

    def globalTopN_reducer(self,_,countsAndWords):
        for countAndWord in heapq.nlargest(TOPN,countsAndWords):
            yield _,countAndWord
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final
                   ),

            MRStep(mapper=self.globalTopN_mapper,
                   reducer=self.globalTopN_reducer) ]



if __name__=="__main__":
    WordCountTopN.run()
