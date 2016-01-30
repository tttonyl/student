# Implement wordcount as a 2-step mapreduce job
# 

from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

TOPN=10
class WordcountTop10(MRJob):
    def wordcount_mapper(self, _, line):
        for word in line.strip().split():
            word = filter(str.isalpha,word.lower())
            yield word,1

    def wordcount_reducer(self, key, values):
        yield key, sum(values)

    def top10_mapper(self, word, count):
        # notice that we put the counts first!
        yield "Top10", (count,word) 

    def top10_reducer(self, key, values):
        for value in heapq.nlargest(TOPN,values):
            yield key,value  

    def steps(self):
        return [
            MRStep(mapper=self.wordcount_mapper,
                   combiner=self.wordcount_reducer,
                   reducer=self.wordcount_reducer),

            MRStep(mapper=self.top10_mapper,
                   combiner=self.top10_reducer,
                   reducer=self.top10_reducer) ]

if __name__=="__main__":
    WordcountTop10.run()

