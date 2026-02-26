#!/usr/bin/env python3
#word count using MRJob (MapReduce)
#DOB: 07/13/2002
import re
from mrjob.job import MRJob

WORD_Regex = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")

class MRWordCount(MRJob):

    def mapper(self, _, line):
        for w in WORD_Regex.findall(line):
            yield w.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    MRWordCount.run()
