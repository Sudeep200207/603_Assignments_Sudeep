#!/usr/bin/env python3
#Task 2: non english word count using MRJob (MapReduce)
#DOB: 07/13/2002
import re
from mrjob.job import MRJob

WORD_Regex = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")

class MRNonEnglish(MRJob):

    def configure_args(self):
        super().configure_args()
        #shipping english_words.txt along with the job
        self.add_file_arg("--dict", default="english_words.txt", help="English dictionary file")

    def mapper_init(self):
        self.english = set()
        with open(self.options.dict, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                self.english.add(line.strip().lower())

    def mapper(self, _, line):
        for w in WORD_Regex.findall(line):
            w = w.lower()

            #filtering tiny junk tokens
            if len(w) < 3:
                continue

            #skipping if it's in dictionary, it's english
            if w in self.english:
                continue

            yield w, 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    MRNonEnglish.run()
