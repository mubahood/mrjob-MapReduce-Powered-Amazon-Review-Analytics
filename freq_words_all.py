from mrjob.job import MRJob
import json, re

STOPWORDS = {
  'the','and','is','to','it','of','a','in','that','this','with',
  'for','as','on','i','was','but','are','my','have','so','at'
}
WORD_RE = re.compile(r"[a-zA-Z']+")

class MRWordFreqAll(MRJob):
    def mapper(self, _, line):
        rec = json.loads(line)
        text = rec.get('text','').lower()
        for w in WORD_RE.findall(text):
            if w not in STOPWORDS and len(w) > 1:
                yield w, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRWordFreqAll.run()