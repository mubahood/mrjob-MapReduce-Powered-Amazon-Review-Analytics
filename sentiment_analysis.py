from mrjob.job import MRJob
from textblob import TextBlob
import json

class MRSentimentAnalysis(MRJob):
    def mapper(self, _, line):
        rec = json.loads(line)
        text = rec.get('text', '')
        if text:
            polarity = TextBlob(text).sentiment.polarity
            if polarity > 0.1:
                sentiment = 'positive'
            elif polarity < -0.1:
                sentiment = 'negative'
            else:
                sentiment = 'neutral'
            yield sentiment, 1

    def reducer(self, sentiment, counts):
        yield sentiment, sum(counts)

if __name__ == '__main__':
    MRSentimentAnalysis.run()