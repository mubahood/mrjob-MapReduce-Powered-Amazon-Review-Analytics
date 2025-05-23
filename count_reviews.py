from mrjob.job import MRJob
import json

class MRCountReviews(MRJob):

    def mapper(self, _, line):
        # parse each JSON line
        review = json.loads(line)
        asin = review.get('asin')
        if asin:
            # emit one count for this ASIN
            yield asin, 1

    def reducer(self, asin, counts):
        # sum all the ones to get total reviews per ASIN
        yield asin, sum(counts)

if __name__ == '__main__':
    MRCountReviews.run()