from mrjob.job import MRJob
import json

class MRAverageRating(MRJob):

    def mapper(self, _, line):
        review = json.loads(line)
        asin   = review.get('asin')
        rating = review.get('rating')
        if asin is not None and rating is not None:
            # emit (sum_of_ratings, count_of_reviews)
            yield asin, (rating, 1)

    def reducer(self, asin, values):
        total, count = 0.0, 0
        for rating, cnt in values:
            total += rating
            count += cnt
        # compute average
        yield asin, total / count

if __name__ == '__main__':
    MRAverageRating.run()