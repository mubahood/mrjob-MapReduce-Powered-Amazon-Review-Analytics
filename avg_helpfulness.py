from mrjob.job import MRJob
import json

class MRAverageHelpfulness(MRJob):

    def mapper(self, _, line):
        review = json.loads(line)
        # number of helpful votes (0 if missing)
        helpful = review.get('helpful_vote', 0)
        # emit (sum_helpful, count_reviews)
        yield None, (helpful, 1)

    def reducer(self, _, values):
        total_helpful = 0
        total_reviews = 0
        for h, c in values:
            total_helpful += h
            total_reviews += c
        # compute global average helpful votes per review
        avg = total_helpful / total_reviews if total_reviews else 0
        yield "average_helpfulness", avg

if __name__ == '__main__':
    MRAverageHelpfulness.run()