from mrjob.job import MRJob
import json

class MRHelpfulnessByRating(MRJob):
    def mapper(self, _, line):
        rec = json.loads(line)
        rating = rec.get('rating')
        helpful = rec.get('helpful_vote', 0)
        if rating is not None:
            yield int(rating), (helpful, 1)

    def reducer(self, rating, values):
        total_helpful, count = 0, 0
        for h, c in values:
            total_helpful += h
            count += c
        avg_helpful = total_helpful/count if count else 0
        yield rating, avg_helpful

if __name__ == '__main__':
    MRHelpfulnessByRating.run()