from mrjob.job import MRJob
import json
from datetime import datetime

class MRAverageRatingTime(MRJob):
    def mapper(self, _, line):
        rec = json.loads(line)
        ts = rec.get('timestamp')
        rating = rec.get('rating')
        if ts and rating is not None:
            # Amazon timestamps are in milliseconds
            dt = datetime.utcfromtimestamp(ts/1000)
            period = dt.strftime('%Y-%m')  # e.g. "2023-05"
            yield period, (rating, 1)

    def reducer(self, period, values):
        total, count = 0.0, 0
        for r, c in values:
            total += r
            count += c
        yield period, total/count

if __name__ == '__main__':
    MRAverageRatingTime.run()