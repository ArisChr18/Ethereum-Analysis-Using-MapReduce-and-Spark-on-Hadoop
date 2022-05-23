from mrjob.job import MRJob
import time

class ContractComplexity(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 9:
                timestamp = int(fields[7])
                month = time.strftime("%m", time.gmtime(timestamp))
                year = time.strftime("%Y", time.gmtime(timestamp))
                key = str(year) + '-' + str(month)
                difficulty = int(fields[3])

                yield key, difficulty
        except:
            pass

    def reducer(self, key, values):
        sum = 0
        count = 0

        for value in values:
            sum += value
            count += 1
        yield key, sum/count

if __name__ == '__main__':
    ContractComplexity.run()
