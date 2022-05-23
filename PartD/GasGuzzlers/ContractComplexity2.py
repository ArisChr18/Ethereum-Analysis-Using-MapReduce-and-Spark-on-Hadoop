from mrjob.job import MRJob
import time

class ContractComplexity2(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 9:
                timestamp = int(fields[7])
                month = time.strftime("%m", time.gmtime(timestamp))
                year = time.strftime("%Y", time.gmtime(timestamp))
                key = str(year) + '-' + str(month)
                gas_used = int(fields[6])

                yield key, gas_used
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
    ContractComplexity2.run()
