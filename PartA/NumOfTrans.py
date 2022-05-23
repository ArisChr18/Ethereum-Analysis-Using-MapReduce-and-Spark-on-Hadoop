from mrjob.job import MRJob
import time

class NumOfTrans(MRJob):

    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                timestamp = int(fields[6])
                year = time.strftime("%Y", time.gmtime(timestamp))
                month = time.strftime("%m", time.gmtime(timestamp))
                key = str(year) + '-' + str(month)

                yield key, 1
        except:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    NumOfTrans.run()
