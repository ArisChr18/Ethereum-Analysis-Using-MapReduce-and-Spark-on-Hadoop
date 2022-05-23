from mrjob.job import MRJob, MRStep
import re
import time
from datetime import datetime

class TopWashTradingAddresses(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(reducer=self.reducerTT)]

    def RangeChecker(self, ToCheck, CheckAgainst):
        return ((ToCheck > CheckAgainst*0.95) and (ToCheck < CheckAgainst*1.05))

    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                from_address = fields[1]
                to_address = fields[2]
                value = float(fields[3])
                if value > 0 and to_address != 'null':
                    yield(to_address,('TO', value))
                    yield(from_address,('FROM', value))
        except Exception as e:
            pass

    def reducer(self, address, items):
        TO = False
        TOTOTAL = 0
        FROMTOTAL = 0
        FROM = False

        count = 0

        for item in items:
            if TO and FROM:
                TO = False
                FROM = False
                count += 1

            if (item[0] == 'TO'): TO = True; TOTOTAL += item[1]
            if (item[0] == 'FROM'): FROM = True; FROMTOTAL += item[1]

        if (count > 10) and self.RangeChecker(TOTOTAL, FROMTOTAL):
            yield(None,(address,count))

    def reducerTT(self, _, vals):
        TopTen = sorted(vals, key=lambda x: x[1], reverse=True)[0:10]
        r = 1
        for ranking in TopTen:
            yield(r,f'{ranking[0]},{ranking[1]}')
            r += 1

if __name__ == '__main__':
    TopWashTradingAddresses.JOBCONF= { 'mapreduce.job.reduces': '5' }
    TopWashTradingAddresses.run()