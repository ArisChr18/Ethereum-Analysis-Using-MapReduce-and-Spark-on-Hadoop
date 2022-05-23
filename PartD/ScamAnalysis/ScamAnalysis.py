import pyspark
import json
from pyspark.sql.functions import *

def good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
            str(fields[2])
            if int(fields[3]) == 0:
                return False
            return True
    except:
        return False

sc = pyspark.SparkContext()
print(sc.applicationId)
newcontext = pyspark.SQLContext(sc)

df = newcontext.read.option('header', False).format('csv').load("scams.csv")
scam_rdd = df.rdd.map(lambda x: (x[0],(x[1],x[2])))

transactions = sc.textFile('/data/ethereum/transactions').filter(good_line)
step1 = transactions.map(lambda l:  (l.split(',')[2], (float(l.split(',')[4]), float(l.split(',')[3]))))
step2 = step1.join(scam_rdd)
stepsum = step2.map(lambda x: (x[1][1][0], (x[1][0][0], x[1][0][1])))
sum_reduce = stepsum.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
sum_reduce.saveAsTextFile('MostLucrativeScams')

map2 = step2.map(lambda x: (x[1][1],  (x[1][0][0], x[1][0][1])))
reduced2 = map2.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).sortByKey(ascending=True)
reduced2.saveAsTextFile('ScamsTable')
