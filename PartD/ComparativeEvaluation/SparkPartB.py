import pyspark
from operator import add
from time import *

def transaction_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        return True
    except:
        return False

def contract_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
    except:
        return False
    return True

def remove_null(features):
    try:
        if features[1][1] is None:
            return False
    except:
        return False
    return True

begin_time = time()

sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions/")
contracts = sc.textFile("/data/ethereum/contracts/")

#Filter and extract values
transaction_features = transactions.filter(transaction_line).map(lambda l: (l.split(",")[2], int(l.split(",")[3])))
#Aggregate
transaction_values = transaction_features.reduceByKey(add)

#Filter and extract address
contract_address = contracts.filter(contract_line).map(lambda l: (l.split(",")[0], "contract"))

#Join values
join_features = contract_address.leftOuterJoin(transaction_values)
#Remove Null values
filter_features = join_features.filter(remove_null)
#Remove label info
result = filter_features.map(lambda l: (l[0], l[1][1]))

#Sort
Top10 = result.takeOrdered(10, key=lambda x: -x[1])
for record in Top10:
    print("{}: {}".format(record[0], record[1]))

#Save as txt
sc.parallelize(Top10).saveAsTextFile("SparkPartB")

end_time = time()
print("Seconds: " + str(end_time - begin_time))