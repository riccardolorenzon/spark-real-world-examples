from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("./customer-orders.csv")
mapped_input = input.map(extractCustomerPricePairs)
total_by_customer = mapped_input.reduceByKey(lambda x, y: x + y)
amount_first = total_by_customer.map(lambda (x,y): (y,x))
amount_ordered = amount_first.sortByKey()

results = amount_ordered.collect();
for result in results:
    print('{:.2f} - customer ID: {}'.format(result[0], result[1]))
