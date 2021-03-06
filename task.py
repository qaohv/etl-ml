from pyspark.sql import SparkSession, Row
import matplotlib.pyplot as plt
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Source csv or path to db not specified!", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("TrafficCount")\
        .getOrCreate()

    broadcast = spark.sparkContext.broadcast(sys.argv[2].strip())

    def IpLookup(pair):
      import geoip2.database
      reader = geoip2.database.Reader(broadcast.value)
      try:
          match = reader.country(pair.ip)
          return (match.country.name, pair.sum)
      except geoip2.errors.AddressNotFoundError:
          return ("NOT_FOUND", pair.sum)
      finally:
          reader.close()

    file = spark.read.csv(sys.argv[1].strip())
    lines = file.rdd.flatMap(lambda line: [(line[10], int(line[18])),(line[11], int(line[18]))]).\
                reduceByKey(lambda sum, add: sum + add).\
                map(lambda tuple: Row(ip=tuple[0],sum=tuple[1]))

    pd_df = spark.createDataFrame(lines).toPandas()
    pd_df.to_json("1.json","records")

    countries_traffic = lines.map(IpLookup).\
            reduceByKey(lambda sum, add: sum + add).\
            map(lambda line: Row(country=line[0], sum=line[1]))

    pd_df = spark.createDataFrame(countries_traffic).toPandas()
    pd_df.to_json("2.json","records")

    collected_countries_traffic = countries_traffic.collect()

    c = [ item[0] for item in collected_countries_traffic]
    t = [ item[1] for item in collected_countries_traffic]

    plt.figure(figsize=(40,40))
    plt.barh(range(len(t)), t)

    step = max(t) // 50
    plt.yticks(range(len(c)), c)
    plt.xticks(range(0,max(t), step), rotation=90)
    plt.grid(linestyle='-', axis='x')

    plt.savefig("3.png")
    spark.stop()
