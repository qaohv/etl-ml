from pyspark.sql import SparkSession, Row
import matplotlib.pyplot as plt
import sys
from operator import add

def IpLookup(ip):
  import geoip2.database
  reader = geoip2.database.Reader(path_to_db.value)
  try:
      match = reader.country(ip)
      if match.country.name is not None:
        return (ip, match.country.name)
      else:
        return (ip, "NOT_FOUND")
  except geoip2.errors.AddressNotFoundError:
      return (ip, "NOT_FOUND")
  finally:
    reader.close()

def resolveCountry(line):
    src_country, dst_country = ip_country.value[line[10]], ip_country.value[line[11]]

    if src_country == dst_country:
        return [(src_country, int(line[18]))]
    else:
        return [(src_country, int(line[18])), (dst_country, int(line[18]))]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Source csv or path to db not specified!", file=sys.stderr)
        exit(-1)

    SAMPLING_RATE = 512

    spark = SparkSession\
        .builder\
        .appName("TrafficCount")\
        .getOrCreate()

    path_to_db = spark.sparkContext.broadcast(sys.argv[2].strip())

    file = spark.read.csv(sys.argv[1].strip())
    all_ips = file.rdd.flatMap(lambda line: [line[10], line[11]]).distinct()
    tuples = all_ips.map(IpLookup)

    d = dict(tuples.collect())
    ip_country = spark.sparkContext.broadcast(d)

    country_with_traffic = file.rdd.flatMap(resolveCountry)\
                                .reduceByKey(add)\
                                .map(lambda tuple: Row(country=tuple[0], sum=tuple[1]*SAMPLING_RATE))

    pd_df = spark.createDataFrame(country_with_traffic).toPandas()
    pd_df.to_json("traffic.json","records")

    collected_countries_traffic = country_with_traffic.collect()

    c = [ item[0] for item in collected_countries_traffic]
    t = [ item[1] for item in collected_countries_traffic]

    plt.figure(figsize=(40,40))
    plt.barh(range(len(t)), t)

    step = max(t) // 50
    plt.yticks(range(len(c)), c)
    plt.xticks(range(0,max(t), step), rotation=90)
    plt.grid(linestyle='-', axis='x')

    plt.savefig("hist.png")

    spark.stop()
