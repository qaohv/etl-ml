### Common info ###

* Spark version 2.1.0, python v 3.5.2
* Spark installed by manual https://www.santoshsrinivas.com/installing-apache-spark-on-ubuntu-16-04/
* Set PYSPARK_PYTHON=python3 and PYSPARK_DRIVER_PYTHON=python3
* Geolite2 lib https://github.com/maxmind/GeoIP2-python
* Download and gunzip GeoLite2 Country database
* System info: Ubuntu 16.04 LTS, Phenom II X4 3.2 GHz, 4GB RAM

### Usage ###

* Run: spark-submit task.py sflow.csv /full/path/to/database.mmdb

### Results ###

* Packets size for every ip in 1.json
* Packets size for every country in 2.json
* Histogram in file 3.png

### Contacts ###

* email: d.v.ryabokon@gmail.com
