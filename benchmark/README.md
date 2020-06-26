# Goal
This project has objectives to present performance of different
full text search approaches using spark.

# How to build benchmark project
```bash
mvn clean package
```

# Infrastructure
All benchmarks run within AWS m5.xlarge instances.

For Spark, 3 workers required.
For Spark+ES: 3 data nodes required.

# Prepare data from master
```sh
curl -L -o companies.zip 'https://storage.googleapis.com/kaggle-data-sets/189687%2F423331%2Fcompressed%2Fcompanies_sorted.csv.zip?GoogleAccessId=XXX' \
  -H 'authority: storage.googleapis.com' \
  -H 'upgrade-insecure-requests: 1' \
  -H 'sec-fetch-site: cross-site' \
  -H 'sec-fetch-mode: navigate' \
  -H 'sec-fetch-user: ?1' \
  -H 'sec-fetch-dest: document' \
  -H 'referer: https://www.kaggle.com/' \
  --compressed

unzip companies.zip

curl -L -o sec__edgar_company_info.csv.zip  'https://storage.googleapis.com/kaggle-data-sets/1538%2F913323%2Fcompressed%2Fsec__edgar_company_info.csv.zip?GoogleAccessId=XXXX' \
  -H 'authority: storage.googleapis.com' \
  -H 'upgrade-insecure-requests: 1' \
  -H 'sec-fetch-site: cross-site' \
  -H 'sec-fetch-mode: navigate' \
  -H 'sec-fetch-user: ?1' \
  -H 'sec-fetch-dest: document' \
  -H 'referer: https://www.kaggle.com/' \
  --compressed
unzip sec__edgar_company_info.csv.zip

sudo hdfs
hdfs dfs -chown zeppelin /
```

Copy to hdfs from zeppelin
```scala
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.FileSystem

import org.apache.hadoop.fs.Path

val hadoopConf = new Configuration()

val hdfs = FileSystem.get(hadoopConf)

val srcCompaniesPath = new Path("companies_sorted.csv")
val destCompaniesPath = new Path("hdfs:///companies_sorted.csv")
hdfs.copyFromLocalFile(srcCompaniesPath, destCompaniesPath)

val srcEdgarCompaniesPath = new Path("sec__edgar_company_info.csv")
val destEdgarCompaniesPath = new Path("hdfs:///sec__edgar_company_info.csv")
hdfs.copyFromLocalFile(srcEdgarCompaniesPath, destEdgarCompaniesPath)
```

# How to submit

 * connect to AWS EMR Master
 * Upload the shaded benchmark jar
 * submit the benchmark job
 
````sh
spark-submit --master yarn --deploy-mode cluster --class benchmark.SearchRDDBenchmark /tmp/benchmark/target/spark-search-benchmark-0.1.5-SNAPSHOT.jar
````


