# Goal
This project has objectives to present performance of different
full text search approaches using spark.

# How to build benchmark project
```bash
mvn clean package
```

# Infrastructure
All benchmarks run within AWS m5.xlarge instances.

For Spark, 3 workers allocated.
For Spark+ES: 3 data nodes allocated.

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
for bench in SearchRDDBenchmark LuceneRDDBenchmark SparkRDDRegexBenchmark
do
 spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --class benchmark.${bench} \
 --executor-memory 10G \
 --executor-cores 4 \
 /tmp/benchmark/target/spark-search-benchmark-0.1.5-SNAPSHOT.jar
done
````

# Output

## Search RDD
````
Count 310 matches in 0.0ms
(6.369204521179199,ibm)
...
Joined 33372 matches in 131.072s
(PLAYDEK, INC.,8.471635818481445,playdek inc.)
...
````

### LuceneRDD
````
Count 310 matches in 393216.0ms
(11.202088356018066,ibm)
...
Joined 33372 matches in 786.432s
(BLUE VASE SECURITIES, LLC,16.833273,blue vase securities llc)
...
````