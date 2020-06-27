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

hdfs dfs -put *.csv /
```

# Build benchmark project

* From the AWS master
````sh
sudo yum install git
wget https://apache.mediamirrors.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
sudo tar xf apache-maven-3.6.3-bin.tar.gz -C /opt
sudo ln -s /opt/apache-maven-3.6.3/ /opt/maven
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64
export M2_HOME=/opt/maven
export MAVEN_HOME=/opt/maven
export PATH=${M2_HOME}/bin:${PATH}
git clone https://github.com/phymbert/spark-search.git
cd spark-search/
mvn install -DskipTests=true
cd benchmark
mvn package
````

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
 target/spark-search-benchmark-0.1.5-SNAPSHOT.jar
done
````

# Output

## Search RDD benchmark
````
Time taken: 128617 ms
for joined 33372 matches
(ICB INTERNATIONAL, INC.,8.383696556091309,icb international, inc.)
...
Time taken: 95525 ms
for joined 33372 matches
Time taken: 95225 ms
for joined 33372 matches
Time taken: 99027 ms
for joined 33372 matches
Time taken: 93551 ms
for joined 33372 matches
Time taken: 41826 ms
for count 310 matches
(6.369204521179199,ibm)
...
Time taken: 45155 ms
for count 310 matches
Time taken: 45238 ms
for count 310 matches
Time taken: 41626 ms
for count 310 matches
Time taken: 45143 ms
for count 310 matches
````

### LuceneRDD
````
Time taken: 1123398 ms
for joined 33372 matches
(FYUSION, INC.,12.636959,fyusion, inc)
Time taken: 1135545 ms
for joined 33372 matches
/!\stopped
Time taken: 367431 ms
for count 310 matches
(11.202088356018066,ibm)
...
Time taken: 405245 ms
for count 310 matches
Time taken: 405559 ms
for count 310 matches
Time taken: 413801 ms
for count 310 matches
/!\ stopped
...
````

## Regex benchmark
````
Time taken: 14400 ms
for count 494 matches
(0.0,instituto de biologia molecular do parana - ibmp)
...
Time taken: 12991 ms
for count 494 matches
Time taken: 12630 ms
for count 494 matches
Time taken: 12450 ms
for count 494 matches
Time taken: 12524 ms
for count 494 matches
````