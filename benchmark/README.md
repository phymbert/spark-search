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