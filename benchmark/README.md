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
# You need your kaggle auth token to download datasets
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
sudo yum install git openjdk-8-jdk java-devel
wget https://apache.mediamirrors.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
sudo tar xf apache-maven-3.6.3-bin.tar.gz -C /opt
sudo ln -s /opt/apache-maven-3.6.3/ /opt/maven
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64
export M2_HOME=/opt/maven
export MAVEN_HOME=/opt/maven
export PATH=${M2_HOME}/bin:${PATH}
git clone https://github.com/phymbert/spark-search.git
cd spark-search/
# Add -Pscala-2.11 to support luceneRDD benchmark
mvn install -DskipTests=true
````

# How to submit

 * connect to AWS EMR Master
 * Upload the shaded benchmark jar
 * submit the benchmark job
 
````sh
for bench in SearchRDDBenchmark ElasticsearchBenchmark LuceneRDDBenchmark SparkRDDRegexBenchmark
do
 spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --class benchmark.${bench} \
 --num-executors 3 \
 --executor-memory 10G \
 --executor-cores 4 \
 benchmark/target/spark-search-benchmark_*.jar
done
````

# Output

## Search RDD benchmark
````
Time taken: 127011 ms
for joined 33372 matches
(ICB INTERNATIONAL, INC.,8.383696556091309,icb international, inc.)
(PROSPECT GLOBAL RESOURCES INC.,9.529265403747559,prospect global resources inc.)
(CELLFOR INC.,8.472620964050293,cellfor inc.)
(PLAYDEK, INC.,8.471635818481445,playdek inc.)
(ASARCO INC,8.469249725341797,asarco inc)
(VICTORY FUNDS,9.432478904724121,victory funds)
(HORIZON SOFTWARE INTERNATIONAL LLC,8.548381805419922,horizon software international, llc.)
(POLUS INC.,8.469675064086914,polus inc.)
(PEERLESS MANUFACTURING CO,9.787846565246582,peerless manufacturing co.)
(BELLWETHER INVESTMENT GROUP, LLC,9.817503929138184,bellwether investment group, llc)
Time taken: 44668 ms
for count 310 matches
(6.369204521179199,ibm)
(6.263510704040527,ibm)
(6.263510704040527,ibm)
(6.263186454772949,ibm)
(6.263186454772949,ibm)
(6.263186454772949,ibm)
(6.171557426452637,ibm)
(6.171557426452637,ibm)
(6.171557426452637,ibm)
(5.75221061706543,viện ibm (ibm institute))
````

### Elastic hadoop
````
Time taken: 719285 ms
for joined 0 matches
Time taken: 486119 ms
for count 0 matches
````

### LuceneRDD
````
Time taken: 597091 ms
for joined 33372 matches
(ICB INTERNATIONAL, INC.,12.414156,icb international, inc.)
(PROSPECT GLOBAL RESOURCES INC.,13.979308,prospect global resources inc.)
(CELLFOR INC.,12.640068,cellfor inc.)
(PLAYDEK, INC.,12.637392,playdek inc.)
(ASARCO INC,12.635831,asarco inc)
(VICTORY FUNDS,14.131169,victory funds)
(HORIZON SOFTWARE INTERNATIONAL LLC,12.751942,horizon software international, llc.)
(POLUS INC.,12.636959,polus inc.)
(PEERLESS MANUFACTURING CO,14.21593,peerless manufacturing co.)
(BELLWETHER INVESTMENT GROUP, LLC,14.327955,bellwether investment group, llc)
Time taken: 400418 ms
for count 310 matches
(11.202088356018066,ibm)
(11.034196853637695,ibm)
(11.034196853637695,ibm)
(11.034196853637695,ibm)
(11.033937454223633,ibm)
(11.033937454223633,ibm)
(10.889117240905762,ibm)
(10.889117240905762,ibm)
(10.889117240905762,ibm)
(7.921072483062744,ibm usa)
...
````

## Regex benchmark
````
Time taken: 14400 ms
for count 494 matches
(0.0,instituto de biologia molecular do parana - ibmp)
````