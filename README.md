## [Spark Search](https://github.com/phymbert/spark-search)

[![Maven Central](https://img.shields.io/maven-central/v/io.github.phymbert/spark-search_2.12.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:io.github.phymbert%20%20a:spark-search_2.12)
[![CI](https://github.com/phymbert/spark-search/workflows/CI/badge.svg)](https://github.com/phymbert/spark-search/actions)
[![license](https://img.shields.io/github/license/phymbert/spark-search.svg)](LICENSE)
[![LoC](https://tokei.rs/b1/github/phymbert/spark-search?category=lines)](https://github.com/phymbert/spark-search)
[![codecov](https://codecov.io/gh/phymbert/spark-search/branch/master/graph/badge.svg)](https://codecov.io/gh/phymbert/spark-search)

[Spark](https://spark.apache.org/) Search brings advanced full text search features to your Dataframe, Dataset and RDD. Powered by [Apache Lucene](https://lucene.apache.org/).

## Context
Let's image you have a billion records dataset you want to query on and match against another one using full text search...
You do not expect an external datasource or database system than Spark, and of course with the best performances.
Spark Search fits your needs: it builds for all parent RDD partitions a one-2-one volatile Lucene index available
 during the lifecycle of your spark session across your executors local directories and RAM.
Strongly typed, Spark Search API plans to support Java, Scala and Python Spark SQL, Dataset and RDD SDKs.
Have a look and feel free to contribute!

## Getting started

### RDD API
* Scala

```scala
import org.apache.spark.search.rdd._ // to implicitly enhance RDD with search features

// Load some Amazon computer user reviews
val computersReviews = loadReviews("**/*/reviews_Computers.json.gz") 
    // Number of partition is the number of Lucene index which will be created across your cluster
    .repartition(4)

// Count positive review: indexation + count matched doc
computersReviews.count("reviewText:happy OR reviewText:best or reviewText:good")

// Search for key words
computersReviews.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 100)
  .foreach(println)

// /!\ Important lucene indexation is done each time a SearchRDD is computed,
// if you do multiple operations on the same parent RDD, you might have a variable in the driver:
val computersReviewsSearchRDD = computersReviewsRDD.searchRDD(
  SearchOptions.builder[Review]() // See all other options SearchOptions, IndexationOptions and ReaderOptions
    .read((r: ReaderOptions.Builder[Review]) => r.defaultFieldName("reviewText"))
    .analyzer(classOf[EnglishAnalyzer])
    .build())

// Boolean queries and boosting examples returning RDD
computersReviewsSearchRDD.search("(RAM or memory) and (CPU or processor)^4", 10).foreach(println)

// Fuzzy matching
computersReviews.searchList("(reviewerName:Mikey~0.8) or (reviewerName:Wiliam~0.4) or (reviewerName:jonh~0.2)",
                                      topKByPartition = 10)
                        .map(doc => s"${doc.source.reviewerName}=${doc.score}"
                        .foreach(println)

// RDD full text joining - example here searches for persons who did both computer and software reviews with fuzzy matching on reviewer name
val softwareReviews = loadReviews("**/*/reviews_Software_10.json.gz")
val matchesReviewers = computersReviews.searchJoin(softwareReviewsRDD, (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName + "\""}~0.4", 10)
matchesReviewersRDD
  .filter(_.hits.nonEmpty)
  .map(m => (m.doc.reviewerName, m.hits.map(h => s"${h.source.reviewerName}=${h.score}=${h.source.asin}")))
  .foreach(println)

// Save then restore onto hdfs
matchesReviewersRDD.save("hdfs:///path-for-later-query-on")
val restoredSearchRDD = loadSearchRDD[Review](sc, "hdfs:///path-for-later-query-on")

// Drop duplicates 
restoredSearchRDD.searchDropDuplicates()
```

See [Examples](examples/src/main/scala/all/examples/org/apache/spark/search/rdd/SearchRDDExamples.scala) and [Documentation](core/src/main/scala/org/apache/spark/search/rdd/SearchRDD.scala) for more details.

* Java
```java
import org.apache.spark.search.rdd.*;

//Create the SearchRDD based on the JavaRDD to enjoy search features
SearchRDDJava<Review> searchRDDJava = new SearchRDDJava<>(reviewRDD, Review.class);

// Count matching docs
System.err.println("Reviews with good recommendations: " + searchRDDJava.count("reviewText:good AND reviewText:quality"));

// List matching docs
System.err.println("Reviews with good recommendations: ");
SearchRecordJava<Review>[] goodReviews = searchRDDJava.searchList("reviewText:recommend~0.8", 100, 0);
Arrays.stream(goodReviews).forEach(r -> System.err.println(r));

// Pass custom search options
searchRDDJava = new SearchRDDJava<>(reviewRDD,
        SearchOptions.<Review>builder().analyzer(ShingleAnalyzerWrapper.class).build(),
        Review.class);

System.err.println("Reviews from Patosh: ");
searchRDDJava.search("reviewerName:Patrik~0.5", 100, 0)
        .map(SearchRecordJava::getSource)
        .map(Review::getReviewerName)
        .distinct()
        .foreach(r -> System.err.println(r));

System.err.println("Top 10 reviews from same reviewer between computer and software:");
computerReviews.searchJoin(softwareReviews,
        r -> String.format("reviewerName:\"%s\"~0.4", r.reviewerName.replaceAll("[\"]", " ")), 10, 3)
        .filter(matches -> matches.hits.length > 0)
        .map(sameReviewerMatches -> String.format("Reviewer:%s reviews computer %s and software %s (score on names matching are %s)",
                sameReviewerMatches.doc.reviewerName,
                sameReviewerMatches.doc.asin,
                Arrays.stream(sameReviewerMatches.hits).map(h -> h.source.asin).collect(toList()),
                Arrays.stream(sameReviewerMatches.hits).map(h -> h.source.reviewerName + ":" + h.score).collect(toList())
        ))
        .foreach(matches -> System.err.println(matches));


```
See [Examples](examples/src/main/java/all/examples/org/apache/spark/search/rdd/SearchRDDJavaExamples.java)  and [Documentation](core/src/main/java/org/apache/spark/search/rdd/ISearchRDDJava.java)for more details.

* Python
```python
from pyspark import SparkContext
import pysparksearch

data = [{"firstName": "Geoorge", "lastName": "Michael"},
         {"firstName": "Bob", "lastName": "Marley"},
         {"firstName": "Agnès", "lastName": "Bartoll"}]

sc = SparkContext()

sc.parallelize(data).count("firstName:agnes~")
```


### Dataset/DataFrame API (In progress)
* Scala
```scala
import org.apache.spark.search.sql._

val sentences = spark.read.csv("...")
sentences.count("sentence:happy OR sentence:best or sentence:good")

// coming soon: SearchSparkStrategy/LogicPlan & column enhanced with search
sentences.where($"sentence".matches($"searchKeyword" ))
```


## Benchmark

All [benchmarks](benchmark) run under AWS EMR with 3 Spark workers EC2 m5.xlarge and/or 3 r5.large.elasticsearch data nodes for AWS Elasticsearch.
The general use cases is to match company names against two data sets (7M vs 600K rows)

| Feature | SearchRDD | Elasticsearch Hadoop |  LuceneRDD | Spark regex matches (no score) |
|---|---|---|---|---|
| Index + Count matches | 51s | 486s (*)  | 400s | 12s  |
| Index + Entity matching | 128s | 719s (*) | 597s | NA (>1h) |

*DISCLAIMER* Benchmarks methodology or related results may improve, feel free to submit a pull request.
 
(*) Results of elasticsearch hadoop benchmark must be carefully reviewed, contribution welcomed

## Release notes

##### v0.1.7

* Enable caching of search index rdd only for yarn cluster, and as an option.
* Remove scala binary version in parent module artifact name
* Expose SearchRDD as a public API to ease Dataset binding and hdfs reloading
* Add join full text search based on the SearchJavaRDD API
* Fix SearchJavaRDD API and examples

##### v0.1.6
* Switch to multi modules build: core, sql, examples, benchmark
* Improve the github build with running examples against a spark cluster in docker
* Improve licence header checking
* RDD lineage works the same on all DAG Scheduler (Yarn/Standalone): SearchIndexRDD computes zipped index per partition for the next rdd
* CI tests examples under Yarn and Standalone cluster mode
* Fix default field where not used under certain circumstances

##### v0.1.5
* Fix SearchRDD#searchDropDuplicate method
* Save/Restore search RDD to/from HDF
* Yarn support and tested over AWS EMR
* Adding and running [benchmark](benchmark) examples with alternatives libraries on AWS EMR
* Support of spark 3.0.0

##### v0.1.4
* Optimize searchJoin for small num partition

##### v0.1.3
* Fix searchJoin on multiple partitions

##### v0.1.2 
* Released to maven central

##### v0.1.1
* First stable version of the Scala Spark Search RDD
* Support of `SearchRDD#searchJoin(RDD, S => String)` - join 2 RDD by matching queries
* Support of `SearchRDD#dropDuplicates(S => String)` - deduplicate an RDD based on matching query

##### v0.1.0
* Support of `SearchRDD#count(String)` -  count matching hits
* Support of `SearchRDD#searchList(String)` - search matching records as list
* Support of `SearchRDD#search(String)` - search matching records as RDD

## Installation Spark Search
** Maven
```xml
<dependency>
  <groupId>io.github.phymbert</groupId>
  <artifactId>spark-search_2.12</artifactId>
  <version>${spark.search.version}</version>
</dependency>
```

*** Gradle
```groovy
implementation 'io.github.phymbert:spark-search_2.12:$sparkSearchVersion'
```

## Building Spark Search
```shell script
git clone https://github.com/phymbert/spark-search.git
cd spark-search
mvn clean verify
```

## Known alternatives

* [Spark LuceneRDD](https://github.com/zouzias/spark-lucenerdd)
* [Elasticsearch Hadoop](https://github.com/elastic/elasticsearch-hadoop)
* [Spark ML TF-IDF](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/TFIDFExample.scala)
