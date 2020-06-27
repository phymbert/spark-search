## [Spark Search](https://github.com/phymbert/spark-search)

[![CI](https://github.com/phymbert/spark-search/workflows/CI/badge.svg)](https://github.com/phymbert/spark-search/actions)
[![version](https://img.shields.io/github/tag/phymbert/spark-search.svg)](https://github.com/phymbert/spark-search/releases/latest)
[![license](https://img.shields.io/github/license/phymbert/spark-search.svg)](LICENSE)
[![LoC](https://tokei.rs/b1/github/phymbert/spark-search?category=lines)](https://github.com/phymbert/spark-search)

[Spark](https://spark.apache.org/) Search brings advanced full text search features to your Dataframe, Dataset and RDD. Powered by [Apache Lucene](https://lucene.apache.org/).

## Context
Let's image you have a billion records dataset you want to query on and match against another one using full text search...
You do not expect an external datasource or database system than Spark, and of course with the best performances.
Spark Search fits your needs: it builds for all parent RDD partitions a one-2-one volatile Lucene index available
 during the lifecycle of your spark session across your executors local directories and RAM.
Strongly typed, Spark Search API plans to support Java, Scala and Python Spark SQL, Dataset and RDD SDKs.
Have a look and feel free to contribute!

## Getting started

### Maven dependency

```xml
<dependency>
    <groupId>io.github.phymbert</groupId>
    <artifactId>spark-search_${scala.binary.version}</artifactId>
    <version>0.1.5</version>
</dependency>
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

### RDD API
* Scala

```scala
import org.apache.spark.search.rdd._

val computersReviewsRDD = sc.parallelize(Seq(Review("AAAAA", Array(3, 3), 3.0, "Ok, this is a good computer to play Civilization IV or World of Warcraft", "11 29, 2010", "XXXXX", "Patrick H.", "Ok for an average user, but not much else.", 1290988800)))
// Number of partition is the number of Lucene index which will be created across your cluster
.repartition(4)

// Count positive review: indexation + count matched doc
computersReviewsRDD.count("reviewText:happy OR reviewText:best or reviewText:good")

// Search for key words
computersReviewsRDD.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 100)
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
computersReviewsSearchRDD.searchList("reviewerName:Mikey~0.8 or reviewerName:Wiliam~0.4 or reviewerName:jonh~0.2", 100)
  .map(doc => (doc.getSource.reviewerName, doc.getScore))
  .foreach(println)

// RDD full text joining
val softwareReviewsRDD = sc.parallelize(Seq(Review("BBBB", Array(1), 4.0, "I use this and Ulead video studio 11.", "09 17, 2008", "YYYY", "Patrick Holtt", "Great, easy to use and user friendly.", 1221609600)))
val matchesRDD = searchRDD.searchJoin(softwareReviewsRDD, (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName + "\""}~8", 10)
val matchesReviewersRDD = computersReviewsSearchRDD.searchJoin(softwareReviewsRDD, (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName + "\""}~8", 10)
matchesReviewersRDD
  .filter(_.hits.nonEmpty)
  .map(m => (m.doc.reviewerName, m.hits.map(h => (h.source.reviewerName, h.score))))
  .foreach(println)
```

See [Examples](src/test/scala/org/apache/spark/search/rdd/SearchRDDExamples.scala) for more details.

* Java
```java
import org.apache.spark.search.rdd.*;

JavaRDD<Review> reviewRDD = sqlContext.read().json(...).as(Encoders.bean(Review.class)).repartition(2).javaRDD();
SearchRDDJava<Review> searchRDDJava = new SearchRDDJava<>(reviewRDD);

// Count matching docs
searchRDDJava.count("reviewText:good AND reviewText:quality")

// List matching docs
searchRDDJava.searchList("reviewText:recommend~0.8", 100).forEach(System.out::println);

// Pass custom search options
searchRDDJava = new SearchRDDJava<>(reviewRDD,
        SearchOptions.<Review>builder().analyzer(ShingleAnalyzerWrapper.class).build());

searchRDDJava.searchList("reviewerName:Patrik", 100)
        .stream()
        .map(SearchRecord::getSource)
        .map(Review::getReviewerName)
        .forEach(System.out::println);
```
See [Examples](src/test/java//org/apache/spark/search/rdd/SearchRDDJavaExamples.java) for more details.

## Benchmark

All [benchmarks](benchmark) run under AWS EMR with 3 Spark workers EC2 m5.xlarge and/or 3 r5.large.elasticsearch data nodes for AWS Elasticsearch.
The general use cases is to match company names against two data sets (7M vs 600K rows)

| Feature | SearchRDD | Elasticsearch Hadoop |  LuceneRDD | Spark regex matches (no score) |
|---|---|---|---|---|
| Index + Count matches | 42s |   | 367s | 12s  |
| Index + Search matches |  |   |  |   |
| Index + Join matches | 94s | | 1123s | NA (>1h) |
| Index |  | |  |  |
| Count matches only |  | |  |  |
| Search matches only |  | |  |  |
| Join matches only |  | |  |  |

*DISCLAIMER* Benchmarks methodology or related results may improve, feel free to submit a pull request. 

## Release notes

##### v0.1.5
* Fix SearchRDD#searchDropDuplicate method
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
