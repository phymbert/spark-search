# [Spark Search](https://github.com/phymbert/spark-search)

[![CI](https://github.com/phymbert/spark-search/workflows/build-package/badge.svg?branch=branch-0.1)](https://github.com/phymbert/spark-search/actions)
[![version](https://img.shields.io/github/tag/phymbert/spark-search.svg)](https://github.com/phymbert/spark-search/releases/latest)
[![license](https://img.shields.io/github/license/phymbert/spark-search.svg)](LICENSE)
[![LoC](https://tokei.rs/b1/github/phymbert/spark-search?category=lines)](https://github.com/phymbert/spark-search)
[![codecov](https://codecov.io/gh/phymbert/spark-search/branch/branch-0.1/graph/badge.svg)](https://codecov.io/gh/phymbert/spark-search)

Spark Search brings advanced full text search features to RDD and Dataset, powered by Apache Lucene.

## Getting started

### Add as maven dependency

```xml
<dependency>
    <groupId>org.phymbert.spark</groupId>
    <artifactId>spark-search_2.12</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Dataset API

* Scala
```scala
import org.apache.spark.search.sql._
// Coming soon
// Might look like
ds.join(other, $"firstName" =~ other("firstName") && $"lastName" =~ other("lastName").boost(3) && $"age" === other("age"))
```

### RDD API
* Scala

```scala
import org.apache.spark.search.rdd._

case class Review(asin: String, helpful: Array[Long], overall: Double,
                  reviewText: String, reviewTime: String, reviewerID: String,
                  reviewerName: String, summary: String, unixReviewTime: Long)

val computersReviewsRDD = spark.read.json("...").as[Review].rdd
// Number of partition is the number of Lucene index which will be created across your cluster
.repartition(4)

// Count positive review: indexation + count matched doc
computersReviewsRDD.count("reviewText:happy OR reviewText:best or reviewText:good")

// Search for key words
computersReviewsRDD.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 100)
  .foreach(println)

// /!\ Important lucene indexation is done each time a SearchRDD is computed,
// if you do multiple operations on the same parent RDD, you might a variable in the driver:
val searchRDD = computersReviewsRDD.searchRDD(
  SearchRDDOptions.builder[Review]() // See all other options SearchRDDOptions, IndexationOptions and ReaderOptions
    .readerOptions(ReaderOptions.builder()
      .defaultFieldName("reviewText")
      .build())
    .analyzer(classOf[EnglishAnalyzer])
    .build())

// Boolean queries and boosting
searchRDD.searchList("(RAM or memory) and (CPU or processor)^4", 10).foreach(println)

// Fuzzy matching
searchRDD.searchList("reviewerName:Mikey~0.8 or reviewerName:Wiliam~0.4 or reviewerName:jonh~0.2", 100)
  .map(doc => (doc.getSource.reviewerName, doc.getScore))
  .foreach(println)

// RDD full text matching
val softwareReviewsRDD = spark.read.json("...").as[Review].rdd
val matchesRDD = searchRDD.matching(softwareReviewsRDD, (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName + "\""}~8", 10)
val matchesReviewersRDD = searchRDD.matching(softwareReviewsRDD, (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName + "\""}~8", 10)
matchesReviewersRDD
  .filter(!_.getHits.isEmpty)
  .map(m => (m.getDoc.reviewerName, m.getHits.asScala.map(h => (h.getSource.reviewerName, h.getScore))))
  .foreach(println)

```
_Outputs_
```text
8103 positive reviews :)
Full text search results:

SearchRecord{id=313, partitionIndex=1, score=12.883961, shardIndex=0, source=Review(B0002F8SOS,[J@10166230,4.0,I purchased this item to improve the graphical preformance of my relatively old Dell Dimension computer. In World of Warcraft and Civilization IV there was a marked graphical improvement, and the pixel blurring that had been bothering me was eliminated. Unfortunately the computer has begin to fail, not surprising due to it's age, and so it looks like this card will be finding a new home soon.A very solid purchase, though I imagine it will be pretty out of date within a year or two.,05 12, 2007,ARFJMOVDG3ZT4,Ryan W. Mcgee "Caffeinated Writer",Definite Improvement,1178928000)}
SearchRecord{id=306, partitionIndex=2, score=7.080506, shardIndex=0, source=Review(B0002F8SOS,[J@7221539,5.0,This card is great for the $50 I spent on it. Granted it's not the top of the line SLI format or anything, but it plays World of Warcraft with all the setting maxed out.Great buy and recommend for anyone that wants to upgrade their computer for cheap.,09 19, 2005,A74QI357VEH5V,Jerred A. Chute,eVGA 256MB vid card,1127088000)}
...

All reviews speaking about hardware:
SearchRecord{id=4799, partitionIndex=3, score=6.9683814, shardIndex=0, source=Review(B005XU6C54,[J@333a44f2,5.0,Excellent laptop, it has a large memory ram an a very fast processor core i3, it looks very nice and elegant, his keyboard looks very modern and confortable.Im very happy with this product, i do highly recommend this dell laptop, the best prize ever!!!,12 17, 2011,A2HWNK6EDQQMWK,Gustavo Mendez (Costa Rica),Great laptop ofr the prize!!!,1324080000)}
SearchRecord{id=4201, partitionIndex=0, score=6.929863, shardIndex=0, source=Review(B0056EW4A4,[J@2c668c2a,5.0,Completely satisfied. I have a lot of time using laptops as desktop CPU, connecting them all peripherals. And if I need to move, I have the intrinsic mobility of a laptop. I needed to replace my current CPU a Dell Inspiron 6400. I chose a ASUS N56VJ-WH71 15.6&#34; Laptop looking for a good processor, and the ability to increase the ram memory, up to 16 gb ... I placed this adapter in place of the optical drive to place the hard drive that originally brings. While I installed an SSD and the result is amazing. Using Acronis Home to clone the hard drive, Windows 8 really does fly with this configuration ... This device brings its screws and allowed me putting on the front piece original optical drive in the laptop. For the price I receive more than I expected. Yes .. I recommend it!,05 10, 2013,AFPN6Y6Y87E3H,Karel Martnez Bebert,Yes .. I recommend it!,1368144000)}
...

Some typo in names:
(John Williams,3.5105515)
(William Wong,2.9425092)
(John H. Williams,2.9341404)
...
(John "John",2.1424108)
(Joanne S. Jones,2.106685)
...

(Patrick Holt "txdragon",ArrayBuffer((Patrick Holt "txdragon",8.627737)))
(Marla,ArrayBuffer((Marla Robles,3.8126698)))
(Jerry Jackson Jr.,ArrayBuffer((Jerry Jackson Jr.,7.2705545)))
(G. Cox "Shanghaied",ArrayBuffer((G. Cox "Shanghaied",7.6521616), (G. Cox "Shanghaied",7.6521616)))
(Wildness,ArrayBuffer((Wilde,4.3258367), (Wild Rice,3.4275503), (andrew wilds,3.4275503), (Leslie L. Fraser "wild ting",2.3578525)))
(Lynn,ArrayBuffer((Lynn,3.7590394), (Huey Lynn,3.324419), (Lynn May,3.3234725), (Beth Lynn,3.3234725), (Lynn Marx,3.3234725), (Lynne McCauley,3.227646), (Lynn Babish,3.227646), (lynn markwell,2.978452), (Lynn Barton,2.978452), (Lynn Fleming,2.978452)))
(Gearhead Mania,ArrayBuffer((Gearhead Mania,7.617262)))
(Mark B,ArrayBuffer((Mark B,4.2918057), (Mark B.,4.088337), (Mark B Greene,3.583696), (Mark B Denver "Mark",3.3450594), (Mark B. Hancock "Jonmarsh",2.8886466)))
(Ace,ArrayBuffer((Ace,4.185837), (Ace,4.185837), (Aces and 8s,3.5885944), (Marijee "AC",3.5885944), (AC "A. Chughtai",3.3180013), (ACE (DSTRYALLORNOTNG),3.3180013), (David H. Thompson "ac",2.691018)))
```

See [Examples](src/test/scala/SearchRDDExamples.scala) for more details.

* Java
```java
import org.apache.spark.search.rdd.*;

JavaRDD<Review> reviewRDD = sqlContext.read().json(...).as(Encoders.bean(Review.class)).repartition(2).javaRDD().cache();
SearchRDDJava<Review> searchRDDJava = new SearchRDDJava<>(reviewRDD);

// Count matching docs
searchRDDJava.count("reviewText:good AND reviewText:quality")

// List matching docs
searchRDDJava.searchList("reviewText:recommend~0.8", 100).forEach(System.out::println);

// Pass custom search options
searchRDDJava = new SearchRDDJava<>(reviewRDD,
        SearchRDDOptions.<Review>builder().analyzer(ShingleAnalyzerWrapper.class).build());

searchRDDJava.searchList("reviewerName:Patrik", 100)
        .stream()
        .map(SearchRecord::getSource)
        .map(Review::getReviewerName)
        .distinct()
        .forEach(System.out::println);
```


_Outputs_
```text
Reviews with good recommendations: 1246
SearchRecord{id=11260, partitionIndex=0, score=2.2550306, shardIndex=0, source=Review{asin='B001A647ZW', helpful=[0], overall=2.0, reviewText='Limited to "great idea," very top heavy and semi productive.  I do live in highly populated mosquito area, but only partially worked for up to 5 or 6 ft and totally quiting after one year.  Would not recommend, but do recommned their wall pest control.  Have had them working perfectly for 7 years in house and garage.', reviewTime='01 2, 2013', reviewerID='A2MQPSQDWTPW2N', reviewerName='joy selph', summary='Mosquito Trap', unixReviewTime=1357084800}}
SearchRecord{id=8399, partitionIndex=1, score=1.7722454, shardIndex=0, source=Review{asin='B004S43T2K', helpful=[0], overall=5.0, reviewText='Great Case, love it recommend it to anyone, easy handling and the stand is great!!! Love it! Recommend it to everyone', reviewTime='05 16, 2012', reviewerID='A3SQWPJRJ81S49', reviewerName='Litelady', summary='Excellent', unixReviewTime=1337126400}}
SearchRecord{id=6338, partitionIndex=0, score=1.7698762, shardIndex=0, source=Review{asin='B006RL1H5I', helpful=[0], overall=2.0, reviewText='Don't recommend...but after having an IPad for 3 months I wouldn't recommend those either..lightweight..does not hold up iPad..topples right over', reviewTime='11 20, 2013', reviewerID='A1TQ5Y60KGAQGE', reviewerName='Mandy', summary='Light weight..not sturdy', unixReviewTime=1384905600}}
...
Patrick
patrick
Patrick Blackwelder "Patrick Black"
Patrick H.
Patrick Mcfarlin
Patrick Hauver
Patrice
...
```

See [Examples](src/test/java/SearchRDDJavaExamples.java) for more details.


## Building Spark Search
```shell script
git clone https://github.com/phymbert/spark-search.git
cd spark-search
mvn clean verify
```

## Contribute

## Roadmap


