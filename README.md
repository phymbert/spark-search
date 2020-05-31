# [Spark Search](https://github.com/phymbert/spark-search)

[![CI](https://github.com/phymbert/spark-search/workflows/build-package/badge.svg?branch=branch-0.1)](https://github.com/phymbert/spark-search/actions)
[![version](https://img.shields.io/github/tag/phymbert/spark-search.svg)](https://github.com/phymbert/spark-search/releases/latest)
[![license](https://img.shields.io/github/license/phymbert/spark-search.svg)](LICENSE)
[![LoC](https://tokei.rs/b1/github/phymbert/spark-search?category=lines)](https://github.com/phymbert/spark-search)
[![codecov](https://codecov.io/gh/phymbert/spark-search/branch/branch-0.1/graph/badge.svg)](https://codecov.io/gh/phymbert/spark-search)

Spark Search brings advanced full text search features to RDD and Dataset, powered by Apache Lucene.

## Getting started

### Add as maven dependency

* Example for Spark 2.4.5 and scala 2.12
```xml
<dependency>
    <groupId>spark-search</groupId>
    <artifactId>spark-search_2.12</artifactId>
    <version>0.1-SNAPSHOT_2.4.5</version>
</dependency>
```

### Dataset API

* Scala
```scala
import org.apache.spark.search.sql._

```

### RDD API
* Scala

```scala
import org.apache.spark.search.rdd._

// Amazon review data type
case class Review(asin: String, helpful: Array[Long], overall: Double,
                reviewText: String, reviewTime: String, reviewerID: String,
                reviewerName: String, summary: String, unixReviewTime: Long)

// Amazon computers review
val computersReviewsRDD = spark.read.json("amazon_reviews_computers.json.gz").as[Review].rdd.cache
// Number of partition is the number of Lucene index which will be created across your cluster
.repartition(4)
println(s"${computersReviewsRDD.count} amazon computers reviews loaded, indexing...")

// Count positive review: indexation + count matched doc
val happyReview = computersReviewsRDD.count("reviewText:happy OR reviewText:best or reviewText:good")
println(s"${happyReview} positive reviews :)")

// Search for key words
println(s"Full text search results:")
computersReviewsRDD.search("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 100)
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
println("All reviews speaking about hardware:")
searchRDD.search("(RAM or memory) and (CPU or processor)^4", 10).foreach(println)

// Fuzzy matching
println("Some typo in names:")
searchRDD.search("reviewerName:Mikey~0.8 or reviewerName:Wiliam~0.4 or reviewerName:jonh~0.2", 100)
  .map(doc => (doc.getSource.reviewerName, doc.getScore))
  .foreach(println)
```
See [Examples](src/test/scala/SearchRDDExamples.scala) for more details.

* Java
```java
import org.apache.spark.search.rdd.*;


```

** Outputs
``
8103 positive reviews :)
Full text search results:

SearchRecord{id=313, partitionIndex=1, score=12.883961, shardIndex=0, source=Review(B0002F8SOS,[J@10166230,4.0,I purchased this item to improve the graphical preformance of my relatively old Dell Dimension computer. In World of Warcraft and Civilization IV there was a marked graphical improvement, and the pixel blurring that had been bothering me was eliminated. Unfortunately the computer has begin to fail, not surprising due to it's age, and so it looks like this card will be finding a new home soon.A very solid purchase, though I imagine it will be pretty out of date within a year or two.,05 12, 2007,ARFJMOVDG3ZT4,Ryan W. Mcgee "Caffeinated Writer",Definite Improvement,1178928000)}
SearchRecord{id=306, partitionIndex=2, score=7.080506, shardIndex=0, source=Review(B0002F8SOS,[J@7221539,5.0,This card is great for the $50 I spent on it. Granted it's not the top of the line SLI format or anything, but it plays World of Warcraft with all the setting maxed out.Great buy and recommend for anyone that wants to upgrade their computer for cheap.,09 19, 2005,A74QI357VEH5V,Jerred A. Chute,eVGA 256MB vid card,1127088000)}
SearchRecord{id=2901, partitionIndex=1, score=5.791459, shardIndex=0, source=Review(B0048G3Y9Q,[J@10166230,5.0,I just have to say wow wow wow great headset was sold as refurbished but when I recived it the only thing with wear on it was the box. It works and sounds great. I have wanted this headset for years and the price has always been out of my range. Well at 79.99 I grabed it fast. Love it with it on its like your part of the game.Refurbished Creative Sound Blaster World Of Warcraft Wireless Gaming Headset powered by THX TruStudio PC,12 30, 2011,A1ERRMFYL8XN6Q,Ec,Creative soundblaster wireless world of warcraft gaming headset,1325203200)}
SearchRecord{id=1303, partitionIndex=2, score=5.7171645, shardIndex=0, source=Review(B001B569YY,[J@7221539,1.0,While it is a good basic pc it is often marketed as a gaming PC which is fine, unless you want to play World of Warcraft. It is knows to Blizzard this model of Dells has a BIG problem playing the game and even more well known to gamers who, like myself own one.It does not patch correctly and the motherboard causes 132 and 134 errors which crash wow and strand characters.BUYER BEWARE! here is the link to Blizzard own forums siting Dell's PC as the only cause.[...],03 31, 2010,A196FF4SGYHDY3,K. M. GUZMANN,Can't Play WoW,1269993600)}


All reviews speaking about hardware:
SearchRecord{id=4799, partitionIndex=3, score=6.9683814, shardIndex=0, source=Review(B005XU6C54,[J@333a44f2,5.0,Excellent laptop, it has a large memory ram an a very fast processor core i3, it looks very nice and elegant, his keyboard looks very modern and confortable.Im very happy with this product, i do highly recommend this dell laptop, the best prize ever!!!,12 17, 2011,A2HWNK6EDQQMWK,Gustavo Mendez (Costa Rica),Great laptop ofr the prize!!!,1324080000)}
SearchRecord{id=4201, partitionIndex=0, score=6.929863, shardIndex=0, source=Review(B0056EW4A4,[J@2c668c2a,5.0,Completely satisfied. I have a lot of time using laptops as desktop CPU, connecting them all peripherals. And if I need to move, I have the intrinsic mobility of a laptop. I needed to replace my current CPU a Dell Inspiron 6400. I chose a ASUS N56VJ-WH71 15.6&#34; Laptop looking for a good processor, and the ability to increase the ram memory, up to 16 gb ... I placed this adapter in place of the optical drive to place the hard drive that originally brings. While I installed an SSD and the result is amazing. Using Acronis Home to clone the hard drive, Windows 8 really does fly with this configuration ... This device brings its screws and allowed me putting on the front piece original optical drive in the laptop. For the price I receive more than I expected. Yes .. I recommend it!,05 10, 2013,AFPN6Y6Y87E3H,Karel Martnez Bebert,Yes .. I recommend it!,1368144000)}
SearchRecord{id=216, partitionIndex=1, score=6.5214305, shardIndex=0, source=Review(B000083GMD,[J@6efd0a6e,5.0,This is the second laptop I bought. I had a wonderful experience with this laptop, and no problem with it at all! I don't have an overheating problem with it and I don't have a fan problem with it. The fan just gets a bit louder when I load an application that uses a lot of memory or CPU. It is really fast and I upgraded the ram to 1 GB. It's perfect for anything!,09 27, 2007,A3N6INLUEHI7XG,Kam Chan,The best laptop I've ever had.,1190851200)}
...

Some typo in names:
(John Williams,3.5105515)
(William Wong,2.9425092)
(John H. Williams,2.9341404)
(William,2.4845433)
(William,2.4845433)
(William Jorth "Bill",2.478262)
(william,2.470595)
(William,2.4626882)
(William,2.4276302)
(John "John",2.1424108)
(Joanne S. Jones,2.106685)
...
``


## Building Spark Search

## Contribute

## Roadmap


