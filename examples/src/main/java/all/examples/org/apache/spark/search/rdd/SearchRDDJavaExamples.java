/*
 * Copyright © 2020 Spark Search (The Spark Search Contributors)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package all.examples.org.apache.spark.search.rdd;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.search.SearchOptions;
import org.apache.spark.search.SearchRecordJava;
import org.apache.spark.search.rdd.SearchRDDJava;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.*;
import java.net.URL;
import java.util.Arrays;

import static java.util.stream.Collectors.toList;

/**
 * Spark Search RDD Java examples.
 */
public class SearchRDDJavaExamples {

    public static void main(String[] args) throws Exception {
        System.err.println("Downloading computer reviews...");

        SparkSession spark = SparkSession.builder()
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        System.err.println("Loading reviews...");
        JavaRDD<Review> reviewsRDD = loadReviewRDD(spark, "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz");

        // Create the SearchRDD based on the JavaRDD to enjoy search features
        SearchRDDJava<Review> computerReviews = SearchRDDJava.of(reviewsRDD, Review.class);

        // Count matching docs
        System.err.println("Computer reviews with good recommendations: "
                + computerReviews.count("reviewText:good AND reviewText:quality"));

        // List matching docs
        System.err.println("Reviews with good recommendations and fuzzy: ");
        SearchRecordJava<Review>[] goodReviews = computerReviews
                .searchList("reviewText:recommend~0.8", 10, 0);
        Arrays.stream(goodReviews).forEach(r -> System.err.println(r));

        // Pass custom search options
        computerReviews = SearchRDDJava.<Review>builder()
                .rdd(reviewsRDD)
                .runtimeClass(Review.class)
                .options(SearchOptions.<Review>builder().analyzer(ShingleAnalyzerWrapper.class).build())
                .build();

        System.err.println("Top 100 reviews from Patosh with fuzzy with 0.5 minimum score:");
        computerReviews.search("reviewerName:Patrik~0.5", 100, 0.5)
                .map(SearchRecordJava::getSource)
                .map(Review::getReviewerName)
                .distinct()
                .collect()
                .forEach(r -> System.err.println(r));

        System.err.println("Loading software reviews...");
        JavaRDD<Review> softwareReviews = loadReviewRDD(spark, "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz");

        System.err.println("Top 10 reviews from same reviewer between computer and software:");
        computerReviews.matches(softwareReviews.filter(r -> r.reviewerName != null && !r.reviewerName.isEmpty())
                                .mapToPair(sr -> new Tuple2<String, Review>(sr.asin, sr)),
                        r -> String.format("reviewerName:\"%s\"~0.4", r.reviewerName.replaceAll("[\"]", " ")), 10, 0)
                .values()
                .filter(matches -> matches._2.length > 0)
                .map(sameReviewerMatches -> String.format("Reviewer:%s reviews computer %s and software %s (score on names matching are %s)",
                        sameReviewerMatches._1.reviewerName,
                        sameReviewerMatches._1.asin,
                        Arrays.stream(sameReviewerMatches._2).map(h -> h.source.asin).collect(toList()),
                        Arrays.stream(sameReviewerMatches._2).map(h -> h.source.reviewerName + ":" + h.score).collect(toList())
                ))
                .take(10)
                .forEach(matches -> System.err.println(matches));

        // Save and search reload example - target path must not exist
        FileSystem.get(new Configuration()).delete(new Path("/hdfs-tmp/hdfs-pathname"), true);
        SearchRDDJava.of(softwareReviews.repartition(8), Review.class)
                .save("/hdfs-tmp/hdfs-pathname");
        SearchRDDJava<Review> restoredSearchRDD = SearchRDDJava
                .load(sc, "/hdfs-tmp/hdfs-pathname", Review.class);
        System.err.println("Software reviews with good recommendations: "
                + restoredSearchRDD.count("reviewText:good AND reviewText:quality"));

        // Reloaded index can be used as classical RDD
        Tuple2<String, Review> longestReview = restoredSearchRDD.javaRDD()
                .filter(t -> t.reviewText != null)
                .map(r -> new Tuple2<>(r.reviewerID, r))
                .sortBy(t -> t._2.reviewText.length(), false, 2)
                .take(1).get(0);
        System.err.printf("Longest review %s has %d chars and has been submitted by %s%n",
                longestReview._1,
                longestReview._2.reviewText.length(),
                longestReview._2.reviewerName);

        spark.stop();
    }

    private static JavaRDD<Review> loadReviewRDD(SparkSession spark, String reviewURL) throws IOException {
        File reviews = loadReview(reviewURL);

        Configuration hadoopConf = new Configuration();
        FileSystem hdfs = FileSystem.get(hadoopConf);
        String dstPathName = "/hdfs-tmp/reviews.json.gz";
        Path dst = new Path(dstPathName);
        hdfs.copyFromLocalFile(new Path(reviews.getAbsolutePath()), dst);
        hdfs.deleteOnExit(dst);

        return spark.read().json(dstPathName)
                .as(Encoders.bean(Review.class))
                .repartition(2).javaRDD().cache();
    }

    private static File loadReview(String reviewURL) throws IOException {
        File reviews = File.createTempFile("reviews", "json.gz");
        reviews.deleteOnExit();
        URL reviewsURL = new URL(reviewURL);
        try (InputStream is = reviewsURL.openStream()) {
            try (FileOutputStream fos = new FileOutputStream(reviews)) {
                IOUtils.copy(is, fos);
            }
        }
        return reviews;
    }

    public static class Review implements Serializable {
        private static final long serialVersionUID = 1L;
        private String asin;
        private Long[] helpful;
        private Double overall;
        private String reviewText;
        private String reviewTime;
        private String reviewerID;
        private String reviewerName;
        private String summary;
        private Long unixReviewTime;

        public String getAsin() {
            return asin;
        }

        public void setAsin(String asin) {
            this.asin = asin;
        }

        public Long[] getHelpful() {
            return helpful;
        }

        public void setHelpful(Long[] helpful) {
            this.helpful = helpful;
        }

        public Double getOverall() {
            return overall;
        }

        public void setOverall(Double overall) {
            this.overall = overall;
        }

        public String getReviewText() {
            return reviewText;
        }

        public void setReviewText(String reviewText) {
            this.reviewText = reviewText;
        }

        public String getReviewTime() {
            return reviewTime;
        }

        public void setReviewTime(String reviewTime) {
            this.reviewTime = reviewTime;
        }

        public String getReviewerID() {
            return reviewerID;
        }

        public void setReviewerID(String reviewerID) {
            this.reviewerID = reviewerID;
        }

        public String getReviewerName() {
            return reviewerName;
        }

        public void setReviewerName(String reviewerName) {
            this.reviewerName = reviewerName;
        }

        public String getSummary() {
            return summary;
        }

        public void setSummary(String summary) {
            this.summary = summary;
        }

        public Long getUnixReviewTime() {
            return unixReviewTime;
        }

        public void setUnixReviewTime(Long unixReviewTime) {
            this.unixReviewTime = unixReviewTime;
        }

        @Override
        public String toString() {
            return "Review{" +
                    "asin='" + asin + '\'' +
                    ", helpful=" + Arrays.toString(helpful) +
                    ", overall=" + overall +
                    ", reviewText='" + reviewText + '\'' +
                    ", reviewTime='" + reviewTime + '\'' +
                    ", reviewerID='" + reviewerID + '\'' +
                    ", reviewerName='" + reviewerName + '\'' +
                    ", summary='" + summary + '\'' +
                    ", unixReviewTime=" + unixReviewTime +
                    '}';
        }
    }
}
