/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.spark.search.rdd;

import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.search.SearchOptions;
import org.apache.spark.search.SearchRecordJava;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

/**
 * Spark Search RDD Java examples.
 */
public class SearchRDDJavaExamples {

    public static void main(String[] args) throws Exception {

        System.out.println("Downloading reviews...");
        File reviews = File.createTempFile("reviews", "json.gz");
        reviews.deleteOnExit();
        URL reviewsURL = new URL("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz");
        try (ReadableByteChannel rbc = Channels.newChannel(reviewsURL.openStream())) {
            try (FileOutputStream fos = new FileOutputStream(reviews)) {
                fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            }
        }

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        System.out.println("Loading reviews...");
        JavaRDD<Review> reviewRDD = spark.read().json(reviews.getAbsolutePath()).as(Encoders.bean(Review.class)).repartition(2).javaRDD().cache();

        //Create the SearchRDD based on the JavaRDD to enjoy search features
        SearchRDDJava<Review> searchRDDJava = new SearchRDDJava<>(reviewRDD);

        // Count matching docs
        System.out.println("Reviews with good recommendations: " + searchRDDJava.count("reviewText:good AND reviewText:quality"));

        // List matching docs
        System.out.println("Reviews with good recommendations: ");
        SearchRecordJava<Review>[] goodReviews = searchRDDJava.searchList("reviewText:recommend~0.8", 100, 0);
        Arrays.stream(goodReviews).forEach(System.out::println);

        // Pass custom search options
        searchRDDJava = new SearchRDDJava<>(reviewRDD,
                SearchOptions.<Review>builder().analyzer(ShingleAnalyzerWrapper.class).build());

        System.out.println("Reviews from Patosh: ");
        searchRDDJava.search("reviewerName:Patrik~0.5", 100, 0)
                .map(SearchRecordJava::getSource)
                .map(Review::getReviewerName)
                .distinct()
                .foreach(System.out::println);

        spark.stop();
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
