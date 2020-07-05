FROM bde2020/spark-worker:2.4.5-hadoop2.7 as spark

FROM bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8

COPY --from=spark /spark /spark

ADD spark-defaults.conf /spark/conf