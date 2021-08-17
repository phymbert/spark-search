#
# Copyright © 2020 Spark Search (The Spark Search Contributors)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM ubuntu:bionic

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64/jre
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR /opt/hadoop/etc/hadoop
ENV SPARK_HOME /opt/spark
ENV PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV HADOOP_VERSION 2.7.0
ENV SPARK_VERSION 2.4.8

RUN apt-get -qq update && \
    apt-get -y -qq upgrade && \
    apt-get -y -qq install wget nano openjdk-8-jdk ssh openssh-server

RUN wget -q -P /tmp/ https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
RUN tar xf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /tmp && \
	mv /tmp/hadoop-${HADOOP_VERSION} /opt/hadoop

RUN wget -q -P /tmp/ https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
RUN tar xf /tmp/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz -C /tmp && \
    mv /tmp/spark-${SPARK_VERSION}-bin-hadoop2.7 ${SPARK_HOME}

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
	cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
	chmod 600 ~/.ssh/authorized_keys
COPY /confs/config /root/.ssh
RUN chmod 600 /root/.ssh/config

COPY /confs/*.xml /opt/hadoop/etc/hadoop/
COPY /confs/slaves /opt/hadoop/etc/hadoop/
COPY /entrypoint-yarn.sh /
COPY /confs/spark-defaults.conf ${SPARK_HOME}/conf
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/environment

ENTRYPOINT ["/bin/bash", "entrypoint-yarn.sh"]