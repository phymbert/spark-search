#
# Copyright 2020 the Spark Search contributors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

__all__ = ["SearchRDD"]


class SearchRDD(object):
    """
    A Spark RDD with search features
    """

    def __init__(self, searchRDD):
        self._searchRDD = searchRDD

    def count(self, query):
        return self._searchRDD.count(query)


def searchcount(self, query):
    searchRDD(self.ctx, self).count(query)

def searchRDD(ctx, rdd):
    return ctx._jvm.org.apache.spark.search.rdd.SearchRDDJava.create(rdd)
