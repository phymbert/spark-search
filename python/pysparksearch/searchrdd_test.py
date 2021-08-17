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

import unittest

import pysparksearch
from pyspark import SparkContext


class RDDTestCase(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.data = [{"firstName": "Geoorge", "lastName": "Michael"},
                     {"firstName": "Bob", "lastName": "Marley"},
                     {"firstName": "Agnès", "lastName": "Bartoll"}]

    def test_count(self):
        self.assertEqual(1, self.sc.parallelize(self.data).searchcount("firstName:agnes~"))

        if __name__ == '__main__':
            unittest.main()
