import unittest

import pysparksearch
from pyspark import SparkContext


class RDDTestCase(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.data = [{"Geoorge", "Michael", 53},
                     {"Bob", "Marley", 37},
                     {"Agnès", "Bartoll", -1}]

    def test_count(self):
        self.assertEqual(2, self.sc.parallelize(self.data).searchcount("firstName:bob"))

        if __name__ == '__main__':
            unittest.main()
