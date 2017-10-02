#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Provides a common test case base for Python Spark tests"""

from .utils import add_pyspark_path, quiet_py4j
from pyspark.context import SparkContext, SparkConf
import os

class Converter():
    def __init__(self, **kwargs):
        print(kwargs)
        self.setUp()
        self.input = kwargs.get('input')
        self.output = kwargs.get('output')
        self.in_format = self.input.split('.')[-1]
        self.out_format = self.output.split('.')[-1]
        self.mode = kwargs.get('mode', 'overwrite')
        self.compression = kwargs.get('compression', None)
        self.partitionBy = kwargs.get('partitionBy', None)
        if self.in_format == 'csv':
            self.df = self.sqlCtx.read.csv(self.input, header=True)
        elif 'parquet':
            self.df = self.sqlCtx.read.parquet(self.input)
        else:
            raise ValueError('Not support this format of source')

    def getMaster(self):
        return os.getenv('SPARK_MASTER')

    def setUp(self):
        self.sc = SparkContext(self.getMaster())
        quiet_py4j()
        try:
            from pyspark.sql import SparkSession
            self.sqlCtx = SparkSession.builder.getOrCreate()
        except:
            self.sqlCtx = SQLContext(self.sc)

    def tearDown(self):
        self.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        self.sc._jvm.System.clearProperty("spark.driver.port")
 
    def head(self):
        return self.df.head

    def take(self, n):
        return self.df.take(n)

    def write(self):
        if self.out_format == 'csv':
            self.df.write.csv(self.output, mode=self.mode, compression=self.compression, header=True)
        elif self.out_format == 'parquet':
            self.df.write.parquet(self.output, mode=self.mode, compression=self.compression)

    def validate(self):
        df_out = self.df = self.sqlCtx.read.format(self.out_format).load(self.output)
        df_out_cnt = df_out.count()
        df_cnt = self.df.count()
        return df_cnt == df_out_cnt
