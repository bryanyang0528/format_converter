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

    def __init__(self, input, output, out_format, mode):
        self.setUp()
        self.df = self.sqlCtx.read.csv(input,header=True)
        self.output = output
        self.out_format = out_format
        self.mode = mode

    def getMaster(self):
        return os.getenv('SPARK_MASTER')

    def setUp(self):
        """Setup a basic Spark context for testing"""
        #conf = SparkConf().setMaster(self.getMaster())
        self.sc = SparkContext(self.getMaster())
        quiet_py4j()
        try:
            from pyspark.sql import SparkSession
            self.sqlCtx = SparkSession.builder.getOrCreate()
        except:
            self.sqlCtx = SQLContext(self.sc)

    def tearDown(self):
        """
        Tear down the basic panda spark test case. This stops the running
        context and does a hack to prevent Akka rebinding on the same port.
        """
        self.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        self.sc._jvm.System.clearProperty("spark.driver.port")
 
    def head(self):
        return self.df.head

    def take(self, n):
        return self.df.take(n)

    def write(self):
        self.df.write.format(self.out_format).save(self.output, mode = self.mode)

    def validate(self):
        df_out = self.df = self.sqlCtx.read.format(self.out_format).load(self.output)
        df_out_cnt = df_out.count()
        df_cnt = self.df.count()
        return df_cnt == df_out_cnt
