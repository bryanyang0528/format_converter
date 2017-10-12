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
        return os.getenv('SPARK_MASTER', 'local[2]')

    def setUp(self):
        self.sc = SparkContext(self.getMaster())
        #quiet_py4j()
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
