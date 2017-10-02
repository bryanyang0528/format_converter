import os
import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from shutil import copyfile, copytree, rmtree
from converter.converter import Converter

dir_path = os.path.dirname(os.path.realpath(__file__))

class ConverterTest(SparkTestingBaseTestCase):
    kwargs = {
        'csv':'{}/data/test.csv'.format(dir_path),
        'parquet':'{}/data/test.parquet'.format(dir_path),
        'csv_copy':'{}/data/test_copy.csv'.format(dir_path),
        'parquet_copy':'{}/data/test_copy.parquet'.format(dir_path)
    }

    csv_copy = kwargs.get('csv_copy')
    parquet_copy = kwargs.get('parquet_copy')

    def setUp(self):
        pass

    def tearDown(self):
        try:
            rmtree(self.parquet_copy)
        except Exception:
            pass
        try:
            os.remove(self.csv_copy)
        except Exception:
            pass
        try:
            rmtree(self.parquet_copy)
        except Exception:
            pass
        try:
            rmtree(self.csv_copy)
        except Exception:
            pass

    def test_csv_to_parquet_overwrite(self):
        copyfile(self.kwargs.get('csv'), self.csv_copy)

        ct = Converter(input=self.csv_copy, output=self.parquet_copy, mode='overwrite')
        ct.write()
        df_in = ct.df
        df_out = ct.sqlCtx.read.format(ct.out_format).load(self.parquet_copy)
        self.assertTrue(self.assertRDDEquals(df_in.rdd, df_out.rdd))
        
        ct.tearDown()

    def test_parquet_to_csv_overwrite(self):
        copytree(self.kwargs.get('parquet'), self.parquet_copy)
        
        ct = Converter(input=self.parquet_copy, output=self.csv_copy, mode='overwrite')
        ct.write()
        df_in = ct.df
        df_out = ct.sqlCtx.read.csv(self.csv_copy, header=True)
        self.assertTrue(self.assertRDDEquals(df_in.rdd, df_out.rdd))
        ct.tearDown()
        

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
