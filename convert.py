import argparse
from converter import Converter


def main(*args, **kwargs):
    input = kwargs.get('in')
    output = kwargs.get('out')
    out_format = kwargs.get('format')
    ct = Converter(input, output)
    print(ct.head())
    print(ct.take(10))
    ct.write(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=main)
    parser.add_argument('--in', type=str, required=True, metavar='input file')
    parser.add_argument('--out', type=str, required=True, metavar='output file')
    parser.add_argument('--format', type=str, required=False, default = 'parquet', metavar='format of source file')
