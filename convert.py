import argparse
from converter import Converter


def main(*args, **kwargs):
    ct = Converter(**kwargs)
    print(ct.head())
    print(ct.take(10))
    ct.write()
    if ct.validate():
        print("convert successed!")
    else:
        raise ValueError("Convert faild!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=main)
    parser.add_argument('--input', type=str, required=True, metavar='input file')
    parser.add_argument('--output', type=str, required=True, metavar='output file')
    parser.add_argument('--mode', type=str, required=False, default = 'error', metavar='if file exists')    

    args = parser.parse_args()
    args.func(**vars(args))
