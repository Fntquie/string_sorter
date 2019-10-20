import argparse
import os
import random
from functools import partial
from multiprocessing import Pool
from string import ascii_lowercase, ascii_uppercase, punctuation
from typing import Any
from config import BULK_SIZE, LINE_SEPARATOR, N_THREADS


class LargeFileWithStringsGenerator:
    def __init__(self, path_to_file: str, max_string_len: int):
        self._path = path_to_file
        self._max_string_len = max_string_len
        self._symbols_for_string = ascii_lowercase + ascii_uppercase + punctuation

    def _generate_string(self) -> str:
        string_len = random.randint(1, self._max_string_len)
        return ''.join(random.choice(self._symbols_for_string) for _ in range(string_len)) + LINE_SEPARATOR

    def _write_strings(self, iteration: Any, bulk_size: int):
        """
        Writes a single string into file and creates a new line

        :param iteration: any, used for compatibility with partial and Pool.map()
        :param bulk_size: int, number of string to write as a bulk
        :return: None
        """
        open(self._path, 'a').writelines(self._generate_string() for _ in range(bulk_size))

    def generate_file(self, file_size: int, n_threads: int):
        # create or replace file
        if os.path.exists(self._path):
            os.remove(self._path)

        print(f'Generating... ({n_threads} threads used)')

        # run parallel writing to file
        p = Pool(n_threads)
        bulk_size = min(BULK_SIZE // 10, file_size // n_threads)
        p.map(partial(self._write_strings, bulk_size=bulk_size), range(0, file_size, bulk_size))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generates text file of size --file-size with strings of length --max-string-length maximum')
    parser.add_argument('--output-file', dest='output_path', help='Text file to write strings into', type=str)
    parser.add_argument('--file-size', dest='file_size', help='Number of lines in text file', type=int)
    parser.add_argument('--max-string-length', dest='max_string_len', help='Maximal length of string', type=int)
    parser.add_argument('--n-threads', dest='n_threads', help='Level of parallelism', default=N_THREADS, type=int)

    arguments = parser.parse_args()
    generator = LargeFileWithStringsGenerator(arguments.output_path, max_string_len=arguments.max_string_len)
    generator.generate_file(file_size=arguments.file_size, n_threads=arguments.n_threads)
