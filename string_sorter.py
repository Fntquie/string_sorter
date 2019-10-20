import argparse
import os
import uuid
from itertools import islice
from multiprocessing import Pool
from typing import Any, IO, List
from config import BULK_SIZE, LINE_SEPARATOR, N_THREADS, TEMP_DIR


class TempFile:
    root_directory = TEMP_DIR

    def __init__(self):
        self._name = uuid.uuid4().hex
        self._start = 0
        self._size = 0
        self._fin = None
        self._current_value = None

    @property
    def path(self) -> str:
        return os.path.join(self.root_directory, self._name)

    @property
    def size(self) -> int:
        return self._size - self._start

    @size.setter
    def size(self, value: int):
        self._size = value

    def _create_or_replace(self):
        os.remove(self.path)
        open(self._name, 'w').close()

    def write_bulk(self, lines: List[str]):
        with open(self.path, 'w') as f:
            f.writelines(sorted(lines))

        self.size = len(lines)

    def write(self, string: str):
        with open(self.path, 'a') as f:
            f.write(string + LINE_SEPARATOR)

        self.size += 1

    def readline(self) -> str or None:
        if self._current_value is not None:
            return self._current_value
        if self._fin is None:
            self._fin = open(self.path, 'r')
        if self.size == 0:
            self._fin.close()
            self._fin = None
            return None

        try:
            self._current_value = list(islice(self._fin, 0, 1))[0].strip()
            return self._current_value
        except IndexError:
            raise IndexError(self._size, self._start, self.size)

    def pop(self):
        self._start += 1
        self._current_value = None

    def move(self, path: str):
        os.replace(self.path, path)

    def delete(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    @staticmethod
    def _file_name_from_path(path: str) -> str:
        return os.path.split(path)[-1]


def initial_sort(path_to_file: str) -> List[TempFile]:
    try:
        os.makedirs(TEMP_DIR)
    except FileExistsError:
        _ = [os.remove(os.path.join(TEMP_DIR, f)) for f in os.listdir(TEMP_DIR)]

    with open(path_to_file, 'r') as fin:

        temp_files = []
        while True:

            # read BULK_SIZE strings from file
            lines = list(islice(fin, 0, BULK_SIZE))
            if len(lines) == 0:
                break

            # write bulk into new temp file
            pointer = TempFile()
            pointer.write_bulk(sorted(lines))
            temp_files.append(pointer)

    print(f'Initial file has been splitted into {len(temp_files)} pre-sorted temporary files of length {BULK_SIZE}')
    return temp_files


def merge_bulks(temp_files: List[TempFile]) -> TempFile:
    if len(temp_files) == 1:
        return temp_files[0]
    elif len(temp_files) > 2:
        raise BaseException(f'Expected 2 files to merge but got {len(temp_files)}')
    else:
        temp_file0, temp_file1 = temp_files
        temp_file01 = TempFile()

        while temp_file0.size > 0 and temp_file1.size > 0:
            line0 = temp_file0.readline()
            line1 = temp_file1.readline()

            if line0 <= line1:
                temp_file01.write(line0)
                temp_file0.pop()
            else:
                temp_file01.write(line1)
                temp_file1.pop()

        while temp_file0.size > 0:
            line0 = temp_file0.readline()
            temp_file01.write(line0)
            temp_file0.pop()

        while temp_file1.size > 0:
            line1 = temp_file1.readline()
            temp_file01.write(line1)
            temp_file1.pop()

        temp_file0.delete()
        temp_file1.delete()

        return temp_file01


def array_split(arr: List[Any]) -> List[List[Any]]:
    chunk_size = 2
    return [arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size)]


def external_sort(temp_files: List[TempFile], n_threads: int) -> TempFile:
    iteration = 0
    while len(temp_files) != 1:
        chunks = array_split(temp_files)
        p = Pool(n_threads)

        print_string = f'Iteration {iteration}:\n\t{len(temp_files)} files have been merged into '
        temp_files = p.map(merge_bulks, chunks)
        print_string += f'{len(temp_files)}'
        print(print_string)

        iteration += 1

    return temp_files[0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sorts strings in large files')
    parser.add_argument('--input-file', dest='input_path', help='Path to file with strings', type=str)
    parser.add_argument('--output-file', dest='output_path', help='Path to file write sorted strings to', type=str)
    parser.add_argument('--n-threads', dest='n_threads', help='Level of parallelism', default=N_THREADS, type=int)

    arguments = parser.parse_args()
    if not os.path.exists(arguments.input_path):
        raise FileNotFoundError(f'The file you are trying to sort does not exist: {arguments.input_path}')
    else:
        temporary_files = initial_sort(arguments.input_path)

        sorted_strings = external_sort(temporary_files, arguments.n_threads)
        sorted_strings.move(arguments.output_path)
