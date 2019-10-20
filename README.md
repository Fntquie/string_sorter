# Usage
### Generate file with strings:
```
python3.7 large_file_generator.py \
--output-file abnormal_amount_of_meanless_strings.txt \
--file-size 10000000 \
--max-string-length 500 \
--n-threads 20
```

### Sort this string with external merge sort:
```
python3.7 string_sorter.py \
--input-file abnormal_amount_of_meanless_strings.txt \
--output-file still_abnormal_but_sorted.txt \
--n-threads 20
```