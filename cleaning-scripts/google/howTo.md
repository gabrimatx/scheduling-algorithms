# How to run the scripts in the cluster
Extract them in the directory of the cluster, then run the two extractors:
```sh
python3 job_extractor.py
python3 task_extractor.py
```
Then write the csv files that associates job ids to logical names:
```sh
python3 write_id_logicalName_csv.py
```
Finally, write the final csv with rows in the format `(arrival_time, task_id, task_size)` using:
```sh
python3 task_csv_writer.py 
```