import csv
import os

def process_csv_file(csv_file, already_joined):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 8: 
                event_type = row[3]
                jobID = row[2]
                logicalName = row[7]
                if logicalName in already_joined:
                    if event_type == "0":
                        already_joined[logicalName].append(jobID)
                else:
                    already_joined[logicalName] = [jobID]
            else:
                print("CSV file {} has less than 8 fields".format(csv_file))

directory = './job_events_extracted'
already_joined = {}

for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        process_csv_file(file_path, already_joined)


