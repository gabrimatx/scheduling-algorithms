import csv
import os

# Use this script to associate logical names to jobIDs

def process_csv_file(csv_file, IdsToLogical):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 8: 
                event_type = row[3]
                jobID = row[2]
                logicalName = row[7]
                if event_type == "0":
                    IdsToLogical[jobID] = logicalName
            else:
                print("CSV file {} has less than 8 fields".format(csv_file))

directory = './job_events_extracted'
IdsToLogical = {}

for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        process_csv_file(file_path, IdsToLogical)

# Writing the dictionary to a CSV file
output_file = 'id_logicalName.csv'
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for item in IdsToLogical:
        writer.writerow([item, IdsToLogical[item]])

print("Data has been written to", output_file)
