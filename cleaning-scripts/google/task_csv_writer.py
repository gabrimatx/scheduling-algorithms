import csv
import os
import pandas as pd

# Run this script to create csv files in the format (task_arrival, id, task_size)

def process_csv_file(csv_file, task_sizes, task_status, starting_times, task_arrivals):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            timestamp = row[0]
            event_type = row[5]
            jobID = row[2]
            taskID = row[3]
            if (jobID, taskID) in task_status:
                if event_type == "4" and task_status[(jobID, taskID)] == "1":
                    task_sizes[(jobID, taskID)] = [int(timestamp) - int(starting_times[(jobID, taskID)]), starting_times[(jobID, taskID)]]
                    del starting_times[(jobID, taskID)]
                    del task_status[(jobID, taskID)]
                elif event_type != "8":
                    del task_status[(jobID, taskID)]
                    del starting_times[(jobID, taskID)]
            else:
                if event_type == "1" and int(timestamp) > 0:
                    task_status[(jobID, taskID)] = "1"
                    starting_times[(jobID, taskID)] = timestamp
                elif event_type == "0":
                    task_arrivals[(jobID, taskID)] = timestamp



directory = "./task_events_extracted"
logical_names_df = pd.read_csv("id_logicalName.csv", names=["jobID", "logicalName"])
sizes = {}
statuses = {}
starts = {}
arrivals = {}
logical_names = {}
output_folder = 'tasks-csv-output/'
processed_files = -1
i = 0
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        process_csv_file(file_path, sizes, statuses, starts, arrivals)
        processed_files += 1

    if processed_files >= 20:
        output_file = output_folder + "task_information_" + str(i) + ".csv"
        i += 1
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for k, v in sorted(sizes.items(), key=lambda x:int(arrivals[x[0][0],x[0][1]])):
                jobID = int(k[0])
                logical_name = str(logical_names_df.loc[logical_names_df.iloc[:, 0] == jobID, logical_names_df.columns[1]].values)

                if logical_name in logical_names:
                    lg = logical_names[logical_name]
                else:
                    logical_names[logical_name] = len(logical_names)
                    lg = logical_names[logical_name]

                if len(logical_name) > 0:
                    logical_name = logical_name[0]
                    writer.writerow([arrivals[(k[0], k[1])], lg, v[0]])
                    del sizes[(k[0], k[1])]
                else:
                    print(f"No matching jobID for {jobID} found in the DataFrame.")
        processed_files = 0

