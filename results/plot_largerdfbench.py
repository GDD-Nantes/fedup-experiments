import pandas
import csv

file = "results_largerdfbench.csv"

dataframes = []
projected_columns = ["query", "approach", "metric", "value"]
kept_approach = "FedX|HiBiscuS|CostFed|RSA|FedUP"

print (f"Reading file {file}")
df = pandas.read_csv(file)
dataframes.append(df[projected_columns])

df = pandas.concat(dataframes)
# df.sort_values(by=['query', 'approach'], inplace=True) already sorted
df = df[df['approach'].str.contains(kept_approach)]

print(df)

# df["exec_time"] = df["exec_time"].div(1000.) ## to seconds
# averaged = df.groupby(['query', 'engine', 'batch']).mean().reset_index()
# stddev = df.groupby(['query', 'engine', 'batch']).sem().reset_index()

TIMEOUT = 120
# averaged["yerr"] = stddev["exec_time"]
# averaged["yerr"] = averaged["yerr"].fillna()
# averaged['yerr'] = averaged["yerr"].replace(0., None)
# averaged["exec_time"] = averaged["exec_time"].fillna(TIMEOUT)

header = ["query", "RSA", "CostFed", "FedX", "HiBiscuS", "FedUP", "rsa_err", "costfed_err", "fedx_err", "semagrow_err", "fedup_err"]

processed_queries = set()

values = []

# ugly but fast to code
for index, row in df.iterrows():
    if row["query"] not in processed_queries:
        processed_queries.add(row["query"])
        values.append([row["query"], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

    index_of_engine = header.index(row["approach"])

    if row["metric"] == "Execution Time" or row["metric"] == "Source Selection Time" or row["metric"] == "Timeout":
        values[-1][index_of_engine] += row["value"]
    values[-1][index_of_engine+5] = None

header = ["query", "sparql 1.1", "costfed", "fedx", "hibiscus", "fedup", "rsa_err", "costfed_err", "fedx_err", "semagrow_err", "fedup_err"]
    
with open('processed.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONE)
    writer.writerow(header)
    for row in values:
        print(row)
        writer.writerow(row)
    
print(values)
        

