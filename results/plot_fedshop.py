import pandas
import csv

file = "results_fedshop.csv"

dataframes = []
projected_columns = ["query", "engine", "batch", "instance", "source_selection_time", "exec_time"]
kept_approach = "costfed|fedup_h0|fedx|rsa|semagrow"
kept_batch = 9

print (f"Reading file {file}")
df = pandas.read_csv(file)
dataframes.append(df[projected_columns])

df = pandas.concat(dataframes)
df.sort_values(by=['query', 'engine', "instance", "batch"], inplace=True)


df["exec_time"] = df["exec_time"].div(1000.) ## to seconds
averaged = df.groupby(['query', 'engine', 'batch']).mean().reset_index()
stddev = df.groupby(['query', 'engine', 'batch']).sem().reset_index()


TIMEOUT = 120
averaged["yerr"] = stddev["exec_time"]
averaged["yerr"] = averaged["yerr"].fillna(0)
averaged['yerr'] = averaged["yerr"].replace(0., None) # to remove error bars on exact values
averaged["exec_time"] = averaged["exec_time"].fillna(TIMEOUT)

# averaged["source_selection_time"] = averaged["source_selection_time"].div(1000.)

# averaged["batch"] = averaged["batch"].add(1).mul(20)

# averaged["ymin"] =  averaged["exec_time"] - averaged["yerr"]
# averaged["ymax"] =  averaged["exec_time"] + averaged["yerr"]

# averaged["exec_time"] = averaged["exec_time"].fillna(0)

averaged = averaged[averaged['engine'].str.contains(kept_approach)]
averaged = averaged[averaged['batch'] == kept_batch]

averaged.reset_index( drop=False, inplace=True )

domain_order = {"SD": 1, "MD": 2, "CD": 3}

domain = [domain_order["MD"]]*5*4+ \
    [domain_order["CD"]]*5+ \
    [domain_order["MD"]]*5+ \
    [domain_order["CD"]]*5+ \
    [domain_order["MD"]]*5+ \
    [domain_order["SD"]]*5+ \
    [domain_order["MD"]]*5+ \
    [domain_order["SD"]]*5*2

averaged = averaged.assign(domain= domain)

# averaged = averaged.assign(domain= [
#     "MD", # q01
#     "MD", # q02
#     "MD", # q03
#     "MD", # q04
#     "CD", # q05
#     "MD", # q06
#     "CD", # q07
#     "MD", # q08
#     "SD", # q09
#     "MD", # q10
#     "SD", # q11
#     "SD", # q12
# ])
averaged.sort_values(by=['domain', 'query'], inplace=True)


header = ["query", "rsa", "costfed", "fedx", "semagrow", "fedup_h0", "rsa_err", "costfed_err", "fedx_err", "semagrow_err", "fedup_err"]

processed_queries = set()

values = []

# ugly but fast to code
for index, row in averaged.iterrows():
    if row["query"] not in processed_queries:
        processed_queries.add(row["query"])
        values.append([row["query"], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

    index_of_engine = header.index(row["engine"])
    
    values[-1][index_of_engine] = row["exec_time"]
    values[-1][index_of_engine+5] = row["yerr"]



    
header = ["query", "rsa", "costfed", "fedx", "semagrow", "fedup", "rsa_err", "costfed_err", "fedx_err", "semagrow_err", "fedup_err"]
    
with open('processed.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    for row in values:
        print (row)
        writer.writerow(row)
    

        

