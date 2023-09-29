import pandas
import glob
from pygnuplot import gnuplot

output = "./data.csv"

# files = glob.glob('./output/largerdfbench/*/*.csv', recursive=True)
files = glob.glob('./output/fedshop/*/*.csv', recursive=True)

dataframes = []
projected_columns = [
    "query", "approach", "run", "workload",
    "sourceSelectionTime", "executionTime", "numASKQueries", "tpwss", "numAssignments", "numSolutions"]
for file in files:
    try:
        df = pandas.read_csv(file)
        dataframes.append(df[projected_columns])
    except Exception as error:
        print(f"error in file {file}: {error}")
        raise error

df = pandas.concat(dataframes)
df.sort_values(by=['query', 'approach', 'run'], inplace=True)
df.to_csv(str(output))

df = df.groupby(by=['query'])
nb_query = len(df)

# g = gnuplot.Gnuplot(terminal = 'pngcairo font "arial,10" fontscale 1.0 size 600, 400',
#                     output = '"simple.1.png"',
#                     multiplot= f'layout 1,{nb_query}')

# query = ""
# for index, group in df:
    
#     # new_plot = query != row["query"]
    
#     # query = row["query"]
#     # ss_time = int(row["sourceSelectionTime"])
#     # ex_time = int(row["executionTime"])
#     # total_time = ss_time + ex_time
#     print(group)
#     #g.plot_data(group,
#     #            "using $2
    
    
    
