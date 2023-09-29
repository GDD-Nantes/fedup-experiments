import glob
from parse import parse
import shutil

files = glob.glob('./benchmark/generation/*/*/injected.sparql')

for file in files:
    result = parse("./benchmark/generation/{query}/instance_{instance}/injected.sparql", file)
    to = "./queries/all_fedshop/" + result["query"] + chr(int(result["instance"])+97) + ".sparql"
    print(file + " -> " + to )
    shutil.copyfile(file, to)
