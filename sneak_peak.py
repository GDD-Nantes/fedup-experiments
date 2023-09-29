import pandas
import json

# just checking manually if the number of results matches with expectations

dataframes = []
try:
    df = pandas.read_csv("./output/fedshop/fedup-id-optimal/q07i.1.csv")
    dataframes.append(df["solutions"])
except Exception as error:
    print(f"error in file {file}: {error}")
    raise error

df = pandas.concat(dataframes)


for solution in df.head().tolist(): #.split("', '"):
    solution = solution[2:-2]
    solution = solution.split("', '")
    # for meow in solution :
    #    print(meow)
    asSet = {m for m in solution}
    print(len(asSet))
    #for meow in asSet :
    #    print(meow)
    print(len(asSet))

    noOrderSet = set()
    for meow in asSet:
        noOrder = []
        eo = meow[1:-1]
        for e in eo.split(";"):
            noOrder.append(e)
        frozenNoOrder = frozenset(noOrder)
        noOrderSet.add(frozenNoOrder)
    for meow in noOrderSet:
        print (meow)
    print(len(noOrderSet))
