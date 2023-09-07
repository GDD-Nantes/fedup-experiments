import os
import json
import time
import signal
import pandas
import requests
import threading
import subprocess
import shutil

TIMEOUT = config.setdefault("timeout", 600) # 10 minutes
XPDIR = config.setdefault("xpdir", "output")
RESTART = config.setdefault("restart", False)
VIRTUOSO_PORT = config.setdefault("virtuoso_port", 8890)
VIRTUOSO = config.setdefault("virtuoso", "/usr/local")

def get_pid(port):
    pid = None
    try:
        output = subprocess.check_output([
            "lsof", "-t", "-i", f":{port}",
            "-sTCP:LISTEN"])
        pid = int(output.strip())
    except subprocess.CalledProcessError:
        pass
    return pid

def ping(endpoint):
    proxies = {
        "http": "",
        "https": "",
    }
    try:
        response = requests.get(endpoint, proxies=proxies)
        # print(response.status_code, response.text)
        return response.status_code
    except: return -1

def start_virtuoso():
    if ping(f"http://localhost:{VIRTUOSO_PORT}/sparql") == 200:
        print("Virtuoso is running")
        return
    
    shell("docker start docker-bsbm-virtuoso-10")
    # print("Running virtuoso")
    # with open("virtuoso.log", "a+") as logfile:
    #     subprocess.Popen([
    #         f"{VIRTUOSO}/bin/virtuoso-t",
    #         "+configfile", f"{VIRTUOSO}/var/lib/virtuoso/db/fedup.ini",
    #         "+foreground"],
    #         stdout=subprocess.DEVNULL,
    #         stderr=subprocess.DEVNULL)
    #         # stdout=logfile.fileno(),
    #         # stderr=logfile.fileno())
    # time.sleep(20)

def start_spring():
    if get_pid(8080) is not None:
        print("Spring is running")
        return
    print("Running spring")
    with open("fedup.log", "w+") as logfile:
        mvn_bin = shutil.which("mvn")

        subprocess.Popen([
            mvn_bin, f"-Dmaven.repo.local={os.getcwd()}/.m2/repository", "spring-boot:run", "-pl", "fedup",
            "-Dspring-boot.run.jvmArguments=\"-Xms4096M\"",
            "-Dspring-boot.run.jvmArguments=\"-Xmx8192M\"",
            "-Dspring-boot.run.jvmArguments=\"-XX:TieredStopAtLevel=4\""],
            # shell=True,
            # stdout=subprocess.DEVNULL,
            # stderr=subprocess.DEVNULL)
            stdout=logfile.fileno(),
            stderr=logfile.fileno())
    time.sleep(10)

def kill_virtuoso_and_spring():
    pid = get_pid(8080)
    if pid:
        os.kill(pid, signal.SIGKILL)
    # pid = get_pid(8890)
    # if pid:
    #     os.kill(pid, signal.SIGKILL)

def delayed_task(delay, cancel_event):
    if not cancel_event.wait(delay):
        print("timeout reached, killing virtuoso and spring")
        kill_virtuoso_and_spring()

def list_queries(workload):
    queries = []
    for query in os.listdir(f"queries/{workload}"):
        queries.append(query.split('.')[0])
    return queries

def todo(wcs):
    files = []
    for workload in config.setdefault("workload", ["largerdfbench"]):
        for approach in config.setdefault("approach", ["fedup-optimal"]):
            for query in list_queries(workload):
                for run in config.setdefault("run", [1]):
                    files.append(f"{XPDIR}/{workload}/{approach}/{query}.{run}.csv")
    return files

def load_blacklist(wcs):
    if not os.path.exists(f"{wcs.xpdir}/.blacklist"):
        bl = []
    else:
        with open(f"{wcs.xpdir}/.blacklist", "r") as reader:
            bl = json.load(reader)
    return bl

def save_blacklist(wcs, bl):
    with open(f"{wcs.xpdir}/.blacklist", "w") as writer:
        json.dump(bl, writer, indent=2)

def is_blacklist(wcs):
    bl = load_blacklist(wcs)
    eq_keys = wcs.keys() - ["reason", "run"]
    for item in bl:
        if all(wcs[k] == item[k] for k in eq_keys):
            return item["reason"]
    return None

def blacklist(wcs, reason):
    bl = load_blacklist(wcs)
    if not is_blacklist(wcs):
        data = dict(wcs.items())
        data["reason"] = reason
        bl.append(data)
        save_blacklist(wcs, bl)
    print(f"blacklisted for {reason}")

def zero_result(status):
    return {
        "status": status,
        "sourceSelectionTime": 0,
        "executionTime": 0,
        "numASKQueries": 0,
        "numSolutions": 0,
        "numAssignments": 0,
        "tpwss": 0,
        "provenanceMappings": "[]",
        "solutions": "[]",
        "assignments": "[]",
        "tpAliases": "[]"
    }

def extend_and_format_result(result, wcs):
    columns = list(result.keys())
    row = list(result.values())
    columns.extend(["query", "approach", "run", "workload"])
    row.extend([wcs.query, wcs.approach, wcs.run, wcs.workload])
    return pandas.DataFrame([row], columns=columns)

def print_dataframe(df):
    for column in df.columns:
        print(f"df[{column}] = {df[column].to_string(index=False)}")

def run(params):
    # starting servers in case they have been killed by the timeout policy
    start_virtuoso()
    start_spring()
    # setting up the timeout policy
    cancel_event = threading.Event()
    delayed_task_thread = threading.Thread(
        target=delayed_task,
        args=(TIMEOUT, cancel_event))
    delayed_task_thread.start()
    # running the query
    start_time = time.time()
    try:
        response = requests.post("http://localhost:8080/fedSparql", json=params)
        # response = requests.get("http://localhost:8080/fedSparql", params=params)
        result = json.loads(response.content.decode("utf-8"))
    except Exception as error:
        elapsed_time = time.time() - start_time
        result = zero_result("TIMEOUT" if elapsed_time >= TIMEOUT else "ERROR")   
    # cancelling the timeout policy if everything went well 
    cancel_event.set()
    delayed_task_thread.join()
    # to deal with CostFed issues...
    if RESTART:
        kill_virtuoso_and_spring()
    return result
    
wildcard_constraints:
    query = "[A-z0-9]+",
    run = "[0-9]+",
    workload = "(largerdfbench|fedshop)",
    approach = "(fedx|hibiscus(-index)?|costfed(-index|-noopt)?|fedup(-id|-h0)-optimal)",
    xpdir = "[A-z0-9\-]+"

rule all:
    input: todo

rule merge_all:
    input: todo
    output: "{xpdir}/data.csv"
    run:
        dataframes = []
        projected_columns = [
            "query", "approach", "run", "workload",
            "sourceSelectionTime", "executionTime", "numASKQueries", "tpwss"]
        for file in input:
            try:
                df = pandas.read_csv(file)
                dataframes.append(df[projected_columns])
            except Exception as error:
                print(f"error in file {file}: {error}")
                raise error
        pandas.concat(dataframes).to_csv(str(output))

rule run_query:
    input:
        query = "queries/{workload}/{query}.sparql",
        config = "config/{workload}/{approach}.props",
        endpoints = "config/{workload}/endpoints.txt"
    output: "{xpdir}/{workload}/{approach}/{query}.{run}.csv"
    run:
        reason = is_blacklist(wildcards)
        if reason is None:
            with open(str(input.query)) as reader:
                query = reader.read()
            result = run({
                "queryString": query,
                "configFileName": f"{os.getcwd()}/{input.config}",
                "endpointsFileName": f"{os.getcwd()}/{input.endpoints}",
                "solutions": [],
                "assignments": [],
                "tpAliases": [],
                "runQuery": True})
            if result["status"] != "OK":
                blacklist(wildcards, result["status"])
        else:
            print(f"skip because blacklisted for {reason}")
            result = zero_result(reason)
        df = extend_and_format_result(result, wildcards)
        print_dataframe(df)
        df.to_csv(str(output))

rule fedup_random_walks_efficiency:
    input:
        query = "queries/{workload}/{query}.sparql",
        config = "config/{workload}/{approach}.props",
        endpoints = "config/{workload}/endpoints.txt",
        optimal = "{xpdir}/{workload}/fedup-id-optimal/{query}.1.csv"
    output: "{xpdir}/{workload}/{approach,fedup(-id|-h0)}/{query}.{run}.csv"
    run:
        reason = is_blacklist(wildcards)
        if reason is None:
            with open(str(input.query)) as reader:
                query = reader.read()
            optimalAssignments = pandas.read_csv(str(input.optimal))["assignments"].values[0]
            print(optimalAssignments)
            optimalAssignments = optimalAssignments.replace("\'", "\"")
            optimalAssignments = json.loads(optimalAssignments)
            result = run({
                "queryString": query,
                "configFileName": f"{os.getcwd()}/{input.config}",
                "endpointsFileName": f"{os.getcwd()}/{input.endpoints}",
                "assignments": optimalAssignments,
                "runQuery": True})
            if result["status"] != "OK":
                blacklist(wildcards, result["status"])
        else:
            print(f"skip because blacklisted for {reason}")
            result = zero_result(reason)
        df = extend_and_format_result(result, wildcards)
        print_dataframe(df)
        df.to_csv(str(output))