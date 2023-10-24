import os
import re
import time
import json
import gdown
import signal
import pandas
import tempfile
import requests
import threading
import subprocess
import SPARQLWrapper

# XP configuration

JENA_HOME = f"{os.getcwd()}/apache-jena-4.9.0"
FUSEKI_HOME = f"{os.getcwd()}/apache-jena-fuseki-4.9.0"
VIRTUOSO_HOME = f"{os.getcwd()}/virtuoso-opensource-7.2.11"

VIRTUOSO_PORT = 8890
FUSEKI_PORT = 3030
FEDUP_PORT = 8080

RESTART = False # restart engines between queries

WORKLOADS = ["fedshop", "largerdfbench"]
APPROACHES = ["fedx", "hibiscus-ask", "costfed-ask", "fedup-h0"]
RUNS = [1, 2, 3, 4, 5]

FEDSHOP_QUERIES = [
    "q01a", "q01b", "q01c", "q01d", "q01e", "q01f", "q01g", "q01h", "q01i", "q01j",
    "q02a", "q02b", "q02c", "q02d", "q02e", "q02f", "q02g", "q02h", "q02i", "q02j",
    "q03a", "q03b", "q03c", "q03d", "q03e", "q03f", "q03g", "q03h", "q03i", "q03j",
    "q04a", "q04b", "q04c", "q04d", "q04e", "q04f", "q04g", "q04h", "q04i", "q04j",
    "q05a", "q05b", "q05c", "q05d", "q05e", "q05f", "q05g", "q05h", "q05i", "q05j",
    "q06a", "q06b", "q06c", "q06d", "q06e", "q06f", "q06g", "q06h", "q06i", "q06j",
    "q07a", "q07b", "q07c", "q07d", "q07e", "q07f", "q07g", "q07h", "q07i", "q07j",
    "q08a", "q08b", "q08c", "q08d", "q08e", "q08f", "q08g", "q08h", "q08i", "q08j",
    "q09a", "q09b", "q09c", "q09d", "q09e", "q09f", "q09g", "q09h", "q09i", "q09j",
    "q10a", "q10b", "q10c", "q10d", "q10e", "q10f", "q10g", "q10h", "q10i", "q10j",
    "q11a", "q11b", "q11c", "q11d", "q11e", "q11f", "q11g", "q11h", "q11i", "q11j",
    "q12a", "q12b", "q12c", "q12d", "q12e", "q12f", "q12g", "q12h", "q12i", "q12j"
]
FEDSHOP_SCALES = [20, 40, 60, 80, 100, 120, 140, 160, 180, 200]

LARGERDFBENCH_QUERIES = [
    "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12", "S13", "S14",
    "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10"
]

# Test configuration

# LARGERDFBENCH_TIMEOUT = 1200 # 20 minutes
# FEDSHOP_TIMEOUT = 60 # 1 minute
#
# JENA_HOME = f"{os.getcwd()}/apache-jena-4.9.0"
# FUSEKI_HOME = f"{os.getcwd()}/apache-jena-fuseki-4.9.0"
# VIRTUOSO_HOME="/usr/local/virtuoso-opensource"
#
# VIRTUOSO_PORT = 8890
# FUSEKI_PORT = 3030
# FEDUP_PORT = 8080
#
# RESTART = False # restart engines between queries
#
# APPROACHES = ["fedx", "fedup-h0", "RSA"]
# RUNS = [1, 2]
#
# FEDSHOP_QUERIES = ["q01a", "q02a"]
# FEDSHOP_SCALES = [20, 40]
#
# LARGERDFBENCH_QUERIES = []
#
# LARGERDFBENCH_TIMEOUT = 1200 # 20 minutes
# FEDSHOP_TIMEOUT = 60 # 1 minute

# def load_query(query_file):
#     with open(query_file, "r") as file:
#         return str(file.read())
#
# def load_endpoints(endpoints_file):
#     with open(endpoints_file, "r") as file:
#         return [str(endpoint).strip() for endpoint in file.readlines()]
#
# def list_queries(workload):
#     queries = []
#     for query in os.listdir(f"queries/{workload}"):
#         queries.append(query.split('.')[0])
#     return queries

# def largerdfbench_queries():
#     queries = []
#     if "largerdfbench" not in WORKLOADS:
#         return queries
#     for approach in APPROACHES:
#         for query in LARGERDFBENCH_QUERIES:
#             for run in RUNS:
#                 queries.append(f"output/largerdfbench/{approach}/{query}.{run}.csv")
#     return queries
#
# def fedshop_queries():
#     queries = []
#     if "fedshop" not in WORKLOADS:
#         return queries
#     for scale in FEDSHOP_SCALES:
#         for approach in APPROACHES:
#             for query in FEDSHOP_QUERIES:
#                 for run in RUNS:
#                     queries.append(f"output/fedshop{scale}/{approach}/{query}.{run}.csv")
#     return queries
#
# def all_queries(wcs):
#     return largerdfbench_queries() + fedshop_queries()

# def get_pid(port):
#     pid = None
#     try:
#         output = subprocess.check_output(["lsof", "-t", "-i", f":{port}", "-sTCP:LISTEN"])
#         pid = int(output.strip())
#     except subprocess.CalledProcessError:
#         pass
#     return pid
#
# def kill_process(port):
#     pid = get_pid(port)
#     if pid is None:
#         return
#     os.kill(pid,signal.SIGKILL)
#
# def stop_virtuoso():
#     kill_process(VIRTUOSO_PORT)
#
# def start_virtuoso(config):
#     if RESTART:
#         stop_virtuoso()
#     if get_pid(VIRTUOSO_PORT) is not None:
#         print("Virtuoso is running")
#         return
#     print("running Virtuoso")
#     subprocess.Popen(
#         [f"{VIRTUOSO_HOME}/bin/virtuoso-t", "+configfile", config, "+foreground"],
#         stdout=subprocess.DEVNULL,
#         stderr=subprocess.DEVNULL)
#     time.sleep(20)
#
# def stop_fuseki():
#     kill_process(FUSEKI_PORT)
#
# def start_fuseki():
#     if RESTART:
#         stop_fuseki()
#     if get_pid(FUSEKI_PORT) is not None:
#         print("Apache Fuseki is running")
#         return
#     print("running Apache Fuseki")
#     subprocess.Popen(
#         [f"{FUSEKI_HOME}/fuseki-server", "--mem", "--port", FUSEKI_PORT, "/sparql"],
#         stdout=subprocess.DEVNULL,
#         stderr=subprocess.DEVNULL)
#     time.sleep(20)
#
# def stop_fedup():
#     kill_process(FEDUP_PORT)
#
# def start_fedup():
#     if RESTART:
#         stop_fedup()
#     if get_pid(FEDUP_PORT) is not None:
#         print("FedUP is running")
#         return
#     print("Running FedUP")
#     subprocess.Popen([
#             "mvn", "spring-boot:run", "-pl", "fedup",
#             f"-Dserver.port={FEDUP_PORT}"
#             " -Dspring-boot.run.jvmArguments=\"-Xms4096M\"",
#             " -Dspring-boot.run.jvmArguments=\"-Xmx8192M\"",
#             " -Dspring-boot.run.jvmArguments=\"-XX:TieredStopAtLevel=4\""],
#         stdout=subprocess.DEVNULL,
#         stderr=subprocess.DEVNULL)
#     time.sleep(10)
#
# def stop_fedup_and_virtuoso_after_timeout(timeout, cancel_event):
#     if not cancel_event.wait(timeout):
#         print("timeout reached, killing Virtuoso and FedUP")
#         stop_fedup()
#         stop_virtuoso()
#
# def run_RSA_query(query_string, virtuoso_config):
#     start_virtuoso(virtuoso_config)
#     start_fuseki()
#
#     sparql = SPARQLWrapper.SPARQLWrapper(f"http://localhost:{FUSEKI_PORT}/sparql")
#
#     sparql.setReturnFormat(SPARQLWrapper.JSON)
#     sparql.setQuery(query_string)
#
#     try:
#         start_time = time.time()
#         results = sparql.queryAndConvert()
#         end_time = time.time()
#         return {
#             "status": ["ok"],
#             "reason": [""],
#             "sourceSelectionTime": [0],
#             "executionTime": [(end_time - start_time) * 1000],
#             "runtime": [(end_time - start_time) * 1000],
#             "numAskQueries": [0],
#             "numSolutions": [len(results["results"]["bindings"])],
#             "numAssignments": [0],
#             "tpwss": [0]
#         }, results
#     except Exception as error:
#         return {"status": ["error"], "reason": [error]}, []
#
# def run_query(query_string, approach_config, virtuoso_config, endpoints, timeout):
#     start_virtuoso(virtuoso_config)
#     start_fedup()
#
#     cancel_event = threading.Event()
#     delayed_task_thread = threading.Thread(
#         target=stop_fedup_and_virtuoso_after_timeout,
#         args=(timeout, cancel_event))
#     delayed_task_thread.start()
#
#     start_time = time.time()
#
#     try:
#         response = requests.post(f"http://localhost:{FEDUP_PORT}/fedSparql", json = {
#             "queryString": query_string,
#             "configFileName": approach_config,
#             "endpoints": endpoints,
#             "runQuery": True
#         })
#         result = json.loads(response.content.decode("utf-8"))
#         return {k:[v] for k, v in result.items() if k in [
#             "sourceSelectionTime",
#             "executionTime",
#             "runtime",
#             "numAskQueries",
#             "numSolutions",
#             "numAssignments",
#             "tpwss"
#         ]} | {"status": ["ok"], "reason": [""]}, result["solutions"]
#     except Exception as error:
#         end_time = time.time()
#         status = "timeout" if (end_time - start_time) >= timeout else "error"
#         return {"status": [status], "reason": [error]}, []
#     finally:
#         cancel_event.set()
#         delayed_task_thread.join()

if "skipInstall" not in config:

    rule install_apache_jena_fuseki:
        priority: 100
        output: directory(FUSEKI_HOME)
        shell:
            f"""
            wget https://archive.apache.org/dist/jena/binaries/apache-jena-fuseki-4.9.0.tar.gz
            tar -zxf apache-jena-fuseki-4.9.0.tar.gz -C {os.getcwd()}
            """

    rule install_apache_jena:
        priority: 100
        output:
            jena = directory(JENA_HOME),
            jena_loader = f"{JENA_HOME}/bin/tdb2.xloader"
        shell:
            f"""
            wget https://archive.apache.org/dist/jena/binaries/apache-jena-4.9.0.tar.gz
            tar -zxf apache-jena-4.9.0.tar.gz -C {os.getcwd()}
            """

    rule install_virtuoso:
        priority: 100
        output:
            virtuoso = directory(VIRTUOSO_HOME),
            virtuoso_isql = f"{VIRTUOSO_HOME}/bin/isql",
            virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/virtuoso.ini"
        shell:
            f"""
            wget https://github.com/openlink/virtuoso-opensource/releases/download/v7.2.11/virtuoso-opensource-7.2.11.tar.gz
            tar -zxf virtuoso-opensource-7.2.11.tar.gz
            cd virtuoso-opensource-7.2.11
            ./autogen.sh
            ./configure
            make && make install prefix={VIRTUOSO_HOME} && make clean && make distclean
            """

if "skipDownload" not in config:

    rule download_datasets:
        output: ["datasets/largerdfbench.nq.gz"] + [f"datasets/fedshop{n}.nq.gz" for n in range(20, 201, 20)]
        run:
            gdown.download("https://drive.google.com/uc?id=1Q4ZwiwTRzRvj9m3jZN-w3R9MIi0xvLGm", "datasets.tar", quiet=True)
            shell("tar -xf datasets.tar")

if "skipLoad" not in config:

    rule load_FedShop_into_Virtuoso:
        priority: 100
        input:
            dataset = ancient("datasets/fedshop{n}.nq.gz"),
            virtuoso = ancient(VIRTUOSO_HOME),
            virtuoso_isql = ancient(f"{VIRTUOSO_HOME}/bin/isql"),
            virtuoso_configfile = ancient(f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/virtuoso.ini")
        output:
            graphs = "config/fedshop{n}/graphs.txt"
        params:
            regex = lambda wildcards: "[0-9]" if (int(wildcards.n) / 2) // 10 == 1 else f"[0-{int(((int(wildcards.n) / 2) // 10) - 1)}]?[0-9]"
        shell:
            """
            python commons.py start-virtuoso --home {input.virtuoso} --config {input.virtuoso_configfile}
            {input.virtuoso_isql} "EXEC=ld_dir('`pwd`/datasets', 'fedshop{wildcards.n}.nq.gz', 'NULL');"
            {input.virtuoso_isql} "EXEC=rdf_loader_run();"
            {input.virtuoso_isql} "EXEC=checkpoint;"
            {input.virtuoso_isql} "EXEC=SPARQL SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?s a ?c }} }};" | egrep "(ratingsite|vendor){params.regex}" > {output.graphs}
            # python commons.py stop-virtuoso --config {input.virtuoso_configfile}
            """

    rule load_LargeRDFBench_into_Virtuoso:
        priority: 100
        input:
            dataset = ancient("datasets/largerdfbench.nq.gz"),
            virtuoso = ancient(VIRTUOSO_HOME),
            virtuoso_isql = ancient(f"{VIRTUOSO_HOME}/bin/isql"),
            virtuoso_configfile = ancient(f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/virtuoso.ini")
        output:
            graphs = "config/largerdfbench/graphs.txt"
        shell:
            """
            python commons.py start-virtuoso --home {input.virtuoso} --config {input.virtuoso_configfile}
            {input.virtuoso_isql} "EXEC=ld_dir('`pwd`/datasets', 'largerdfbench.nq.gz', 'NULL');"
            {input.virtuoso_isql} "EXEC=rdf_loader_run();"
            {input.virtuoso_isql} "EXEC=checkpoint;"
            {input.virtuoso_isql} "EXEC=SPARQL SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?s a ?c }} }};" | egrep "(example)" > {output.graphs}
            # python commons.py stop-virtuoso --config {input.virtuoso_configfile}
            """

rule setup_virtuoso:
    priority: 100
    input:
        virtuoso_port = str(VIRTUOSO_PORT),
        virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/virtuoso.ini"
    output:
        virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/fedup.ini"
    shell:
        f"""
        cp {{input.virtuoso_configfile}} {{output.virtuoso_configfile}}
        sed -i -E 's@(^ServerPort.*?= [^(1111)].*?$)@ServerPort = {{input.virtuoso_port}}@g' {{output.virtuoso_configfile}} # update Virtuoso port
        sed -i -E 's@(^DefaultHost.*$)@DefaultHost = localhost:{{input.virtuoso_port}}@g' {{output.virtuoso_configfile}} # update Virtuoso port
        sed -i -E 's@DirsAllowed(.*)$@DirsAllowed\\1, {os.getcwd()}@g' {{output.virtuoso_configfile}} # allow data to be loaded from the FedUP directory
        sed -i -E 's@(^ResultSetMaxRows.*$)@;\\1@g' {{output.virtuoso_configfile}} # disable quotas on query results
        sed -i -E 's@(^MaxQueryCostEstimationTime.*$)@;\\1@g' {{output.virtuoso_configfile}} # disable quotas on query plans
        sed -i -E 's@(^MaxQueryExecutionTime.*$)@;\\1@g' {{output.virtuoso_configfile}} # disable quotas on query execution time
        sed -i -E 's@(^NumberOfBuffers.*$)@;\\1@g' {{output.virtuoso_configfile}}
        sed -i -E 's@(^MaxDirtyBuffers.*$)@;\\1@g' {{output.virtuoso_configfile}}
        sed -i -E 's@^;(NumberOfBuffers.*?= 2720000.*$)@\\1@g' {{output.virtuoso_configfile}} # update Virtuoso resources for a machine with 32G RAM
        sed -i -E 's@^;(MaxDirtyBuffers.*?= 2000000.*$)@\\1@g' {{output.virtuoso_configfile}} # update Virtuoso resources for a machine with 32G RAM
        sed -i -E 's@(^MaxQueryMem.*)@MaxQueryMem = 8G@g' {{output.virtuoso_configfile}} # increase MaxQueryMem from 2G to 8G
        """

rule generate_endpoints:
    input:
        virtuoso_port = str(VIRTUOSO_PORT),
        graphs = "config/{dataset}/graphs.txt"
    output:
        endpoints = "config/{dataset,(largerdfbench|fedshop[0-9]+)}/endpoints.txt"
    shell:
        "sed -E 's@(^.*$)@http://localhost:{input.virtuoso_port}/sparql?default-graph-uri=\\1@g' {input.graphs} > {output.endpoints}"

if "skipGenerate" not in config:

    rule generate_FedUP_summary:
        priority: 100
        input:
            virtuoso = VIRTUOSO_HOME,
            virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/fedup.ini",
            endpoints = "config/{dataset}/endpoints.txt",
            jena_loader = f"{JENA_HOME}/bin/tdb2.xloader"
        output:
            summary = directory("summaries/{dataset,(largerdfbench|fedshop[0-9]+)}/{approach,fedup(-h0)}")
        shell:
            """
            python commons.py start-virtuoso --home {input.virtuoso} --config {input.virtuoso_configfile}
            mvn exec:java -Dexec.mainClass="fr.univnantes.gdd.fedup.startup.GenerateSummaries" -Dexec.args="fedup -e {input.endpoints} -m 0 -o {output.summary}.nq" -pl fedup
            # python commons.py stop-virtuoso --config {input.virtuoso_configfile}
            {input.jena_loader} --loc {output.summary} {output.summary}.nq
            """

    rule generate_FedUP_identity:
        priority: 100
        input:
            dataset = "datasets/{dataset}.nq.gz",
            jena_loader = f"{JENA_HOME}/bin/tdb2.xloader"
        output:
            summary = directory("summaries/{dataset,(largerdfbench|fedshop[0-9]+)}/{approach,fedup(-id)}")
        shell:
            "{input.jena_loader} --loc {output.summary} {input.dataset}"

    rule generate_CostFed_summary:
        priority: 100
        input:
            virtuoso = VIRTUOSO_HOME,
            virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/fedup.ini",
            endpoints = "config/{dataset}/endpoints.txt"
        output:
            summary = "summaries/{dataset,(largerdfbench|fedshop[0-9]+)}/costfed.n3"
        shell:
            """
            python commons.py start-virtuoso --home {input.virtuoso} --config {input.virtuoso_configfile}
            mvn exec:java -Dexec.mainClass="fr.univnantes.gdd.fedup.startup.GenerateSummaries" -Dexec.args="costfed -e {input.endpoints} -o {output.summary}" -pl fedup
            # python commons.py stop-virtuoso --config {input.virtuoso_configfile}
            """

    rule generate_HiBISCuS_summary:
        priority: 100
        input:
            virtuoso = VIRTUOSO_HOME,
            virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/fedup.ini",
            endpoints = "config/{dataset}/endpoints.txt"
        output:
            summary = "summaries/{dataset,(largerdfbench|fedshop[0-9]+)}/hibiscus.n3"
        shell:
            """
            python commons.py start-virtuoso --home {input.virtuoso} --config {input.virtuoso_configfile}
            mvn exec:java -Dexec.mainClass="fr.univnantes.gdd.fedup.startup.GenerateSummaries" -Dexec.args="hibiscus -e {input.endpoints} -o {output.summary}" -pl fedup
            # python commons.py stop-virtuoso --config {input.virtuoso_configfile}
            """

rule generate_FedUP_configuration_file:
    priority: 100
    input:
        configfile = "config/{approach}.props",
        summary = "summaries/{dataset}/{approach}"
    output:
        configfile = "config/{dataset,(largerdfbench|fedshop[0-9]+)}/{approach,fedup(-h0|-id)}.props"
    shell:
        f"sed -E 's@(^fedup\.summary=.*$)@fedup.summary={os.getcwd()}/{{input.summary}}@g' {{input.configfile}} > {{output.configfile}}"

rule generate_CostFed_configuration_file:
    priority: 100
    input:
        configfile = "config/{approach}.props",
        summary = "summaries/{dataset}/costfed.n3"
    output:
        configfile = "config/{dataset,(largerdfbench|fedshop[0-9]+)}/{approach,costfed(-ask|-index)(-noopt)?}.props"
    shell:
        f"sed -E 's@(^quetzal\.fedSummaries=.*$)@quetzal.fedSummaries={os.getcwd()}/{{input.summary}}@g' {{input.configfile}} > {{output.configfile}}"

rule generate_HiBISCuS_configuration_file:
    priority: 100
    input:
        configfile = "config/{approach}.props",
        summary = "summaries/{dataset}/hibiscus.n3"
    output:
        configfile = "config/{dataset,(largerdfbench|fedshop[0-9]+)}/{approach,hibiscus(-ask|-index)}.props"
    shell:
        f"sed -E 's@(^quetzal\.fedSummaries=.*$)@quetzal.fedSummaries={os.getcwd()}/{{input.summary}}@g' {{input.configfile}} > {{output.configfile}}"

rule generate_FedX_configuration_file:
    priority: 100
    input:
        configfile = "config/{approach}.props",
    output:
        configfile = "config/{dataset,(largerdfbench|fedshop[0-9]+)}/{approach,fedx}.props"
    shell:
        "cp {input.configfile} {output.configfile}"

rule run_RSA_query:
    priority: 50
    input:
        virtuoso = VIRTUOSO_HOME,
        virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/fedup.ini",
        fuseki = FUSEKI_HOME,
        query = "queries/{workload}-RSA/{query}.sparql",
        endpoints = "config/{workload}/endpoints.txt"
    output:
        metrics = "output/{workload,(largerdfbench|fedshop[0-9]+)}/{approach,RSA}/{query}.{run}.csv",
        solutions = "output/{workload,(largerdfbench|fedshop[0-9]+)}/{approach,RSA}/{query}.{run}.json"
    params:
        timeout = lambda wildcards: LARGERDFBENCH_TIMEOUT if wildcards.workload == "largerdfbench" else FEDSHOP_TIMEOUT
    run:
        query_file = tempfile.NamedTemporaryFile()
        shell(f"sed -E 's@http://localhost:[0-9]+/sparql@http://localhost:{VIRTUOSO_PORT}/sparql@g' {{input.query}} > {query_file.name}")
        shell(f"python commons.py start-virtuoso --home {{input.virtuoso}} --config {{input.virtuoso_configfile}} --restart {RESTART}")
        shell(f"python commons.py start-fuseki --home {{input.fuseki}} --port {FUSEKI_PORT} --restart {RESTART}")
        shell(f"timeout {{params.timeout}} python commons.py run-rsa-query {query_file.name} --metrics-output {{output.metrics}} --solutions-output {{output.solutions}}")
        df = pandas.read_csv(str(output.metrics))
        df["query"] = wildcards.query
        df["workload"] = wildcards.workload
        df["approach"] = wildcards.approach
        df["run"] = wildcards.run
        df.to_csv(str(output.metrics))
        query_file.close()

rule run_query:
    input:
        virtuoso = VIRTUOSO_HOME,
        virtuoso_configfile = f"{VIRTUOSO_HOME}/var/lib/virtuoso/db/fedup.ini",
        query = "queries/{workload}/{query}.sparql",
        endpoints = "config/{workload}{scale}/endpoints.txt",
        approach_configfile = "config/{workload}{scale}/{approach}.props"
    output:
        metrics = "output/{workload,(largerdfbench|fedshop)}{scale,[0-9]*}/{approach}/{query}.{run}.csv",
        solutions = "output/{workload,(largerdfbench|fedshop)}{scale,[0-9]*}/{approach}/{query}.{run}.json"
    params:
        timeout = lambda wildcards: LARGERDFBENCH_TIMEOUT if wildcards.workload == "largerdfbench" else FEDSHOP_TIMEOUT
    run:
        shell(f"python commons.py start-virtuoso --home {{input.virtuoso}} --config {{input.virtuoso_configfile}} --restart {RESTART}")
        shell(f"python commons.py start-fedup --port {FEDUP_PORT} --restart {RESTART}")
        shell(f"timeout {{params.timeout}} python commons.py run-query {{input.query}} {{input.endpoints}} {os.getcwd()}/{{input.approach_configfile}} --metrics-output {{output.metrics}} --solutions-output {{output.solutions}} || true")
        df = pandas.read_csv(str(output.metrics))
        df["query"] = wildcards.query
        df["workload"] = "{wildcards.workload}{wildcards.scale}"
        df["approach"] = wildcards.approach
        df["run"] = wildcards.run
        if df["status"].values[0] == "timeout":
            shell(f"python commons.py stop-process {VIRTUOSO_PORT}")
            shell(f"python commons.py stop-process {FEDUP_PORT}")
        df.to_csv(str(output.metrics))

rule run_all_queries:
    input:
        largerdfbench = expand("output/largerdfbench/{approach}/{query}.{run}.csv", approach=APPROACHES, query=LARGERDFBENCH_QUERIES, run=RUNS),
        fedshop = expand("output/fedshop{n}/{approach}/{query}.{run}.csv", n=FEDSHOP_SCALES, approach=APPROACHES, query=FEDSHOP_QUERIES, run=RUNS)
    output: "output/data.csv"
    run:
        data = [pandas.read_csv(file) for file in input.largerdfbench + input.fedshop]
        pandas.concat(data,axis=0).to_csv(str(output))

rule run_xp:
    input: "output/data.csv"

# onstart:
#     shell("mvn clean install")
# onsuccess:
#     shell(f"python commons.py stop-process {VIRTUOSO_PORT}")
#     shell(f"python commons.py stop-process {FUSEKI_PORT}")
#     shell(f"python commons.py stop-process {FEDUP_PORT}")
# onerror:
#     shell(f"python commons.py stop-process {VIRTUOSO_PORT}")
#     shell(f"python commons.py stop-process {FUSEKI_PORT}")
#     shell(f"python commons.py stop-process {FEDUP_PORT}")

# import os
# import json
# import time
# import signal
# import pandas
# import requests
# import threading
# import subprocess
# import shutil
#
# TIMEOUT = config.setdefault("timeout", 600) # 10 minutes
# XP_DIR = config.setdefault("directory", "output")
# RESTART = config.setdefault("restart", False)
# VIRTUOSO_PORT = config.setdefault("virtuoso_port", 8890)
# VIRTUOSO = config.setdefault("virtuoso", "/usr/local")
#
# def get_pid(port):
#     pid = None
#     try:
#         output = subprocess.check_output(["lsof", "-t", "-i", f":{port}", "-sTCP:LISTEN"])
#         pid = int(output.strip())
#     except subprocess.CalledProcessError:
#         pass
#     return pid
#
# def ping(endpoint):
#     proxies = {
#         "http": "",
#         "https": "",
#     }
#     try:
#         response = requests.get(endpoint, proxies=proxies)
#         # print(response.status_code, response.text)
#         return response.status_code
#     except: return -1
#
# def start_virtuoso():
#     if ping(f"http://localhost:{VIRTUOSO_PORT}/sparql") == 200:
#         print("Virtuoso is running")
#         return
#
#     shell("docker start docker-bsbm-virtuoso-10")
#     # print("Running virtuoso")
#     # with open("virtuoso.log", "a+") as logfile:
#     #     subprocess.Popen([
#     #         f"{VIRTUOSO}/bin/virtuoso-t",
#     #         "+configfile", f"{VIRTUOSO}/var/lib/virtuoso/db/fedup.ini",
#     #         "+foreground"],
#     #         stdout=subprocess.DEVNULL,
#     #         stderr=subprocess.DEVNULL)
#     #         # stdout=logfile.fileno(),
#     #         # stderr=logfile.fileno())
#     # time.sleep(20)
#
# def start_spring():
#     if get_pid(8080) is not None:
#         print("Spring is running")
#         return
#     print("Running spring")
#     with open("FedUP.log", "w+") as logfile:
#         subprocess.Popen([
#             shutil.which("mvn"),
#             f"-Dmaven.repo.local={os.getcwd()}/.m2/repository", "spring-boot:run", "-pl", "fedup",
#             f"-Dspring-boot.run.jvmArguments=\"-Xms4096M\"",
#             f"-Dspring-boot.run.jvmArguments=\"-Xmx8192M\"",
#             f"-Dspring-boot.run.jvmArguments=\"-XX:TieredStopAtLevel=4\""
#         ],
#         # shell=True,
#         # stdout=subprocess.DEVNULL,
#         # stderr=subprocess.DEVNULL)
#         stdout=logfile.fileno(),
#         stderr=logfile.fileno())
#     time.sleep(10)
#
# def kill_virtuoso_and_spring():
#     pid = get_pid(8080)
#     if pid:
#         os.kill(pid, signal.SIGKILL)
#     # pid = get_pid(8890)
#     # if pid:
#     #     os.kill(pid, signal.SIGKILL)
#
# def delayed_task(delay, cancel_event):
#     if not cancel_event.wait(delay):
#         print("timeout reached, killing virtuoso and spring")
#         kill_virtuoso_and_spring()
#
# def list_queries(workload):
#     queries = []
#     for query in os.listdir(f"queries/{workload}"):
#         queries.append(query.split('.')[0])
#     return queries
#
# def todo(wcs):
#     files = []
#     for workload in config.setdefault("workload", ["largerdfbench"]):
#         for batch in config.setdefault("batch", [0]):
#             for approach in config.setdefault("approach", ["fedup-id"]):
#                 for query in list_queries(workload):
#                     for run in config.setdefault("run", [1]):
#                         if query not in config.setdefault("blacklist", []):
#                             files.append(f"{XP_DIR}/{workload}/batch{batch}/{approach}/{query}.{run}.csv")
#     return files
#
# def load_blacklist(wcs):
#     if not os.path.exists(f"{wcs.directory}/.blacklist"):
#         bl = []
#     else:
#         with open(f"{wcs.directory}/.blacklist", "r") as reader:
#             bl = json.load(reader)
#     return bl
#
# def save_blacklist(wcs, bl):
#     with open(f"{wcs.directory}/.blacklist", "w") as writer:
#         json.dump(bl, writer, indent=2)
#
# def is_blacklist(wcs):
#     bl = load_blacklist(wcs)
#     eq_keys = wcs.keys() - ["reason", "run"]
#     for item in bl:
#         if all(wcs[k] == item[k] for k in eq_keys):
#             return item["reason"]
#     return None
#
# def blacklist(wcs, reason):
#     bl = load_blacklist(wcs)
#     if not is_blacklist(wcs):
#         data = dict(wcs.items())
#         data["reason"] = reason
#         bl.append(data)
#         save_blacklist(wcs, bl)
#     print(f"blacklisted for {reason}")
#
# def zero_result(status):
#     return {
#         "status": status,
#         "sourceSelectionTime": 0,
#         "executionTime": 0,
#         "runtime": 0,
#         "numASKQueries": 0,
#         "numSolutions": 0,
#         "numAssignments": 0,
#         "tpwss": 0,
#         "provenanceMappings": "[]",
#         "solutions": "[]",
#         "assignments": "[]",
#         "tpAliases": "[]"
#     }
#
# def extend_and_format_result(result, wcs):
#     columns = list(result.keys())
#     row = list(result.values())
#     columns.extend(["query", "approach", "run", "workload"])
#     row.extend([wcs.query, wcs.approach, wcs.run, wcs.workload])
#     return pandas.DataFrame([row], columns=columns)
#
# def print_dataframe(df):
#     for column in df.columns:
#         print(f"df[{column}] = {df[column].to_string(index=False)}")
#
# def run(params):
#     # starting servers in case they have been killed by the timeout policy
#     start_virtuoso()
#     start_spring()
#     # setting up the timeout policy
#     cancel_event = threading.Event()
#     delayed_task_thread = threading.Thread(
#         target=delayed_task,
#         args=(TIMEOUT, cancel_event))
#     delayed_task_thread.start()
#     # running the query
#     start_time = time.time()
#     try:
#         response = requests.post("http://localhost:8080/fedSparql", json=params)
#         # response = requests.get("http://localhost:8080/fedSparql", params=params)
#         result = json.loads(response.content.decode("utf-8"))
#     except Exception as error:
#         elapsed_time = time.time() - start_time
#         result = zero_result("TIMEOUT" if elapsed_time >= TIMEOUT else "ERROR")
#     # cancelling the timeout policy if everything went well
#     cancel_event.set()
#     delayed_task_thread.join()
#     # to deal with CostFed issues...
#     if RESTART:
#         kill_virtuoso_and_spring()
#     return result
#
# wildcard_constraints:
#     query = "[A-z0-9]+",
#     run = "[0-9]+",
#     workload = "(largerdfbench|fedshop)(-RSA)?",
#     approach = "(fedx|hibiscus(-index|-ask)|costfex(-index|-ask)(-noopt)?|fedup(-id|-h0))",
#     batch = "[0-9]",
#     directory = "[A-z0-9\-]+"
#
# rule all:
#     input: todo
#
# rule merge_all:
#     input: todo
#     output: "{directory}/data.csv"
#     run:
#         dataframes = []
#         projected_columns = [
#             "query", "approach", "run", "workload", "batch",
#             "sourceSelectionTime", "executionTime", "runtime",
#             "numASKQueries", "tpwss", "numAssignments", "numSolutions"]
#         for file in input:
#             try:
#                 df = pandas.read_csv(file)
#                 dataframes.append(df[projected_columns])
#             except Exception as error:
#                 print(f"error in file {file}: {error}")
#                 raise error
#         pandas.concat(dataframes).to_csv(str(output))
#
# rule run_query:
#     input:
#         query = "queries/{workload}/{query}.sparql",
#         config = "config/{workload}/{batch}/{approach}.props",
#         endpoints = "config/{workload}/{batch}/endpoints.txt"
#     output: "{directory}/{workload}/{batch}/{approach,(largerdfbench|fedshop)}/{query}.{run}.csv"
#     run:
#         reason = is_blacklist(wildcards)
#         if reason is None:
#             with open(str(input.query)) as reader:
#                 query = reader.read()
#             result = run({
#                 "queryString": query,
#                 "configFileName": f"{os.getcwd()}/{input.config}",
#                 "endpointsFileName": f"{os.getcwd()}/{input.endpoints}",
#                 "solutions": [],
#                 "assignments": [],
#                 "tpAliases": [],
#                 "runQuery": True})
#             if result["status"] != "OK":
#                 blacklist(wildcards, result["status"])
#         else:
#             print(f"skip because blacklisted for {reason}")
#             result = zero_result(reason)
#         df = extend_and_format_result(result, wildcards)
#         print_dataframe(df)
#         df.to_csv(str(output))
#
# rule run_rsa:
#     input: "queries/{workload}/{query}.sparql"
#     output: "{directory}/{workload,(largerdfbench|fedshop)-RSA}/{approach,RSA}/{query}.{run}.csv"
#     run:
#         cmd = subprocess.run(['arq', '--file', str(input), '--time', '--results', 'CSV'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#         stdout_args = cmd.stdout.decode('utf-8').split('\n')
#         stderr_args = cmd.stderr.decode('utf-8').split('\n')
#         exec_time = float(stderr_args[0].split(' ')[1])
#         num_solutions = len([x for x in stdout_args if x != '']) - 1
#         result = {
#             "status": "OK",
#             "sourceSelectionTime": 0,
#             "executionTime": exec_time,
#             "runtime": exec_time,
#             "numASKQueries": 0,
#             "numSolutions": num_solutions,
#             "numAssignments": 0,
#             "tpwss": 0,
#             "provenanceMappings": "[]",
#             "solutions": "[]",
#             "assignments": "[]",
#             "tpAliases": "[]"
#         }
#         df = extend_and_format_result(result, wildcards)
#         print_dataframe(df)
#         df.to_csv(str(output))
