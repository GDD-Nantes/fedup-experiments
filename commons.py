import os
import re
import sys
import time
import json
import click
import signal
import pandas
import requests
import subprocess
import SPARQLWrapper


def get_pid(port):
    pid = None
    try:
        output = subprocess.check_output(["lsof", "-t", "-i", f":{port}", "-sTCP:LISTEN"])
        pid = int(output.strip())
    except subprocess.CalledProcessError:
        pass
    return pid


def load_query(query_file):
    with open(query_file, "r") as file:
        return str(file.read())


def load_endpoints(endpoints_file):
    with open(endpoints_file, "r") as file:
        return [str(endpoint).strip() for endpoint in file.readlines()]


def write_solutions(solutions, output):
    if output is not None:
        with open(output, "w") as file:
            file.write(json.dumps(solutions))


def write_metrics(metrics, output):
    if output is not None:
        pandas.DataFrame.from_dict(metrics).to_csv(output)


def send_signal(port, signal):
    pid = get_pid(port)
    if pid is None:
        return
    print(f"Stopping process with pid {pid}")
    os.kill(pid, signal)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("port", type=click.INT)
@click.option("--soft/--kill", type=click.BOOL, default=True)
def stop_process(port, soft):
    send_signal(port, signal.SIGTERM if soft else signal.SIGKILL)


@cli.command()
@click.option("--config", type=click.Path(exists=True, dir_okay=False), default="virtuoso-opensource-7.2.11/var/lib/virtuoso/db/fedup.ini")
@click.option("--restart", type=click.BOOL, default=False)
@click.option("--home", type=click.Path(exists=True, file_okay=False), default="virtuoso-opensource-7.2.11")
def start_virtuoso(config, restart, home):
    with open(config, "r") as file:
        s = file.read().split("[HTTPServer]")[1]
        port = int(re.findall(r"ServerPort\s*=\s([0-9]+)", s)[0])
    if restart:
        send_signal(port, signal.SIGTERM)
    if get_pid(port) is not None:
        print("Virtuoso is running")
        return
    print("running Virtuoso")
    subprocess.Popen(
        [f"{home}/bin/virtuoso-t", "+configfile", config, "+foreground"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    time.sleep(20)


@cli.command()
@click.option("--config", type=click.Path(exists=True, dir_okay=False), default="virtuoso-opensource-7.2.7/var/lib/virtuoso/db/fedup.ini")
@click.option("--soft/--kill", type=click.BOOL, default=True)
def stop_virtuoso(config, soft):
    with open(config, "r") as file:
        s = file.read().split("[HTTPServer]")[1]
        port = int(re.findall(r"ServerPort\s*=\s([0-9]+)", s)[0])
    send_signal(port, signal.SIGTERM if soft else signal.SIGKILL)


@cli.command()
@click.option("--port", type=click.INT, default=3030)
@click.option("--restart", type=click.BOOL, default=False)
@click.option("--home", type=click.Path(exists=True, file_okay=False), default="apache-jena-fuseki-4.9.0")
def start_fuseki(port, restart, home):
    if restart:
        send_signal(port, signal.SIGTERM)
    if get_pid(port) is not None:
        print("Apache Fuseki is running")
        return
    print("running Apache Fuseki")
    command_line_process = subprocess.Popen(
        [f"{home}/fuseki-server", "--mem", "--port", str(port), "/sparql"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    time.sleep(10)


@cli.command()
@click.option("--port", type=click.INT, default=3030)
@click.option("--soft/--kill", type=click.BOOL, default=True)
def stop_fuseki(port, soft):
    send_signal(port, signal.SIGTERM if soft else signal.SIGKILL)


@cli.command()
@click.option("--port", type=click.INT, default=8080)
@click.option("--restart", type=click.BOOL, default=False)
def start_fedup(port, restart):
    if restart:
        send_signal(port, signal.SIGTERM)
    if get_pid(port) is not None:
        print("FedUP is running")
        return
    print("running FedUP")
    subprocess.Popen([
            "mvn", "spring-boot:run", "-pl", "fedup",
            f"-Dserver.port={port}"
            " -Dspring-boot.run.jvmArguments=\"-Xms4096M -Xmx8192M -XX:TieredStopAtLevel=4\""],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    time.sleep(10)


@cli.command()
@click.option("--port", type=click.INT, default=8080)
@click.option("--soft/--kill", type=click.BOOL, default=True)
def stop_fedup(port, soft):
    send_signal(port, signal.SIGTERM if soft else signal.SIGKILL)


@cli.command()
@click.argument("query", type=click.Path(exists=True, dir_okay=False))
@click.option("--port", type=click.INT, default=3030)
@click.option("--metrics-output", type=click.Path())
@click.option("--solutions-output", type=click.Path())
def run_rsa_query(query, port, metrics_output, solutions_output):
    sparql = SPARQLWrapper.SPARQLWrapper(f"http://localhost:{port}/sparql")

    sparql.setReturnFormat(SPARQLWrapper.JSON)
    sparql.setQuery(load_query(query))

    try:
        start_time = time.time()
        solutions = sparql.queryAndConvert()
        end_time = time.time()
        metrics = {
            "status": ["ok"],
            "reason": [None],
            "sourceSelectionTime": [0],
            "executionTime": [(end_time - start_time) * 1000],
            "runtime": [(end_time - start_time) * 1000],
            "numASKQueries": [0],
            "numSolutions": [len(solutions["results"]["bindings"])],
            "numAssignments": [0],
            "tpwss": [0]
        }
    except Exception as error:
        metrics = {"status": ["error"], "reason": [error]}
        solutions = []

    write_metrics(metrics, metrics_output)
    write_solutions(solutions, solutions_output)

    sys.exit(0 if metrics["status"] == "ok" else 1)


@cli.command()
@click.argument("query", type=click.Path(exists=True, dir_okay=False))
@click.argument("endpoints", type=click.Path(exists=True, dir_okay=False))
@click.argument("config", type=click.Path(exists=True, dir_okay=False))
@click.option("--port", type=click.INT, default=8080)
@click.option("--metrics-output", type=click.Path())
@click.option("--solutions-output", type=click.Path())
def run_query(query, endpoints, config, port, metrics_output, solutions_output):

    def signal_handler(signal, frame):  # used to handle the signal sent by the timeout command
        write_metrics({"status": ["timeout"], "reason": [None]}, metrics_output)
        write_solutions([], solutions_output)
        sys.exit(124)

    signal.signal(signal.SIGTERM, signal_handler)

    try:
        response = requests.post(f"http://localhost:{port}/fedSparql", json={
            "queryString": load_query(query),
            "configFileName": config,
            "endpoints": load_endpoints(endpoints),
            "runQuery": True
        })
        result = json.loads(response.content.decode("utf-8"))
        metrics = {k: [v] for k, v in result.items() if k in [
            "sourceSelectionTime",
            "executionTime",
            "runtime",
            "numASKQueries",
            "numSolutions",
            "numAssignments",
            "tpwss"
        ]} | {"status": ["ok"], "reason": [None]}
        solutions = result["solutions"]
    except Exception as error:
        metrics = {"status": ["error"], "reason": [error]}
        solutions = []

    write_metrics(metrics, metrics_output)
    write_solutions(solutions, solutions_output)

    sys.exit(0 if metrics["status"] == "ok" else 1)


if __name__ == "__main__":
    cli()
