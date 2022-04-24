
import argparse
import pprint
import re
import socket
import time
from datetime import timedelta
from itertools import groupby

import paramiko
import yaml
from yaml.loader import SafeLoader

"""
Arguments (cli): 
    -n: dry-run key, if enabled, then script does noth optimizes tables
    -db_names: up to n db names that script is going to optimize
Important variables:
    SERVERS_PATH: path to .yml config file where listed all servers in format user@host@
    command: path to the sh script on the dedicated server
    metrics: .....
Description:
    Connect via ssh to each server listed in SERVERS_PATH. For each database given in cli
    runs OPTIMIZE TABLE command locally. Collects statistics for each db:
        How much tables was able to optimize
        How much time takes server to optimize all db listed in servers.yml'
        Total time spent
    metrics format:
        hostname = {
            db: {OK: count, FAIL: count},
            time: time spent on server,
            }
"""

parser = argparse.ArgumentParser(
    description="Runs OPTIMIZE TABLE querry for each database declared in command line. Collects metrics for each server: sucess/fail, total time", 
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-n", "--dry-run", action="store_true", help="Do not apply SQL: OPTIMIZE_TABLE funtion.")
parser.add_argument("db_names", nargs="+", help="Databases that going to be optimized. At least 1 name required.")
args = parser.parse_args()

PORT = 22
SERVERS_PATH = "servers.yml"
PASSWORD = "pass"
DB_NAMES_STR = " ".join(args.db_names) if not args.dry_run else "-n " + " ".join(args.db_names)
COMMAND = "/path_to_script/./optimize_table.sh %s"

def process_metrics(metrics, server, lines, start_time, error_message=None):
    """
    Description:
        This function collect metrics of the output message for each server, where
        databases are delimited with "==========".
        Message example:
        ==========
        db_name1.movies
        note     : Table does not support optimize, doing recreate + analyze instead
        status   : OK
        db_name1.test_table
        note     : Table does not support optimize, doing recreate + analyze instead
        status   : OK
        ==========
        mysqlcheck: Got error: 1049: Unknown database 'db_name222222' when selecting the database
    """
    metrics[server] = {}

    if error_message:
        metrics[server] = error_message
        return

    unknown_db_error = "mysqlcheck: Got error: 1049: Unknown database"
    unknown_db_name_start = "database '"
    unknown_db_name_end = "' when"

    db_delimiter = '==========\n'
    db_chunks = (list(g) for k,g in groupby(lines, key=lambda x: x != db_delimiter) if k)  # [1, 2, "delimiter", 4, 5] => [[1, 2], [3, 4]]

    for index, db_mesages in enumerate(db_chunks):
        if unknown_db_error in db_mesages[0]:
            result = (db_mesages[0].split(unknown_db_name_start))[1].split(unknown_db_name_end)[0]
            metrics[server][f"Unknown db {index}"] = result # Unique name for each error, otherwise would be overwritten
        else:
            
            db_name = db_mesages[0].split(".")[0] # always first elem in message, fromat "db_name.table_name"
            count_ok = 0
            count_fail = 0
            for line in db_mesages:
                if "status" in line:
                    status_msg = line.replace(" ", "").replace("\n", "").split(":")
                    if status_msg[1] == "OK":
                        count_ok += 1
                    else:
                        count_fail += 1
            metrics[server][db_name] = {"OK": count_ok, "FAIL": count_fail}

    metrics[server]["time"] = str(timedelta(seconds=(time.time() - start_time)))

def main():
    total_time_start = time.time()

    with open("servers.yml") as f:
        servers = yaml.load(f, Loader=SafeLoader)["servers"]

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    metrics = {}
    for server in servers:
        server_start_time = time.time()

        username, host = server.split("@")
        lines = None
        try:
            ssh.connect(host, PORT, username, PASSWORD)
            stdin, stdout, stderr = ssh.exec_command(COMMAND % DB_NAMES_STR)
            lines = stdout.readlines()

            process_metrics(metrics, server, lines, server_start_time, error_message=None)
            ssh.close()
        except paramiko.ssh_exception.AuthenticationException:
            process_metrics(metrics=metrics, server=server, lines=[], start_time=server_start_time, error_message="Invalid user")

        except socket.gaierror:
            process_metrics(metrics=metrics, server=server, lines=[], start_time=server_start_time, error_message="Invalid host")

    pprint.pprint(metrics)
    total_time = str(timedelta(seconds=(time.time() - total_time_start)))
    print("Total time: " + total_time)

if __name__ == "__main__":
    main()



