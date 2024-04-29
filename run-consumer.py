#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import os
import sys
from collections import defaultdict

import zmq

import shared


def run_consumer(server=None, port=None):
    config = {
        "port": port if port else "7777",
        "server": server if server else "localhost",
        "path-to-output-dir": "./out",
    }

    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://" + config["server"] + ":" + config["port"])

    socket.RCVTIMEO = 6000

    path_to_out_dir = config["path-to-output-dir"]
    if not os.path.exists(path_to_out_dir):
        try:
            os.makedirs(path_to_out_dir)
        except OSError:
            print(f"{os.path.basename(__file__)} Couldn't create dir {path_to_out_dir}! Exiting.")
            exit(1)

    no_of_trts_to_receive = None
    no_of_trts_received = 0
    while no_of_trts_to_receive != no_of_trts_received:
        try:
            msg = socket.recv_json()

            if msg.get("errors", []):
                print(f"{os.path.basename(__file__)} received errors: {msg['errors']}")
                continue

            custom_id = msg.get("customId", {})
           
            if custom_id.get("nodata", False):
                no_of_trts_to_receive = custom_id.get("no_of_trts", None)
                print(f"{os.path.basename(__file__)} received nodata=true -> done")
                continue

            no_of_trts_received += 1
            id = custom_id.get("id", None)

            print(f"{os.path.basename(__file__)} received result {id}")

            filepath = f"{path_to_out_dir}/out_{id}.csv"
            with open(filepath, "wt", newline="", encoding="utf-8") as _:
                writer = csv.writer(_, delimiter=",")
                writer.writerow(["id", "year", "doy_stage_4", "doy_stage_6"])
                #writer.writerow(["", "", "", ""])

                year_to_row = defaultdict(lambda: [id, None, None, None])
                for data in msg.get("data", []):
                    results = data.get("results", [])
                    for vals in results:
                        if "year_stage_4" in vals and "doy_stage_4" in vals:
                            year_to_row[int(vals["year_stage_4"])][1] = vals["year_stage_4"]
                            year_to_row[int(vals["year_stage_4"])][2] = vals["doy_stage_4"]
                        if "year_stage_6" in vals and "doy_stage_6" in vals:
                            year_to_row[int(vals["year_stage_6"])][3] = vals["doy_stage_6"]
                for year in sorted(year_to_row.keys()):
                    writer.writerow(year_to_row[year])

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception: {e}")

    print(f"{os.path.basename(__file__)} exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
    