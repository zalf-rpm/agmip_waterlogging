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

import asyncio
import capnp
from collections import defaultdict
import csv
from datetime import datetime
import json
import os
import sys
import zmq

import common
import monica_io3

fbp_capnp = capnp.load("capnp_schemas/fbp.capnp", imports=[])

PATHS = {
    "remoteConsumer-remoteMonica": {
        "path-to-data-dir": "./data/",
        "path-to-output-dir": "/out/out/",
        "path-to-csv-output-dir": "/out/csv-out/"
    }
}


async def run_consumer(server=None, port=None):
    """collect data from workers"""

    config = {
        "mode": "remoteConsumer-remoteMonica",
        "port": port if port else "7777",  # local 7778,  remote 7777
        "server": server if server else "localhost",  # "login01.cluster.zalf.de",
        "writer_sr": None,
        "path_to_out": "out/",
        "timeout": 600000  # 10min
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    path_to_out_file = config["path_to_out"] + "/consumer.out"
    if not os.path.exists(config["path_to_out"]):
        try:
            os.makedirs(config["path_to_out"])
        except OSError:
            print("run-calibration-consumer.py: Couldn't create dir:", config["path_to_out"], "!")
    with open(path_to_out_file, "a") as _:
        _.write(f"config: {config}\n")

    context = zmq.Context()
    socket = context.socket(zmq.PULL)

    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = config["timeout"]

    trt_no_to_output_name_to_result = defaultdict(dict)

    conman = common.ConnectionManager()
    writer = await conman.try_connect(config["writer_sr"], cast_as=fbp_capnp.Channel.Writer, retry_secs=1)  #None

    no_of_trts_received = 0
    no_of_trts_expected = None

    while True:
        try:
            msg: dict = socket.recv_json()  # encoding="latin-1"

            custom_id = msg["customId"]
            if "no_of_trts" in custom_id:
                no_of_trts_expected = custom_id["no_of_trts"]
            else:
                no_of_trts_received += 1

                #with open(path_to_out_file, "a") as _:
                #    _.write(f"received result customId: {custom_id}\n")
                #print("received result customId:", custom_id)

                trt_no = custom_id["trt_no"]

                #write_monica_out(trt_no, msg)
                #continue

                for data in msg.get("data", []):
                    results = data.get("results", [])
                    for vals in results:
                        for output_name, val in vals.items():
                            trt_no_to_output_name_to_result[trt_no][output_name] = val

            if no_of_trts_expected == no_of_trts_received and writer:
                with open(path_to_out_file, "a") as _:
                    _.write(f"{datetime.now()} last expected env received\n")

                out_ip = fbp_capnp.IP.new_message(content=json.dumps(trt_no_to_output_name_to_result))
                await writer.write(value=out_ip)

                # reset and wait for next round
                trt_no_to_output_name_to_result.clear()
                no_of_trts_expected = None
                no_of_trts_received = 0

        except zmq.error.Again as _e:
            with open(path_to_out_file, "a") as _:
                _.write(f"no response from the server (with {socket.RCVTIMEO} ms timeout)\n")
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            continue
        except Exception as e:
            with open(path_to_out_file, "a") as _:
                _.write(f"Exception: {e}\n")
            print("Exception:", e)
            break

    #print("exiting run_consumer()")


def write_monica_out(trt_no, msg):
    path_to_out_dir = "out"
    if not os.path.exists(path_to_out_dir):
        try:
            os.makedirs(path_to_out_dir)
        except OSError:
            print("c: Couldn't create dir:", path_to_out_dir, "! Exiting.")
            exit(1)

    # with open("out/out-" + str(i) + ".csv", 'wb') as _:
    path_to_file = path_to_out_dir + "/trt_no-" + str(trt_no) + ".csv"
    with open(path_to_file, "w", newline='') as _:
        writer = csv.writer(_, delimiter=",")
        for data_ in msg.get("data", []):
            results = data_.get("results", [])
            orig_spec = data_.get("origSpec", "")
            output_ids = data_.get("outputIds", [])
            if len(results) > 0:
                writer.writerow([orig_spec.replace("\"", "")])
                for row in monica_io3.write_output_header_rows(output_ids,
                                                               include_header_row=True,
                                                               include_units_row=True,
                                                               include_time_agg=False):
                    writer.writerow(row)
                for row in monica_io3.write_output_obj(output_ids, results):
                    writer.writerow(row)
            writer.writerow([])
    print("wrote:", path_to_file)


if __name__ == "__main__":
    asyncio.run(capnp.run(run_consumer()))
