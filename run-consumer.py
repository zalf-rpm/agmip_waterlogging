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
from datetime import datetime, timedelta
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

    daily_filepath = f"{path_to_out_dir}/Ex_1a_Daily_MONICA_calib_Results.csv"
    daily_f = open(daily_filepath, "wt", newline="", encoding="utf-8")
    daily_f.write(
        "Tret: 1 to 13,Day after planting,Zadocks phenology stage,Total above biomass,Leaf Area Index,Daily transpiration,Actual evapotranspiration,Runoff,Deep Percolation,N Leaching,Soil Water Content_layer_1,Soil Water Content_layer_2,Soil Water Content_layer_3,Soil Water Content_layer_4,Soil Water Content_layer_5,Soil Water Content_layer_6,Soil Water Content_layer_7,Soil Water Content_layer_8,Soil Water Content_layer_9,Soil Water Content_layer_10,Soil Water Content_layer_11,Soil Water Content_layer_12,Soil Water Content_layer_13,Soil Water Content_layer_14,Soil Water Content_layer_15\n")
    daily_f.write(
        "Treatment,DAP,ZDPH,CWAD,LAI,TRANS,ETa,Roff,DPER,NLEA,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC,SWC\n")
    daily_f.flush()
    daily_writer = csv.writer(daily_f, delimiter=",")

    crop_filepath = f"{path_to_out_dir}/Ex_1a_MONICA_calib_Results.csv"
    crop_f = open(crop_filepath, "wt", newline="", encoding="utf-8")
    crop_f.write(
        ",Grain yield,Grain number ,grain unit weight ,Maximum Leaf Area Index,Total biomass at maturity ,Root Biomass\n")
    crop_f.write("Treatment,GWAD,G#AD,GWGD,LAID,CWAD,RWAD\n")
    crop_f.flush()
    crop_writer = csv.writer(crop_f, delimiter=",")

    no_of_trts_to_receive = None
    no_of_trts_received = 0
    while no_of_trts_to_receive != no_of_trts_received:
        try:
            msg: dict = socket.recv_json()

            if msg.get("errors", []):
                print(f"{os.path.basename(__file__)} received errors: {msg['errors']}")
                continue

            custom_id = msg.get("customId", {})
           
            if custom_id.get("nodata", False):
                no_of_trts_to_receive = custom_id.get("no_of_trts", None)
                print(f"{os.path.basename(__file__)} received nodata=true -> done")
                continue

            no_of_trts_received += 1
            trt_no = custom_id.get("trt_no", None)
            soil_name = custom_id.get("soil_name", None)

            print(f"{os.path.basename(__file__)} received result trt_no: {trt_no}")

            # [(layer_bottom_depth_cm, layer_index), ...]
            layers = {
                "CH5531001": [(5, 0), (10, 0), (20, 1), (30, 2), (40, 3), (50, 4), (60, 5), (70, 6),
                              (90, (7, 8)), (110, (9, 10)), (130, (11, 12)), (150, (13, 14)),
                              (170, (15, 16)), (190, (17, 18)), (210, 19)],
                "LLWatelg01": [(5, 0), (15, (0, 1)), (20, 1), (30, 2), (40, 3), (50, 4), (60, 5),
                               (70, 6), (90, (7, 8)), (110, (9, 10)), (125, (11, 13))]
            }.get(soil_name, None)
            if not layers:
                continue

            data: dict = msg["data"][0]
            results: list = data["results"]
            sowing_date = None
            for vals in results:
                if not sowing_date:
                    sowing_date = vals["Date"]
                current_date = vals["Date"]
                dap = (datetime.fromisoformat(current_date) - datetime.fromisoformat(sowing_date)).days
                swcs = []
                for layer_bottom_depth_cm, layer_indices in layers:
                    from_layer_index = layer_index = layer_indices if isinstance(layer_indices, int) else layer_indices[0]
                    to_layer_index = layer_indices if isinstance(layer_indices, int) else layer_indices[1]
                    layer_swc_vals = vals["SWC"][from_layer_index:to_layer_index+1]
                    swcs.append(sum(layer_swc_vals) / len(layer_swc_vals))
                row = [trt_no, dap, -1, vals["CWAD"], vals["LAI"], vals["TRANS"], vals["ETa"],
                       vals["Roff"], vals["DPER"], vals["NLEA"]] + swcs
                daily_writer.writerow(row)

            data: dict = msg["data"][1]
            vals: dict = data["results"][0]
            row = [trt_no, -1, -1, -1, vals["LAID"], vals["CWAD"], vals["RWAD"]]
            crop_writer.writerow(row)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception: {e}")

    daily_f.close()
    crop_f.close()

print(f"{os.path.basename(__file__)} exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
    