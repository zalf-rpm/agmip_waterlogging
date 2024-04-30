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

    files_created = { "soil": False, "crop": False, "n2o": False }
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
            trt_no = custom_id.get("trt_no", None)
            soil_name = custom_id.get("soil_name", None)

            print(f"{os.path.basename(__file__)} received result {id}")

            soil_filepath = f"{path_to_out_dir}/out_soil.csv"
            if files_created["soil"]:
                soil_f = open(soil_filepath, "at", newline="", encoding="utf-8")
            else:
                soil_f = open(soil_filepath, "wt", newline="", encoding="utf-8")
                soil_f.write("Treatment_number,replicate_number,DATE,soil_layer_top_depth,soil_layer_bot_depth,soil_water_by_layer,soil_nitrogen_by_layer,NO3_soil_by_layer,NH4_soil_by_layer\n")
                soil_f.write("TRTNO,RP,DATE,SLDUB,SLDLB,SWLD,NSLD1,NOSLD,NHSLD\n")
            soil_writer = csv.writer(soil_f, delimiter=",")
            
            n2o_filepath = f"{path_to_out_dir}/out_n2o.csv"
            if files_created["n2o"]:
                n2o_f = open(n2o_filepath, "at", newline="", encoding="utf-8")
            else:
                n2o_f = open(n2o_filepath, "wt", newline="", encoding="utf-8")
                n2o_f.write("Treatment_number,replicate_number,DATE,N20 emission\n")
                n2o_f.write("TRTNO,RP,DATE,N20EM\n")
            n2o_writer = csv.writer(n2o_f, delimiter=",")
            
            crop_filepath = f"{path_to_out_dir}/out_crop.csv"
            if files_created["n2o"]:
                crop_f = open(crop_filepath, "at", newline="", encoding="utf-8")
            else:
                crop_f = open(crop_filepath, "wt", newline="", encoding="utf-8")
                crop_f.write("Treatment_number,replicate_number,DATE,growth_stage_Zadoks,growth_stage_Haun,tiller_number,leaf_number_as_haun_stg,canopy_length,green_area_index,leaf_area_index,stem_area_index,specific_leaf_area,tops_dry_weight,leaf_dry_weight,dead_leaf_dry_weight,stem_dry_weight,crown_dry_weight,ear_grain_chaff_dry_wt,chaff_dry_weight,grain_dry_weigh,harvest_index,nitrogen_harvest_index,grain_unit_dry_weight,ear_number,grain_number\n")
                crop_f.write("TRTNO,RP,DATE,GSTZD,GSTHD,TnoAD,LNUM,CLAD,GAID,LAID,SAID,SLAD,CWAD,LWAD,LDAD,SWAD,CRAD,EWAD,CHWAD,GWAD,HIAD,NHID,GWGD,EnoAD,GnoAD\n")
            crop_writer = csv.writer(crop_f, delimiter=",")   

            layers = {
                "CH5531001": [5,10,20,30,40,50,60,70,90,110,130,150,170,190,210],
                "LLWatelg01": [5,15,20,30,40,50,60,70,90,110,125]
            }.get(soil_name, None)
            if not layers:
                continue

            for data in msg.get("data", []):
                results = data.get("results", [])
                for vals in results:
                    layer_top_depth = 0
                    from_layer_index = 0
                    for layer_bottom_depth_cm in layers:
                        layer_index = min(max(0, int(layer_bottom_depth_cm / 10)), 20)
                        layer_mois_vals = vals["Mois"][from_layer_index:layer_index]
                        layer_avg_mois_val = sum(layer_mois_vals) / len(layer_mois_vals)
                        layer_n_vals = vals["N"][from_layer_index:layer_index]
                        layer_avg_n_val = sum(layer_n_vals) / len(layer_n_vals)
                        layer_no3_vals = vals["NO3"][from_layer_index:layer_index]
                        layer_avg_no3_val = sum(layer_no3_vals) / len(layer_no3_vals)
                        layer_nh4_vals = vals["NH4"][from_layer_index:layer_index]
                        layer_avg_nh4_val = sum(layer_nh4_vals) / len(layer_nh4_vals)
                        soil_writer.writerow([
                            trt_no, 1, vals["Date"], layer_top_depth, layer_bottom_depth_cm, 
                            layer_avg_mois_val, layer_avg_n_val, layer_avg_no3_val, layer_avg_nh4_val
                        ])

                        layer_top_depth = layer_bottom_depth_cm
                        from_layer_index = layer_index

            soil_f.close()
            crop_f.close()
            n2o_f.close()

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception: {e}")

    print(f"{os.path.basename(__file__)} exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
    