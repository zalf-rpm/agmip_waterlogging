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

from collections import defaultdict
import csv
import copy
import json
import os
import sys
import zmq

import monica_io3
import shared


def run_producer(server=None, port=None):

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "server-port": port if port else "6666",
        "server": server if server else "localhost",
        "sim.json": os.path.join(os.path.dirname(__file__), "sim.json"),
        "crop.json": os.path.join(os.path.dirname(__file__), "crop.json"),
        "site.json": os.path.join(os.path.dirname(__file__), "site.json"),
        "monica_path_to_climate_dir": "C:/Users/berg/Documents/GitHub/agmip_waterlogging/data",
        "path_to_data_dir": "./data/",
    }
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    socket.connect("tcp://" + config["server"] + ":" + config["server-port"])

    with open(config["sim.json"]) as _:
        sim_json = json.load(_)

    with open(config["site.json"]) as _:
        site_json = json.load(_)

    with open(config["crop.json"]) as _:
        crop_json = json.load(_)
    
    fert_template = crop_json.pop("fert_template")
    irrig_template = crop_json.pop("irrig_template")

    env_template = monica_io3.create_env_json_from_json_config({
        "crop": crop_json,
        "site": site_json,
        "sim": sim_json,
        "climate": ""  # climate_csv
    })

    soil_profiles = defaultdict(list)
    soil_csv_path = f"{config['path_to_data_dir']}/Soil_layers.csv"
    print(f"{os.path.basename(__file__)} CSV path:", soil_csv_path)
    with open(soil_csv_path) as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        prev_depth_m = 0
        for line in reader:
            soil_id = line[0]
            current_depth_m = float(line[1])/100.0
            thickness = current_depth_m - prev_depth_m
            prev_depth_m = current_depth_m
            layer = {
                "Thickness": [thickness, "m"],
                "PoreVolume": [float(line[2]), "m3/m3"],
                "FieldCapacity": [(float(line[3]), "m3/m3"],
                "PermanentWiltingPoint": [float(line[5]), "m3/m3"],
                "Lambda": float(line[9]),
                "SoilBulkDensity": [float(line[10])*1000.0, "kg/m3"],
                "SoilOrganicCarbon": [float(line[11]), "%"],
                "Clay": [float(line[12])/100.0, "m3/m3"],
                "Sand": [float(line[14])/100.0, "m3/m3"],
                #"Sceleton": [float(line[15]), "m3/m3"],
                "pH": float(line[18])
            }
            soil_profiles[soil_id].append(layer)
        

    trt_no_to_fertilizers = defaultdict(dict)
    fert_csv_path = f"{config['path_to_data_dir']}/Fertilizers.csv"
    print(f"{os.path.basename(__file__)} CSV path:", fert_csv_path)
    with open(fert_csv_path) as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        for line in reader:
            fert_temp = copy.deepcopy(fert_template)
            trt_no = int(line[0])
            fert_temp["date"] = line[2]
            fert_temp["partition"][2] = line[3]
            fert_temp["amount"][0] = float(line[6])
            trt_no_to_fertilizers[trt_no][fert_temp["date"]] = fert_temp

    trt_no_to_irrigation = defaultdict(dict)


    trt_no_to_plant = defaultdict(dict)

    trt_no_to_meta = {}


    no_of_trts = 0
    for trt_no, meta in trt_no_to_meta.items():

        env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
        env_template["pathToClimateCSV"] = \
            f"{config['monica_path_to_climate_dir']}/Weather_daily_{meta['WST_ID']}.csv"

        env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profiles[meta['SOIL_ID']]

        env_template["params"]["siteParameters"]["HeightNN"] = float(meta["FLELE"])
        env_template["params"]["siteParameters"]["Latitude"] = float(meta["FL_LAT"])
        #env_template["params"]["siteParameters"]["Slope"] = float(site["Slope"])

        env_template["customId"] = {
            "nodata": False,
            "trt_no": trt_no,
        }
        socket.send_json(env_template)
        no_of_trts += 1
        print(f"{os.path.basename(__file__)} sent job {no_of_trts}")

    # send done message
    env_template["customId"] = {
        "no_of_trts": no_of_trts,
        "nodata": True,
    }
    socket.send_json(env_template)
    print(f"{os.path.basename(__file__)} done")


if __name__ == "__main__":
    run_producer()