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

import capnp
from collections import defaultdict
import csv
import copy
import json
import os
import sys
import zmq

import monica_io3
import shared

fbp_capnp = capnp.load("capnp_schemas/fbp.capnp", imports=[])


def run_producer(server=None, port=None, calibration=False):

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "mode": "mbm-local-remote",
        "calibration": calibration,
        "server-port": port if port else "6666",
        "server": server if server else "localhost",
        "sim.json": os.path.join(os.path.dirname(__file__), "sim.json"),
        "crop.json": os.path.join(os.path.dirname(__file__), "crop.json"),
        "site.json": os.path.join(os.path.dirname(__file__), "site.json"),
        #"monica_path_to_climate_dir": "C:/Users/berg/Documents/GitHub/agmip_waterlogging/data",
        "monica_path_to_climate_dir": "C:/Users/giri/Documents/GitHub/agmip_waterlogging/data",
        #"monica_path_to_climate_dir": "/home/berg/GitHub/agmip_waterlogging/data",
        "path_to_data_dir": "./data/",
        "path_to_out": "out/",
    }
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    socket.connect("tcp://" + config["server"] + ":" + config["server-port"])

    with open(config["sim.json"]) as _:
        sim_json = json.load(_)

    if calibration:
        sim_json["events"] = sim_json["calibration_events"]

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
    with open(f"{config['path_to_data_dir']}/Soil_layers.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        prev_depth_m = 0
        prev_soil_name = None
        for line in reader:
            soil_name = line[0]
            if soil_name != prev_soil_name:
                prev_soil_name = soil_name
                prev_depth_m = 0
            current_depth_m = float(line[1])/100.0
            thickness = round(current_depth_m - prev_depth_m, 1)
            prev_depth_m = current_depth_m
            layer = {
                "Thickness": [thickness, "m"],
                #"PoreVolume": [float(line[2]), "m3/m3"],
                #"FieldCapacity": [float(line[3]), "m3/m3"],
                #"PermanentWiltingPoint": [float(line[5]), "m3/m3"],
                #"Lambda": float(line[9]),
                "SoilBulkDensity": [float(line[10])*1000.0, "kg/m3"],
                "SoilOrganicCarbon": [float(line[11]), "%"],
                "Clay": [float(line[12])/100.0, "m3/m3"],
                "Sand": [float(line[14])/100.0, "m3/m3"],
                #"Sceleton": [float(line[15]), "m3/m3"],
                #"pH": float(line[18])
            }
            soil_profiles[soil_name].append(layer)

    trt_no_to_fertilizers = defaultdict(dict)
    with open(f"{config['path_to_data_dir']}/Fertilizers.csv") as file:
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
    with open(f"{config['path_to_data_dir']}/Irrigations.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        for line in reader:
            irrig_temp = copy.deepcopy(irrig_template)
            trt_no = int(line[0])
            irrig_temp["date"] = line[2]
            irrig_temp["amount"][0] = float(line[4])
            trt_no_to_irrigation[trt_no][irrig_temp["date"]] = irrig_temp

    trt_no_to_plant = defaultdict(dict)
    with open(f"{config['path_to_data_dir']}/Plantings.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        for line in reader:
            copy.deepcopy(irrig_template)
            trt_no = int(line[0])
            trt_no_to_plant[trt_no]["PDATE"] = line[2]
            trt_no_to_plant[trt_no]["PLPOP"] = float(line[5])
            trt_no_to_plant[trt_no]["PLRS"] = float(line[7])
            trt_no_to_plant[trt_no]["PLDP"] = float(line[9])

    trt_no_to_meta = defaultdict(dict)
    with open(f"{config['path_to_data_dir']}/Meta.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        header = next(reader)
        for line in reader:
            for i, h in enumerate(header):
                trt_no_to_meta[int(line[0])][h] = line[i]

    # if calibration connect to reader for new parameters
    conman = reader = None
    if calibration:
        conman = common.ConnectionManager()
        reader = conman.try_connect(config["reader_sr"], cast_as=fbp_capnp.Channel.Reader, retry_secs=1)

    while True:
        if calibration:
            msg = reader.read().wait()
            # check for end of data from in port
            if msg.which() == "done":
                break

            # with open(config["path_to_out"] + "/spot_setup.out", "a") as _:
            #    _.write(f"{datetime.now()} connected\n")

            env_template = None
            try:
                in_ip = msg.value.as_struct(fbp_capnp.IP)
                s: str = in_ip.content.as_text()
                params = json.loads(s)  # keys: MaxAssimilationRate, AssimilateReallocation, RootPenetrationRate

                sowing_ws: dict = env_template["cropRotation"][0]["worksteps"][0]
                ps = sowing_ws["crop"]["cropParams"]
                for pname, pval in params.items():
                    pname_arr = pname.split("_")
                    i = None
                    if len(pname_arr) == 2:
                        pname = pname_arr[0]
                        i = int(pname_arr[1])
                    if pname in ps["species"]:
                        if i:
                            if len(ps["species"][pname]) < i:
                                ps["species"][pname][i] = pval
                        else:
                            ps["species"][pname] = pval
                    elif pname in ps["cultivar"]:
                        if i:
                            if len(ps["cultivar"][pname]) > i:
                                ps["cultivar"][pname][i] = pval
                        else:
                            ps["cultivar"][pname] = pval
            except Exception as e:
                print(f"{os.path.basename(__file__)} exception: {e}")
                raise e

        no_of_trts = 0
        for trt_no, meta in trt_no_to_meta.items():

            env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
            env_template["pathToClimateCSV"] = \
                f"{config['monica_path_to_climate_dir']}/Weather_daily_{meta['WST_NAME']}.csv"

            env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profiles[meta['SOIL_NAME']]

            env_template["params"]["siteParameters"]["HeightNN"] = float(meta["FLELE"])
            env_template["params"]["siteParameters"]["Latitude"] = float(meta["FL_LAT"])
            #env_template["params"]["siteParameters"]["Slope"] = float(site["Slope"])

            # complete crop rotation
            dates = set()
            dates.update(trt_no_to_fertilizers[trt_no].keys())
            dates.update(trt_no_to_irrigation[trt_no].keys())

            worksteps : list = env_template["cropRotation"][0]["worksteps"]
            worksteps[0]["date"] = trt_no_to_plant[trt_no]["PDATE"]
            ld = worksteps[-1]["latest-date"]
            worksteps[-1]["latest-date"] = f"{int(trt_no_to_plant[trt_no]['PDATE'][:4])+1}{ld[4:]}"
            for date in sorted(dates):
                if date in trt_no_to_fertilizers[trt_no]:
                    worksteps.insert(-1, trt_no_to_fertilizers[trt_no][date])
                if date in trt_no_to_irrigation[trt_no]:
                    worksteps.insert(-1, trt_no_to_irrigation[trt_no][date])

            env_template["customId"] = {
                "nodata": False,
                "trt_no": trt_no,
                "soil_name": meta['SOIL_NAME']
            }
            socket.send_json(env_template)
            no_of_trts += 1
            print(f"{os.path.basename(__file__)} sent job {no_of_trts}")

        # send done message
        env_template["customId"] = {
            "no_of_trts": no_of_trts,
            "nodata": True,
            "soil_name": meta['SOIL_NAME']
        }
        socket.send_json(env_template)
        print(f"{os.path.basename(__file__)} done")

        if not calibration:
            break


if __name__ == "__main__":
    run_producer()
