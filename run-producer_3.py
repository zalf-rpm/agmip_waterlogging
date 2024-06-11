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
import copy
import json
import os
import sys
import zmq

import monica_io3
import shared
import common

fbp_capnp = capnp.load("capnp_schemas/fbp.capnp", imports=[])


async def run_producer(server=None, port=None, calibration=False):

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "mode": "mbm-local-remote",
        "calibration": calibration,
        "server-port": port if port else "6666",
        "server": server if server else "localhost",
        "sim.json": os.path.join(os.path.dirname(__file__), "sim.json"),
        "crop.json": os.path.join(os.path.dirname(__file__), "crop_3.json"),
        "site.json": os.path.join(os.path.dirname(__file__), "site.json"),
        #"monica_path_to_climate_dir": "C:/Users/berg/Documents/GitHub/agmip_waterlogging/data",
        "monica_path_to_climate_dir": "C:/Users/palka/GitHub/agmip_waterlogging/data",
        #"monica_path_to_climate_dir": "/home/berg/GitHub/agmip_waterlogging/data",
        "path_to_data_dir": "./data/",
        "path_to_out": "out/",
        "treatments": "[]",
        "reader_sr": None,
    }
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    socket.connect("tcp://" + config["server"] + ":" + config["server-port"])

    treatments = json.loads(config["treatments"])

    with open(config["sim.json"]) as _:
        sim_json = json.load(_)

    calibration = config["calibration"]
    if calibration:
        #sim_json["output"]["events"] = sim_json["output"]["debug_events"]
        sim_json["output"]["events"] = sim_json["output"]["calibration_events"]
    #sim_json["output"]["events"] = sim_json["output"]["debug_events"]

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

    worksteps: list = copy.deepcopy(env_template["cropRotation"][0]["worksteps"])

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
                "PoreVolume": [float(line[2]), "m3/m3"],
                "FieldCapacity": [float(line[3]), "m3/m3"],
                "PermanentWiltingPoint": [float(line[5]), "m3/m3"],
                "Lambda": float(line[9]),
                "SoilBulkDensity": [float(line[10])*1000.0, "kg/m3"],
                "SoilOrganicCarbon": [float(line[11]), "%"],
                "Clay": [float(line[12])/100.0, "m3/m3"],
                "Sand": [float(line[14])/100.0, "m3/m3"],
                "Sceleton": [float(line[15]), "m3/m3"],
                "pH": float(line[18])
            }
            soil_profiles[soil_name].append(layer)

    trt_no_to_fertilizers = defaultdict(dict)
    with open(f"{config['path_to_data_dir']}/Fertilizers_3.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        for line in reader:
            fert_temp = copy.deepcopy(fert_template)  # fert_template --> crop json
            trt_no = int(line[0])  # trt_no
            fert_temp["date"] = line[2]  # date
            fert_temp["partition"][2] = line[3]  # fertilizer_material
            fert_temp["amount"][0] = float(line[6])  # N_in_applied_fertilizer
            trt_no_to_fertilizers[trt_no][fert_temp["date"]] = fert_temp

    trt_no_to_irrigation = defaultdict(dict)
    with open(f"{config['path_to_data_dir']}/Irrigations_3.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        for line in reader:
            irrig_temp = copy.deepcopy(irrig_template)
            trt_no = int(line[0])  # TRTNO
            irrig_temp["date"] = line[2]  # date
            irrig_temp["amount"][0] = float(line[4])  # irrig_amount_depth
            trt_no_to_irrigation[trt_no][irrig_temp["date"]] = irrig_temp

    trt_no_to_plant = defaultdict(dict)
    with open(f"{config['path_to_data_dir']}/Plantings_3.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader)
        next(reader)
        for line in reader:
            copy.deepcopy(irrig_template)
            trt_no = int(line[0])  # trt_no
            trt_no_to_plant[trt_no]["PDATE"] = line[2]  # date
            trt_no_to_plant[trt_no]["PLPOP"] = float(line[5])  # plant_pop_at_planting
            # trt_no_to_plant[trt_no]["PLPOE"] = line[6]
            trt_no_to_plant[trt_no]["PLRS"] = float(line[7])  # row_spacing
            trt_no_to_plant[trt_no]["PLDP"] = float(line[9])  # planting_depth
            

    trt_no_to_meta = defaultdict(dict)
    with open(f"{config['path_to_data_dir']}/Meta_3.csv") as file:
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
        reader = await conman.try_connect(config["reader_sr"], cast_as=fbp_capnp.Channel.Reader, retry_secs=1)

    iter_count = 0
    while True:
        if calibration:
            msg = await reader.read()
            # check for end of data from in port
            if msg.which() == "done":
                break

            # with open(config["path_to_out"] + "/spot_setup.out", "a") as _:
            #    _.write(f"{datetime.now()} connected\n")

            try:
                in_ip = msg.value.as_struct(fbp_capnp.IP)
                s: str = in_ip.content.as_text()
                params: dict = json.loads(s)  # keys: MaxAssimilationRate, AssimilateReallocation, RootPenetrationRate

                sowing_ws: dict = worksteps[0]
                ps = sowing_ws["crop"]["cropParams"]
                cps = env_template["params"]["userCropParameters"]
                for pname, pval in params.items():
                    pname_arr = pname.split("_")
                    i = j = None
                    if len(pname_arr) >= 2:
                        pname = pname_arr[0]
                        i = int(pname_arr[1]) - 1  # for the user we start counting at 1
                        if len(pname_arr) >= 3:
                            j = int(pname_arr[2]) - 1  # for the user we start counting at 1
                    if pname in ps["species"] or ("=" in ps["species"] and pname in ps["species"]["="]):
                        old_val = ps["species"][pname]
                        if isinstance(old_val, list) and len(old_val) > 1 and isinstance(old_val[1], str):
                            ps["species"][pname] = old_val[0]
                        if i is not None:
                            if len(ps["species"][pname]) > i:
                                if j is not None:
                                    if len(ps["species"][pname][i]) > j:
                                        ps["species"][pname][i][j] = pval
                                else:
                                    ps["species"][pname][i] = pval
                        else:
                            ps["species"][pname] = pval
                    elif pname in ps["cultivar"] or ("=" in ps["cultivar"] and pname in ps["cultivar"]["="]):
                        old_val = ps["cultivar"][pname]
                        if isinstance(old_val, list) and len(old_val) > 1 and isinstance(old_val[1], str):
                            ps["cultivar"][pname] = old_val[0]
                        if i is not None:
                            if len(ps["cultivar"][pname]) > i:
                                if j is not None:
                                    if len(ps["cultivar"][pname][i]) > j:
                                        ps["cultivar"][pname][i][j] = pval
                                else:
                                    ps["cultivar"][pname][i] = pval
                        else:
                            ps["cultivar"][pname] = pval
                    elif pname in cps or ("=" in cps and pname in cps["="]):
                        old_val = cps[pname]
                        if isinstance(old_val, list) and len(old_val) > 1 and isinstance(old_val[1], str):
                            cps[pname] = old_val[0]
                        if i is not None:
                            if len(cps[pname]) > i:
                                if j is not None:
                                    if len(cps[pname][i]) > j:
                                        cps[pname][i][j] = pval
                                else:
                                    cps[pname][i] = pval
                        else:
                            cps[pname] = pval
            except Exception as e:
                print(f"{os.path.basename(__file__)} exception: {e}")
                raise e

        no_of_trts = 0
        for trt_no, meta in trt_no_to_meta.items():
            if len(treatments) > 0 and trt_no not in treatments:
                continue

            env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
            env_template["pathToClimateCSV"] = \
                f"{config['monica_path_to_climate_dir']}/Weather_daily_{meta['WST_ID']}.csv"

            env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profiles[meta['SOIL_ID']]
            #env_template["params"]["siteParameters"]["SoilProfileParameters"] = site_json[meta['SOIL_ID']]


            env_template["params"]["siteParameters"]["HeightNN"] = float(meta["FLELE"])
            env_template["params"]["siteParameters"]["Latitude"] = float(meta["FL_LAT"])
            env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"]["CropSpecificMaxRootingDepth"] = float(meta["SLRTD"])
            #env_template["params"]["siteParameters"]["Slope"] = float(site["Slope"])

            # complete crop rotation
            dates = set()
            dates.update(trt_no_to_fertilizers[trt_no].keys())
            dates.update(trt_no_to_irrigation[trt_no].keys())

            worksteps_copy = copy.deepcopy(worksteps)
            worksteps_copy[0]["date"] = trt_no_to_plant[trt_no]["PDATE"]
            ld = worksteps_copy[-1]["latest-date"]
            worksteps_copy[-1]["latest-date"] = f"{int(trt_no_to_plant[trt_no]['PDATE'][:4])+1}{ld[4:]}"
            worksteps_copy[0]["PlantDensity"] = int(trt_no_to_plant[trt_no]["PLPOP"])

            for date in sorted(dates):
                if date in trt_no_to_fertilizers[trt_no]:
                    worksteps_copy.insert(-1, copy.deepcopy(trt_no_to_fertilizers[trt_no][date]))
                if date in trt_no_to_irrigation[trt_no]:
                    worksteps_copy.insert(-1, copy.deepcopy(trt_no_to_irrigation[trt_no][date]))

            env_template["cropRotation"][0]["worksteps"] = worksteps_copy

            env_template["customId"] = {
                "nodata": False,
                "trt_no": int(trt_no),
                "soil_name": meta["SOIL_ID"]
            }
            socket.send_json(env_template)
            #with open(f"out/env_template_trt_no-{trt_no}_iter_count-{iter_count}.json", "w") as _:
            #    json.dump(env_template, _, indent=2)
            #iter_count += 1
            no_of_trts += 1
            print(f"{os.path.basename(__file__)} sent job {no_of_trts}")

        # send done message
        env_template["customId"] = {
            "no_of_trts": no_of_trts,
            "nodata": True,
            "soil_name": meta["SOIL_ID"]
        }
        socket.send_json(env_template)
        print(f"{os.path.basename(__file__)} done")

        if not calibration:
            break


if __name__ == "__main__":
    asyncio.run(capnp.run(run_producer()))
