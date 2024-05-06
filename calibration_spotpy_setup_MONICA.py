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
from datetime import datetime
import json
import numpy as np
import os
from pathlib import Path
import spotpy

fbp_capnp = capnp.load("capnp_schemas/fbp.capnp", imports=[])


class spot_setup(object):
    def __init__(self, user_params, observations, observations_order, prod_writer, cons_reader, path_to_out):
        self.user_params = user_params
        self.params = []
        self.observations = observations
        self.observations_order = observations_order
        self.prod_writer = prod_writer
        self.cons_reader = cons_reader
        self.path_to_out_file = path_to_out + "/spot_setup.out"

        if not os.path.exists(path_to_out):
            try:
                os.makedirs(path_to_out)
            except OSError:
                print("spot_setup.__init__: Couldn't create dir:", path_to_out, "!")

        with open(self.path_to_out_file, "a") as _:
            _.write(f"observations: {self.observations}\n")

        for par in user_params:
            par_name = par["name"]
            if "array" in par:
                array_indices = "_".join(par["array"])
                par["name"] = f"{par_name}_{array_indices}"  # spotpy does not allow two parameters to have the same name
                del par["array"]
            if "derive_function" not in par:  # spotpy does not care about derived params
                self.params.append(spotpy.parameter.Uniform(**par))

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector):
        # vector = MaxAssimilationRate, AssimilateReallocation, RootPenetrationRate
        msg_content = dict(zip(vector.name, vector))
        out_ip = fbp_capnp.IP.new_message(content=json.dumps(msg_content))
        loop = asyncio.get_running_loop()
        f = asyncio.run_coroutine_threadsafe(self.prod_writer.write(value=out_ip), loop)
        with open(self.path_to_out_file, "a") as _:
            _.write(f"{datetime.now()} sent params to monica setup: {vector}\n")
        print("sent params to monica setup:", vector, flush=True)

        r = f.result()
        f2 = asyncio.run_coroutine_threadsafe(self.cons_reader.read(), loop)
        msg = f2.result()#asyncio.wait(self.cons_reader.read())
        # check for end of data from in port
        if msg.which() == "done":
            return

        in_ip = msg.value.as_struct(fbp_capnp.IP)
        s: str = in_ip.content.as_text()
        trt_no_to_output_name_to_result = json.loads(s)

        #with open(self.path_to_out_file, "a") as _:
        #    _.write(f"{datetime.now()} jsons loaded cal-sp-set-M\n")
        # print("received monica results:", country_id_and_year_to_avg_yield, flush=True)

        # remove all simulation results which are not in the observed list
        sim_list = []
        for trt_no in sorted(trt_no_to_output_name_to_result.keys()):
            output_name_to_result = trt_no_to_output_name_to_result[trt_no]
            for output_name in self.observations_order:
                if output_name in output_name_to_result:
                    sim_list.append(output_name_to_result[output_name])
                else:
                    sim_list.append(np.nan)

        #with open(self.path_to_out_file, "a") as _:
        #    _.write(f"{datetime.now()} simulation and observation matchedcal-sp-set-M\n\n")

        print("len(sim_list):", len(sim_list), "== len(self.obsservations):", len(self.observations), flush=True)
        with open(self.path_to_out_file, "a") as _:
            #_.write(f"received monica results: {country_id_and_year_to_avg_yield}\n")
            _.write(f"{datetime.now()}  len(sim_list): {len(sim_list)} == len(self.observations): {len(self.observations)}\n")
            #_.write(f"sim_list: {sim_list}\n")
            #_.write(f"obs_list: {self.obs_flat_list}\n")
        # besides the order the length of observation results and simulation results should be the same
        return sim_list

    def evaluation(self):
        return self.observations

    def objectivefunction(self, simulation, evaluation):
        return spotpy.objectivefunctions.rmse(evaluation, simulation)
