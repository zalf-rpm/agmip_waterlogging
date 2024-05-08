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
from threading import Thread

import capnp
from datetime import datetime
import json
import numpy as np
import os
from pathlib import Path
from queue import SimpleQueue
import spotpy

import common

fbp_capnp = capnp.load("capnp_schemas/fbp.capnp", imports=[])


async def send_receive(prod_writer_sr, prod_writer_queue: SimpleQueue, cons_reader_sr, cons_reader_queue: SimpleQueue):
    async with capnp.kj_loop():
        con_man = common.ConnectionManager()
        cons_reader = await con_man.try_connect(cons_reader_sr, cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
        prod_writer = await con_man.try_connect(prod_writer_sr, cast_as=fbp_capnp.Channel.Writer, retry_secs=1)

        while True:
            out_ip = prod_writer_queue.get()
            await prod_writer.write(value=out_ip)
            msg = await cons_reader.read()
            cons_reader_queue.put(msg.value.as_struct(fbp_capnp.IP))


def start_thread(prod_writer_sr, prod_writer_queue: SimpleQueue, cons_reader_sr, cons_reader_queue: SimpleQueue):
    asyncio.run(send_receive(prod_writer_sr, prod_writer_queue, cons_reader_sr, cons_reader_queue))


class spot_setup(object):
    def __init__(self, user_params, observations, observations_order, prod_writer_sr, cons_reader_sr, path_to_out):
        self.user_params = user_params
        self.params = []
        self.observations = observations
        self.observations_order = observations_order
        self.prod_writer_queue = SimpleQueue()
        self.cons_reader_queue = SimpleQueue()
        self.path_to_out_file = path_to_out + "/spot_setup.out"
        self.capnp_thread = Thread(target=start_thread, args=(prod_writer_sr, self.prod_writer_queue,
                                                              cons_reader_sr, self.cons_reader_queue))
        self.capnp_thread.start()

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
        self.prod_writer_queue.put(out_ip)
        #await self.prod_writer.write(value=out_ip)
        with open(self.path_to_out_file, "a") as _:
            _.write(f"{datetime.now()} sent params to monica setup: {vector}\n")
        print("sent params to monica setup:", vector, flush=True)

        in_ip = self.cons_reader_queue.get()
        #await self.cons_reader.read().wait()
        # check for end of data from in port
        #if msg.which() == "done":
        #    return

        #in_ip = msg.value.as_struct(fbp_capnp.IP)
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
                if output_name == "HIAM":
                    gwam = output_name_to_result.get("GWAM", None)
                    cwam = output_name_to_result.get("CWAM", None)
                    if gwam is not None and cwam is not None:
                        sim_list.append(gwam / cwam * 100.0)
                    else:
                        sim_list.append(np.nan)
                else:
                    v = output_name_to_result[output_name]
                    if v is np.nan:
                        sim_list.append(np.nan)
                        continue
                    if output_name in ["Z31D", "ADAT", "MDAT"]:
                        sim_list.append(int(datetime.fromisoformat(v).timetuple().tm_yday))
                    else:
                        sim_list.append(v)

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
