#!/usr/bin/python3
# coding=utf-8

#   Copyright 2022 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Module """

import os
import sys
import shutil
import tempfile

from pylon.core.tools import log  # pylint: disable=E0401
from pylon.core.tools import module  # pylint: disable=E0401
from pylon.core.tools.context import Context as Holder  # pylint: disable=E0401

from arbiter.minion import Minion  # pylint: disable=E0401


class Module(module.ModuleModel):
    """ Pylon module """

    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor
        #
        self.minion = None
        self.runner_path = None

    def init(self):
        """ Init module """
        log.info("Initializing module")
        #
        config = self.descriptor.config.get("rabbitmq", dict())
        self.minion = Minion(
            host=config.get("host"),
            port=int(config.get("port", 5672)),
            user=config.get("user"),
            password=config.get("password"),
            vhost=config.get("vhost", "carrier"),
            queue=config.get("queue", "tasklets-worker"),
            all_queue=config.get("all_queue", "tasklets-arbiter-all"),
        )
        #
        runner = self.descriptor.loader.get_data("data/runner.py")
        self.runner_path = tempfile.mkdtemp()
        with open(os.path.join(self.runner_path, "runner.py"), "wb") as file:
            file.write(runner)
        sys.path.append(self.runner_path)
        import runner
        #
        self.minion.task(name="run_tasklet")(runner.run_tasklet)
        self.minion.run(self.descriptor.config.get("workers", 1))

    def deinit(self):  # pylint: disable=R0201
        """ De-init module """
        log.info("De-initializing module")
        #
        self.minion.disconnect()
        shutil.rmtree(self.runner_path)
