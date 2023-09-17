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

""" Runner """

import os
import io
import sys
import gzip
import json
import yaml
import time
import shutil
import socket
import signal
import logging
import tempfile
import traceback
import importlib
import subprocess
import pkg_resources

import paramiko  # pylint: disable=E0401
import requests  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0401
from pylon.core.tools import log_loki  # pylint: disable=E0401
from pylon.core.tools import config  # pylint: disable=E0401
from pylon.core.tools import module  # pylint: disable=E0401
from pylon.core.tools import event  # pylint: disable=E0401
from pylon.core.tools import git  # pylint: disable=E0401
from pylon.core.tools import rpc  # pylint: disable=E0401

from pylon.core.tools.signal import signal_sigterm  # pylint: disable=E0401
from pylon.core.tools.context import Context  # pylint: disable=E0401
from pylon.core.tools.minio.client import MinIOHelper  # pylint: disable=E0401


def run_tasklet(configuration):
    """ Run tasklet """
    fork_result = os.fork()
    #
    if fork_result == 0:
        log.info("Preparing to start tasklet")
        #
        import ssl
        ssl.RAND_bytes(1)
        #
        this_tasklet_name = configuration["tasklet"]["name"]
        this_run_id = configuration["runner"]["run_id"]
        this_title = f"{this_tasklet_name} {this_run_id}"
        #
        import setproctitle
        setproctitle.setproctitle(this_title)
        #
        tasklet = Tasklet(configuration)
        try:
            tasklet.run()
        except:  # pylint: disable=W0702
            log.exception("Exception during tasklet execution")
            tasklet.run_exception = traceback.format_exc()
        finally:
            tasklet.cleanup()
        #
        os._exit(0)
    else:
        log.info("Waiting for forked process: %s", fork_result)
        #
        return_status = os.waitpid(fork_result, 0)
        #
        log.info("Forked process return status: %s", return_status)


def patched_paramiko_client_SSHClient_auth(original_auth):  # pylint: disable=C0103
    """ Allow to pass prepared pkey in key_filename(s) """
    def patched_function(  # pylint: disable=R0913
            self, username, password, pkey, key_filenames, allow_agent, look_for_keys,  # pylint: disable=W0613
            gss_auth, gss_kex, gss_deleg_creds, gss_host, passphrase,
    ):
        if isinstance(key_filenames, paramiko.RSAKey):
            target_key_filenames = list()
            #
            return original_auth(
                self, username, password, key_filenames, target_key_filenames, allow_agent, look_for_keys,
                gss_auth, gss_kex, gss_deleg_creds, gss_host, passphrase,
            )
        return original_auth(
            self, username, password, pkey, key_filenames, allow_agent, look_for_keys,
            gss_auth, gss_kex, gss_deleg_creds, gss_host, passphrase,
        )
    return patched_function


#
# Tasklet
#


class Tasklet:
    """ Tasklet executor """

    def __init__(self, configuration):
        self.configuration = configuration
        self.pylon_context = Context()
        #
        self.temporary_objects = list()
        self.source_provider = None
        #
        self.tasklet_context = None
        #
        self.clean_run = True
        self.run_exception = None
        #
        self.module_site_paths = list()
        self.module_constraint_paths = list()
        self.loaded_libs = set()

    def run(self):
        """ Run tasklet """
        self.clean_run = False
        #
        # Here we are running in new process. Pylon is not initialized. Re-init
        #
        # Register signal handling
        signal.signal(signal.SIGTERM, signal_sigterm)
        # Enable logging and say hello to terminal log
        log.enable_logging()
        #
        log.info("Starting tasklet runner")
        # Save debug status
        self.pylon_context.debug = os.environ.get(
            "CORE_DEVELOPMENT_MODE", ""
        ).lower() in ["true", "yes"]
        # Disable bytecode caching and register resource providers
        sys.dont_write_bytecode = True
        pkg_resources.register_loader_type(
            module.DataModuleLoader, module.DataModuleProvider
        )
        # Prepare settings
        try:
            pylon_settings = self.configuration.get("pylon", dict())
            pylon_settings = config.config_substitution(
                pylon_settings, config.vault_secrets(pylon_settings)
            )
            if "storage" in pylon_settings:
                pylon_settings["storage"]["endpoint"] = pylon_settings["storage"]["endpoint"].replace("http://", "").replace("https://", "").rstrip("/")  # pylint: disable=C0103
        except:  # pylint: disable=W0702
            log.exception("Failed to prepare settings")
            pylon_settings = dict()
        #
        self.pylon_context.settings = pylon_settings
        # Set global node name
        self.pylon_context.node_name = self.configuration["runner"]["run_id"]
        # Enable Loki logging if requested in config
        log_loki.enable_loki_logging(self.pylon_context)
        # Fake ModuleManager
        self.pylon_context.module_manager = Context()
        self.pylon_context.module_manager.temporary_objects = \
            self.temporary_objects
        # Make EventManager instance
        self.pylon_context.event_manager = event.EventManager(
            self.pylon_context
        )
        # Make RpcManager instance
        config_rpc_prefix = self.pylon_context.settings["rpc"]["id_prefix"]
        worker_rpc_prefix = f'{config_rpc_prefix}{self.pylon_context.node_name}_'
        self.pylon_context.settings["rpc"]["id_prefix"] = worker_rpc_prefix
        self.pylon_context.rpc_manager = rpc.RpcManager(self.pylon_context)
        # Apply patches needed for pure-python git and providers
        # git.apply_patches()
        #
        import getpass
        from dulwich import repo, client  # pylint: disable=E0401
        from dulwich.contrib.paramiko_vendor import ParamikoSSHVendor  # pylint: disable=E0401
        import paramiko  # pylint: disable=E0401
        import paramiko.transport  # pylint: disable=E0401
        # Set USERNAME if needed
        try:
            getpass.getuser()
        except:  # pylint: disable=W0702
            os.environ["USERNAME"] = "git"
        # Patch dulwich to work without valid UID/GID
        repo._get_default_identity = git.patched_repo_get_default_identity(repo._get_default_identity)  # pylint: disable=W0212
        # Patch dulwich to use paramiko SSH client
        client.get_ssh_vendor = ParamikoSSHVendor
        # Patch paramiko to skip key verification
        paramiko.transport.Transport._verify_key = git.patched_paramiko_transport_verify_key  # pylint: disable=W0212
        # Patch paramiko to support direct pkey usage
        paramiko.client.SSHClient._auth = patched_paramiko_client_SSHClient_auth(paramiko.client.SSHClient._auth)  # pylint: disable=C0301,W0212
        #
        # Update run state
        #
        try:
            self.pylon_context.rpc_manager.timeout(5).tasklets_set_run_state(
                self.configuration["runner"]["run_id"], "Initializing"
            )
        except:  # pylint: disable=W0702
            pass
        #
        # Now we need to get tasklet code
        #
        log.info("Tasklet: %s", self.configuration["tasklet"])
        #
        self.source_provider = importlib.import_module(
            self.configuration["provider"]["name"]
        ).Provider(self.pylon_context, self.configuration["provider"]["args"])
        self.source_provider.init()
        #
        source_target = {
            "source": self.configuration["tasklet"]["source_url"],
            "branch": self.configuration["tasklet"]["source_branch"],
        }
        #
        if "git@" not in self.configuration["tasklet"]["source_url"]:
            source_target["key_data"] = None
        #
        source = None
        #
        for retry in range(5):
            try:
                source = self.source_provider.get_source(source_target)
                self.temporary_objects.append(source)
                log.info("Source: %s", source)
                break
            except:  # pylint: disable=W0702
                log.exception("Failed to get source at retry %s", retry)
                time.sleep(5)
        #
        if source is None:
            raise RuntimeError("Failed to get source")
        #
        # Load metadata
        #
        metadata_path = os.path.join(source, "metadata.json")
        if not os.path.exists(metadata_path):
            log.error("No tasklet metadata")
            return
        #
        with open(metadata_path, "rb") as file:
            metadata = json.load(file)
        #
        # Install requirements
        #
        requirements_path = os.path.join(source, "requirements.txt")
        #
        if os.path.exists(requirements_path):
            requirements_base = tempfile.mkdtemp()
            self.temporary_objects.append(requirements_base)
            #
            try:
                module.ModuleManager.install_requirements(
                    requirements_path=requirements_path,
                    target_site_base=requirements_base,
                    additional_site_paths=self.module_site_paths,
                    constraint_paths=self.module_constraint_paths,
                )
            except:  # pylint: disable=W0702
                log.exception("Failed to install requirements")
                return
            #
            requirements_path = module.ModuleManager.get_user_site_path(
                requirements_base
            )
            #
            module.ModuleManager.activate_path(requirements_path)
            self.module_site_paths.append(requirements_path)
            pkg_resources._initialize_master_working_set()  # pylint: disable=W0212
        #
        # Install libs
        #
        for library in metadata.get("libs", list()):
            self.load_library(library)
        #
        # Activate loader
        #
        load_path = source
        if metadata.get("subpath", None):
            load_path = os.path.join(load_path, metadata["subpath"])
        #
        loader = module.LocalModuleLoader(
            metadata["module_name"], load_path
        )
        module.ModuleManager.activate_loader(loader)
        pkg_resources._initialize_master_working_set()  # pylint: disable=W0212
        #
        # Update run state
        #
        try:
            self.pylon_context.rpc_manager.timeout(5).tasklets_set_run_state(
                self.configuration["runner"]["run_id"], "Running"
            )
        except:  # pylint: disable=W0702
            pass
        #
        # Run
        #
        self.tasklet_context = TaskletContext(self)
        #
        code = importlib.import_module(f'{metadata["module_name"]}.code')
        result = code.execute(self.tasklet_context, self.temporary_objects)
        #
        try:
            self.pylon_context.rpc_manager.timeout(5).tasklets_set_run_result(
                self.configuration["runner"]["run_id"], result
            )
        except:  # pylint: disable=W0702
            pass
        #
        self.clean_run = True
        #
        try:
            self.pylon_context.rpc_manager.timeout(5).tasklets_set_run_state(
                self.configuration["runner"]["run_id"], "Finished"
            )
        except:  # pylint: disable=W0702
            pass

    def load_library(self, name):
        """ Load additional library """
        if name in self.loaded_libs:
            return
        self.loaded_libs.add(name)
        #
        # Get tasklet info
        #
        try:
            tasklet_info = self.pylon_context.rpc_manager.timeout(5).tasklets_get_by_name(
                name
            )
        except:  # pylint: disable=W0702
            raise
        #
        log.info("Library: %s", tasklet_info)
        #
        # Now we need to get code
        #
        source_target = {
            "source": tasklet_info["source_url"],
            "branch": tasklet_info["source_branch"],
        }
        #
        if "git@" not in tasklet_info["source_url"]:
            source_target["key_data"] = None
        #
        source = None
        #
        for retry in range(5):
            try:
                source = self.source_provider.get_source(source_target)
                self.temporary_objects.append(source)
                log.info("Library source: %s", source)
                break
            except:  # pylint: disable=W0702
                log.exception("Failed to get library source at retry %s", retry)
                time.sleep(5)
        #
        if source is None:
            raise RuntimeError("Failed to get library source")
        #
        # Load metadata
        #
        metadata_path = os.path.join(source, "metadata.json")
        if not os.path.exists(metadata_path):
            log.error("No library metadata")
            raise RuntimeError("No library metadata")
        #
        with open(metadata_path, "rb") as file:
            metadata = json.load(file)
        #
        # Install requirements
        #
        requirements_path = os.path.join(source, "requirements.txt")
        #
        if os.path.exists(requirements_path):
            requirements_base = tempfile.mkdtemp()
            self.temporary_objects.append(requirements_base)
            #
            try:
                module.ModuleManager.install_requirements(
                    requirements_path=requirements_path,
                    target_site_base=requirements_base,
                    additional_site_paths=self.module_site_paths,
                    constraint_paths=self.module_constraint_paths,
                )
            except:  # pylint: disable=W0702
                log.exception("Failed to install requirements")
                raise
            #
            requirements_path = module.ModuleManager.get_user_site_path(
                requirements_base
            )
            #
            module.ModuleManager.activate_path(requirements_path)
            self.module_site_paths.append(requirements_path)
            pkg_resources._initialize_master_working_set()  # pylint: disable=W0212
        #
        # Install libs
        #
        for library in metadata.get("libs", list()):
            self.load_library(library)
        #
        # Activate loader
        #
        load_path = source
        if metadata.get("subpath", None):
            load_path = os.path.join(load_path, metadata["subpath"])
        #
        loader = module.LocalModuleLoader(
            metadata["module_name"], load_path
        )
        module.ModuleManager.activate_loader(loader)
        pkg_resources._initialize_master_working_set()  # pylint: disable=W0212
        #
        log.info("Library added: %s", name)

    def cleanup(self):
        """ Cleanup worker environment """
        for handler in logging.getLogger("").handlers:
            try:
                handler.flush()
            except:  # pylint: disable=W0702
                pass
        #
        if not self.clean_run:
            try:
                self.pylon_context.rpc_manager.timeout(5).tasklets_set_run_result(
                    self.configuration["runner"]["run_id"], self.run_exception
                )
            except:  # pylint: disable=W0702
                pass
            try:
                self.pylon_context.rpc_manager.timeout(5).tasklets_set_run_state(
                    self.configuration["runner"]["run_id"], "Crashed"
                )
            except:  # pylint: disable=W0702
                pass
        #
        if self.source_provider is not None:
            self.source_provider.deinit()
        #
        for obj in self.temporary_objects:
            try:
                if os.path.isdir(obj):
                    shutil.rmtree(obj)
                else:
                    os.remove(obj)
            except:  # pylint: disable=W0702
                pass


#
# Context
#


class TaskletContext:
    """ Context proxy """

    def __init__(self, tasklet):
        self.tasklet = tasklet
        #
        try:
            current_run = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_run(
                self.tasklet.configuration["runner"]["run_id"]
            )
        except:  # pylint: disable=W0702
            raise
        #
        self.stream = current_run["stream"]
        self.cycle = current_run["cycle"]
        self.worker = current_run["worker"]
        #
        self.args = self.tasklet.configuration["runner"]["kvargs"]
        if self.args is None:
            self.args = dict()
        #
        self.check_entities()

    #
    # Init
    #

    def check_entities(self):
        """ ... """
        current_cycle_number = self.get_stream_meta("cycle_number", None)
        if current_cycle_number is None:
            current_cycle_number = 0
            self.set_stream_meta("cycle_number", current_cycle_number)
        #
        system_buckets = ["tasklets-support", "tasklets-data"]
        run_buckets = [
            f"tasklets-stream-{self.stream.lower()}",
            f"tasklets-cycle-{self.stream.lower()}-{self.cycle.lower()}",
        ]
        #
        for bucket in system_buckets+run_buckets:
            if not self.bucket_exists(bucket):
                self.create_bucket(bucket)

    #
    # Cycle
    #

    def next_cycle(self):
        """ Init next scan cycle """
        current_cycle = self.get_stream_meta("cycle_number")
        next_cycle = current_cycle + 1
        self.set_stream_meta("cycle_number", next_cycle)
        #
        self.cycle = str(next_cycle)
        self.check_entities()

    #
    # Meta
    #

    def get_tasklet_meta(self, key, default=None):
        """ ... """
        try:
            meta = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_meta(
                self.tasklet.configuration["tasklet"]["id"]
            )
        except:  # pylint: disable=W0702
            raise
        #
        return meta.get(key, default)

    def set_tasklet_meta(self, key, value):
        """ ... """
        try:
            meta = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_meta(
                self.tasklet.configuration["tasklet"]["id"]
            )
        except:  # pylint: disable=W0702
            raise
        #
        meta[key] = value
        #
        try:
            self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_set_meta(
                self.tasklet.configuration["tasklet"]["id"], meta
            )
        except:  # pylint: disable=W0702
            raise

    def get_stream_meta(self, key, default=None):
        """ ... """
        try:
            meta = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_stream_meta(
                self.stream
            )
        except:  # pylint: disable=W0702
            raise
        #
        return meta.get(key, default)

    def set_stream_meta(self, key, value):
        """ ... """
        try:
            meta = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_stream_meta(
                self.stream
            )
        except:  # pylint: disable=W0702
            raise
        #
        meta[key] = value
        #
        try:
            self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_set_stream_meta(
                self.stream, meta
            )
        except:  # pylint: disable=W0702
            raise

    def get_cycle_meta(self, key, default=None):
        """ ... """
        try:
            meta = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_cycle_meta(
                self.stream, self.cycle
            )
        except:  # pylint: disable=W0702
            raise
        #
        return meta.get(key, default)

    def set_cycle_meta(self, key, value):
        """ ... """
        try:
            meta = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_cycle_meta(
                self.stream, self.cycle
            )
        except:  # pylint: disable=W0702
            raise
        #
        meta[key] = value
        #
        try:
            self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_set_cycle_meta(
                self.stream, self.cycle, meta
            )
        except:  # pylint: disable=W0702
            raise

    #
    # Subtask
    #

    # TODO: stop, delete, purge

    def run_tasklet(self, name, kvargs=None, stream=..., cycle=..., description="", worker=..., join=True, join_poll_interval=1):
        """ ... """
        if stream is ...:
            stream=self.stream
        if cycle is ...:
            cycle=self.cycle
        if worker is ...:
            worker=self.worker
        #
        try:
            run_id = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_run_tasklet(
                name, kvargs, stream, cycle, description, worker
            )
        except:  # pylint: disable=W0702
            raise
        #
        if join:
            return self.join_tasklet(run_id, join_poll_interval)
        #
        return run_id

    def join_tasklet(self, run_id, poll_interval=1):
        """ ... """
        while True:
            try:
                tasklet_run = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_run(
                    run_id
                )
            except:  # pylint: disable=W0702
                time.sleep(poll_interval)
                continue
            #
            if tasklet_run["state"] in ["Finished"]:
                return tasklet_run.get("result", None)
            #
            if tasklet_run["state"] in ["Crashed"]:
                raise RuntimeError(tasklet_run.get("result", None))
            #
            if tasklet_run["state"] in ["Failed"]:
                raise RuntimeError("Tasklet failed")
            #
            time.sleep(poll_interval)

    def is_tasklet_running(self, run_id):
        """ Check if tasklet is running """
        try:
            tasklet_run = self.tasklet.pylon_context.rpc_manager.timeout(5).tasklets_get_run(
                run_id
            )
        except:  # pylint: disable=W0702
            raise
        return tasklet_run["state"] not in [
            "Finished", "Crashed", "Failed"
        ]

    def fetch_tasklet_logs(self, run_id):
        """ ... """
        result = list()
        #
        if "loki" not in self.tasklet.pylon_context.settings:
            return result
        #
        loki_settings = self.tasklet.pylon_context.settings["loki"]
        #
        ts_logs_start = 0
        ts_logs_end = int(time.time() * 1000000000)
        #
        loki_auth = None
        if "user" in loki_settings and "password" in loki_settings:
            loki_auth = (loki_settings["user"], loki_settings["password"])
        #
        loki_query_url = loki_settings["url"]
        loki_query_url = loki_query_url.replace(
            "api/v1/push", "api/v1/query_range"
        )
        #
        logs_query = "{" + f'log_type="tasklet",run_id="{run_id}"' + "}"
        logs_start = ts_logs_start
        logs_end = ts_logs_end
        logs_limit = 1000
        #
        all_log_items = list()
        #
        try:
            while True:
                response = requests.get(
                    loki_query_url,
                    params={
                        "query": logs_query,
                        "start": str(logs_start),
                        "end": str(logs_end),
                        "limit": str(logs_limit),
                    },
                    auth=loki_auth,
                    verify=loki_settings.get("verify", False),
                )
                response.raise_for_status()
                response_data = response.json()
                #
                log_items = list()
                for response_stream in response_data.get("data", dict).get("result", list()):
                    for response_item in response_stream.get("values", list()):
                        log_items.append((int(response_item[0]), response_item[1]))
                log_items.sort(key=lambda data: data[0])
                #
                all_log_items.extend(log_items)
                #
                if len(log_items) < logs_limit:
                    break
                #
                logs_end = log_items[0][0] - 1
        except:  # pylint: disable=W0702
            log.exception("Failed to fetch tasklet logs from Loki")
        #
        all_log_items.sort(key=lambda data: data[0])
        for log_item in all_log_items:
            result.append(log_item[1])
        #
        return result

    #
    # Compat: subtask
    #

    def check_run_tasklet(  # pylint: disable=R0913,R0914
            self, name, kvargs=None, stream=..., cycle=..., description="", worker=...,
            retries=5, delay=60.0, timeout=None,
    ):
        """ Execute subtasklet (check if result is good, if not - wait and retry) """
        _ = timeout  # Placeholder for future
        run_id = None
        for retry in range(retries):
            try:
                run_id = self.run_tasklet(
                    name, kvargs, stream, cycle, description, worker, join=False
                )
                result = self.join_tasklet(run_id)
                return result
            except:  # pylint: disable=W0702
                log.exception("Subtask failed on retry %s", retry+1)
                log.info("Waiting for retry (%s seconds)", delay)
                time.sleep(delay)
        log.error("Subtask failed after %s retries", retries)
        if run_id is not None:
            logs = self.fetch_tasklet_logs(run_id)
            for line in logs:
                log.error("Failed subtask log: %s", line)
        raise RuntimeError(f"Subtask failed after {retries} retries")

    #
    # Compat: args
    #

    def check_needed_args(self, needed_args):
        """ Check all required args present """
        for arg in needed_args:
            if arg not in self.args:
                raise ValueError(f"Required arg '{arg}' is not present")

    #
    # Compat: logging
    #

    def logging_fix_context(self):
        """ Save and restore loggers (for log-breaking tasks) """
        _ = self
        return LoggingFix()

    #
    # Compat: subprocess
    #

    @staticmethod
    def run_command(*args, **kvargs):
        """ Run command and log output """
        proc = subprocess.Popen(*args, **kvargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        #
        while proc.poll() is None:
            while True:
                line = proc.stdout.readline().decode().strip()
                #
                if not line:
                    break
                #
                log.info(line)
        #
        if proc.returncode != 0:
            raise RuntimeError(f"Command failed, return code={proc.returncode}")

    @staticmethod
    def run_command_one_log(*args, **kvargs):
        """ Run command and log output as one big chunk """
        proc = subprocess.Popen(*args, **kvargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        log_lines = list()
        #
        while proc.poll() is None:
            while True:
                line = proc.stdout.readline().decode().strip()
                #
                if not line:
                    break
                #
                log_lines.append(line)
        #
        log.info("\n".join(log_lines))
        #
        if proc.returncode != 0:
            raise RuntimeError(f"Command failed, return code={proc.returncode}")

    @staticmethod
    def get_limited_run_command_one_log(log_limit):
        """ Run command, log output as one limited line """
        #
        def limited_run_command_one_log(*args, **kvargs):
            """ Run command and log output as one big chunk """
            proc = subprocess.Popen(
                *args, **kvargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            log_lines = list()
            #
            while proc.poll() is None:
                while True:
                    line = proc.stdout.readline().decode().strip()
                    #
                    if not line:
                        break
                    #
                    log_lines.append(line)
            #
            log_line = "\n".join(log_lines)
            if len(log_line) > log_limit:
                truncated = " ... <truncated>"
                log_line = f"{log_line[:log_limit-len(truncated)]}{truncated}"
            log.info(log_line)
            #
            if proc.returncode != 0:
                raise RuntimeError(f"Command failed, return code={proc.returncode}")
        #
        return limited_run_command_one_log

    @staticmethod
    def run_command_get_result(*args, **kvargs):
        """ Run command, log output, return logs and returncode """
        proc = subprocess.Popen(*args, **kvargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        log_lines = list()
        #
        while proc.poll() is None:
            while True:
                line = proc.stdout.readline().decode().strip()
                #
                if not line:
                    break
                #
                log.info(line)
                log_lines.append(line)
        #
        result = dict()
        result["returncode"] = proc.returncode
        result["logs"] = log_lines
        #
        return result

    #
    # Storage
    #

    def get_minio_client(self):
        """ ... """
        if "storage" not in self.tasklet.pylon_context.settings:
            return None
        #
        return MinIOHelper.get_client(
            self.tasklet.pylon_context.settings["storage"]
        )

    def get_manager_minio(self, timeout=None):
        """ ... """
        _ = timeout
        return self.get_minio_client()

    def get_dsast_minio(self, timeout=None):
        """ ... """
        _ = timeout
        raise ValueError("Not supported yet")

    def load_object(self, bucket, key, raw=True, storage="manager", encoder_decoder=None, timeout=None):  # pylint: disable=R0913,C0301
        """ Load object from MinIO """
        if storage == "manager":
            minio = self.get_manager_minio(timeout=timeout)
        elif storage == "dsast":
            minio = self.get_dsast_minio(timeout=timeout)
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            if raw:
                return minio.get_object(bucket, key).read()
            #
            object_hook = None
            if encoder_decoder is not None:
                object_hook = encoder_decoder.object_hook
            #
            return json.loads(
                gzip.decompress(minio.get_object(bucket, key).read()),
                object_hook=object_hook,
            )
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to load object: %s/%s (storage=%s, raw=%s)", bucket, key, storage, raw
            )
            return None

    def save_object(self, bucket, key, data, raw=True, storage="manager", encoder_decoder=None):  # pylint: disable=R0913
        """ Save object to MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            if not raw:
                data = json.dumps(data, cls=encoder_decoder)
                if isinstance(data, str):
                    data = data.encode("utf-8")
                data = gzip.compress(data)
                data_obj = io.BytesIO(data)
            else:
                if isinstance(data, str):
                    data = data.encode("utf-8")
                data_obj = io.BytesIO(data)
            #
            minio.put_object(bucket, key, data_obj, len(data))
            return True
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to save object: %s/%s (storage=%s, raw=%s)", bucket, key, storage, raw
            )
            return False

    def delete_object(self, bucket, key, storage="manager"):
        """ Delete object from MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            minio.remove_object(bucket, key)
            return True
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to delete object: %s/%s (storage=%s)", bucket, key, storage
            )
            return False

    def list_objects(self, bucket, storage="manager"):
        """ List objects in MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            return [obj.object_name for obj in minio.list_objects(bucket, recursive=True)]
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to list objects: %s (storage=%s)", bucket, storage
            )
            return None

    def object_exists(self, bucket, key, storage="manager"):
        """ Check if object exists in MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            minio.stat_object(bucket, key)
            return True
        except:  # pylint: disable=W0702
            return False

    def bucket_exists(self, bucket, storage="manager"):
        """ Check if bucket exists in MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            return minio.bucket_exists(bucket)
        except:  # pylint: disable=W0702
            return False

    def list_buckets(self, storage="manager"):
        """ List buckets in MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            return [obj.name for obj in minio.list_buckets()]
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to list buckets: (storage=%s)", storage
            )
            return None

    def create_bucket(self, bucket, storage="manager"):
        """ Crate bucket in MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            minio.make_bucket(bucket)
            return True
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to create bucket: %s (storage=%s)", bucket, storage
            )
            return False

    def delete_bucket(self, bucket, storage="manager"):
        """ Delete bucket from MinIO """
        if storage == "manager":
            minio = self.get_manager_minio()
        elif storage == "dsast":
            minio = self.get_dsast_minio()
        else:
            raise ValueError(f"Unknown storage: '{storage}'")
        #
        try:
            minio.remove_bucket(bucket)
            return True
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to delete bucket: %s (storage=%s)", bucket, storage
            )
            return False

    #
    # Settings
    #

    def load_config(self, bucket, key):
        """ Load and expand config object from storage """
        object_data = self.load_object(bucket, key)
        if object_data is None:
            return dict()
        try:
            return self.expand_config_object(object_data)
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to expand config: %s/%s", bucket, key
            )
            return dict()

    def expand_config_object(self, object_data):
        """ Expand env vars, parse yaml, insert secrets """
        result = yaml.load(os.path.expandvars(object_data), Loader=yaml.SafeLoader)
        result = config.config_substitution(
            result, config.vault_secrets(self.tasklet.pylon_context.settings)
        )
        if result is None:
            return dict()
        return result

    def load_support_config(self, config_object):
        """ Load and expand config object from context-support """
        return self.load_config("tasklets-support", config_object)

    #
    # Context data
    #

    def context_data_exists(self, key, cycle=None, as_subtask=False, common=False, universal=False):  # pylint: disable=R0913,C0301,R0911
        """ Check data presence in context storage """
        if universal:
            return self.object_exists("tasklets-data", f"{key}")
        if cycle is not None:
            return self.object_exists(f"tasklets-cycle-{self.stream.lower()}-{cycle}", f"{key}")
        if common:
            return self.object_exists(f"tasklets-stream-{self.stream.lower()}", f"{key}")
        return self.object_exists(f"tasklets-cycle-{self.stream.lower()}-{self.cycle.lower()}", f"{key}")

    def load_context_data(self, key, raw=False, cycle=None, as_subtask=False, common=False, universal=False):  # pylint: disable=R0913,C0301,R0911
        """ Load data from context storage """
        if universal:
            return self.load_object("tasklets-data", f"{key}", raw)
        if cycle is not None:
            return self.load_object(f"tasklets-cycle-{self.stream.lower()}-{cycle}", f"{key}", raw)
        if common:
            return self.load_object(f"tasklets-stream-{self.stream.lower()}", f"{key}", raw)
        return self.load_object(f"tasklets-cycle-{self.stream.lower()}-{self.cycle.lower()}", f"{key}", raw)

    def save_context_data(self, key, data, raw=False, cycle=None, as_subtask=False, common=False, universal=False):  # pylint: disable=R0913,C0301,R0911
        """ Save data to context storage """
        if universal:
            return self.save_object("tasklets-data", f"{key}", data, raw)
        if cycle is not None:
            return self.save_object(f"tasklets-cycle-{self.stream.lower()}-{cycle}", f"{key}", data, raw)
        if common:
            return self.save_object(f"tasklets-stream-{self.stream.lower()}", f"{key}", data, raw)
        return self.save_object(f"tasklets-cycle-{self.stream.lower()}-{self.cycle.lower()}", f"{key}", data, raw)

    def delete_context_data(self, key, cycle=None, as_subtask=False, common=False, universal=False):  # pylint: disable=R0913,C0301,R0911
        """ Delete data from context storage """
        if universal:
            return self.delete_object("tasklets-data", f"{key}")
        if cycle is not None:
            return self.delete_object(f"tasklets-cycle-{self.stream.lower()}-{cycle}", f"{key}")
        if common:
            return self.delete_object(f"tasklets-stream-{self.stream.lower()}", f"{key}")
        return self.delete_object(f"tasklets-cycle-{self.stream.lower()}-{self.cycle.lower()}", f"{key}")


#
# Fixes
#


class LoggingFix:
    """ Fix logging if needed """

    def __init__(self):
        self.current_handlers = list()

    def __enter__(self):
        root_logger_handlers = logging.getLogger("").handlers
        for handler in root_logger_handlers:
            if isinstance(handler, (log_loki.CarrierLokiLogHandler, log_loki.CarrierLokiBufferedLogHandler)):
                self.current_handlers.append(handler)
        #
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        _ = exc_type, exc_value, traceback
        #
        to_add = list()
        root_logger = logging.getLogger("")
        for needed_handler in self.current_handlers:
            if needed_handler not in root_logger.handlers:
                to_add.append(needed_handler)
        for handler in to_add:
            root_logger.addHandler(handler)
