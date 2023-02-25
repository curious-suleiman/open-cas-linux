#
# Copyright(c) 2019-2022 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause
#

import datetime
import uuid

import test_tools.fio.fio_param
import test_tools.fs_utils
from core.test_run import TestRun
from test_tools import fs_utils
from test_utils import os_utils


class Fio:
    def __init__(self, executor_obj=None):
        self.fio_path = None    # this will be set later
        self.fio_version = "fio-3.30"
        self.default_run_time = datetime.timedelta(hours=1)
        self.jobs = []
        self.executor = executor_obj if executor_obj is not None else TestRun.executor
        self.base_cmd_parameters: test_tools.fio.fio_param.FioParam = None
        self.global_cmd_parameters: test_tools.fio.fio_param.FioParam = None

    def create_command(self, output_type=test_tools.fio.fio_param.FioOutput.json):
        self.base_cmd_parameters = test_tools.fio.fio_param.FioParamCmd(
            self, self.executor, command_name=self.fio_path)
        self.global_cmd_parameters = test_tools.fio.fio_param.FioParamConfig(self, self.executor)

        self.fio_file = f'fio_run_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{uuid.uuid4().hex}'
        self.base_cmd_parameters\
            .set_param('eta', 'always')\
            .set_param('output-format', output_type.value)\
            .set_param('output', self.fio_file)

        self.global_cmd_parameters.set_flags('group_reporting')

        return self.global_cmd_parameters

    def is_installed(self):
        # [CSU] When running the tests using sudo (as they will not work with regular permissions
        # due to the need to e.g. create devices, mount/unmount devices, etc) the below check
        # is not consistent with the result of calling install(). This is due to the following:
        #   - install() downloads the fio snap to /tmp, unpacks, then runs configures/make/make install
        #     on the unpacked snap
        #   - this installs fio into /usr/local/bin (as expected)
        #   - /usr/local/bin is not included in the binary search path when running 'sudo pytest ...'
        #     and so the locally-installed version of fio will not be found
        #   - is_installed() will then always return False as it cannot detect the locally-installed
        #     version, which means that _every_ test will result in fio being downloaded, made, and
        #     installed. this can actually lead to occasional test failures when running a lot of
        #     tests due to intermittent failures downloading the fio snap
        #   - after all this, tests that require fio will still fail as the just-installed fio binary
        #     will not be found on the sudo binary search path
        #   - however, if the user has installed a distributed version of fio on their system (e.g. via DNF)
        #     then this is likely going to be found in the sudo binary search path. this means that
        #     tests that need fio will not fail due to fio not being found, however they are
        #     not guaranteed to use the version of fio that this test suite requires (3.30) and so
        #     may exhibit unexpected behaviour.
        #
        # According to the ./configure script included in the snap for fio-3.30, if the --prefix
        # option is not supplied, the installation prefix will be set to /usr/local. It should then
        # be possible to:
        #   - do a post-install check to confirm that either fio is available implicitly in the
        #     prevailing search path or available explicity at /usr/local/bin/fio. the version
        #     should also be checked at this time (if the command is found)
        #   - if install() fails to find fio after attempting to install it, this should result in
        #     a suitable error being emitted to the TestRun log and an exception being thrown
        #   - if install() finds fio implicitly, then the command_name provided to FioParamCmd
        #     should be 'fio' (the default value)
        #   - if install() finds fio explicitly, then the command_name provided to FioParamCmd
        #     should be /usr/local/bin/fio
        
        # if self.fio_path is already set, then this check has already been performed at least once
        # check whether it is still accurate
        if self.fio_path is not None and \
            self.executor.run(f"{self.fio_path} --version").stdout.strip() == self.fio_version:
            return True

        # check whether it is available explicitly at /usr/local/bin/fio
        if self.executor.run("/usr/local/bin/fio --version").stdout.strip() == self.fio_version:
            self.fio_path = '/usr/local/bin/fio'
            return True

        # check whether it is available on the prevailing binary search path
        if self.executor.run("fio --version").stdout.strip() == self.fio_version:
            self.fio_path = 'fio'
            return True
        
        return False

    def install(self):
        fio_url = f"http://brick.kernel.dk/snaps/{self.fio_version}.tar.bz2"
        fio_package = os_utils.download_file(fio_url)
        fs_utils.uncompress_archive(fio_package)
        TestRun.executor.run_expect_success(f"cd {fio_package.parent_dir}/{self.fio_version}"
                                            f" && ./configure && make -j && make install")
        # [CSU] refer notes in is_installed() above
        # confirm that fio was installed correctly at the expected location
        self.fio_path = '/usr/local/bin/fio'
        if not self.is_installed():
            TestRun.LOGGER.error('Failed to confirm the installed fio binary at /usr/local/bin/fio after installation, cannot continue')
            # TODO: more specific exception
            raise Exception('Failed to confirm the installed fio binary at /usr/local/bin/fio after installation, cannot continue')

    def calculate_timeout(self):
        if "time_based" not in self.global_cmd_parameters.command_flags:
            return self.default_run_time

        # [CSU] If fio is set to time_based mode, then it will continue running the
        # defined workload until runtime is reached (repeating the workload if
        # required)
        # If the executor timeout is set to the same value as the runtime in this case, then
        # it is highly likely that the executor will hit its timeout at the same time as
        # the fio process will hit its timeout
        # this will result in the fio execution being judged as a failure (with a TimeoutExpired
        # exception) even though in time_based mode it will always hit this exception
        # because fio is instructed to run for this amount of time regardless of the
        # workload definition
        # So, if time_based is set, then an allowance has to be added to the executor timeout
        # time to allow fio to start, run for the specified runtime + ramp_time, stop, and
        # then cleanup before the executor terminates the process
        # for now, use a fixed value of 60s
        # TODO: use a scaling value based on the specified runtime + ramp_time, between a fixed min/max
        total_time_allowance = 60
    
        total_time = self.global_cmd_parameters.get_parameter_value("runtime")
        if len(total_time) != 1:
            raise ValueError("Wrong fio 'runtime' parameter configuration")
        total_time = int(total_time[0])
        ramp_time = self.global_cmd_parameters.get_parameter_value("ramp_time")
        if ramp_time is not None:
            if len(ramp_time) != 1:
                raise ValueError("Wrong fio 'ramp_time' parameter configuration")
            ramp_time = int(ramp_time[0])
            total_time += ramp_time
        return datetime.timedelta(seconds=total_time + total_time_allowance)

    def run(self, timeout: datetime.timedelta = None):
        if timeout is None:
            timeout = self.calculate_timeout()

        self.prepare_run()
        return self.executor.run(str(self), timeout)

    def run_in_background(self):
        self.prepare_run()
        return self.executor.run_in_background(str(self))

    def prepare_run(self):
        if not self.is_installed():
            self.install()

        if len(self.jobs) > 0:
            self.executor.run(f"{str(self)}-showcmd -")
            TestRun.LOGGER.info(self.executor.run(f"cat {self.fio_file}").stdout)
        TestRun.LOGGER.info(str(self))

    def execution_cmd_parameters(self):
        if len(self.jobs) > 0:
            separator = "\n\n"
            return f"{str(self.global_cmd_parameters)}\n" \
                f"{separator.join(str(job) for job in self.jobs)}"
        else:
            return str(self.global_cmd_parameters)

    def __str__(self):
        if len(self.jobs) > 0:
            command = f"echo '{self.execution_cmd_parameters()}' |" \
                f" {str(self.base_cmd_parameters)} -"
        else:
            fio_parameters = test_tools.fio.fio_param.FioParamCmd(
                self, self.executor, command_name=self.fio_path)
            fio_parameters.command_env_var.update(self.base_cmd_parameters.command_env_var)
            fio_parameters.command_param.update(self.base_cmd_parameters.command_param)
            fio_parameters.command_param.update(self.global_cmd_parameters.command_param)
            fio_parameters.command_flags.extend(self.global_cmd_parameters.command_flags)
            fio_parameters.set_param('name', 'fio')
            command = str(fio_parameters)
        return command
