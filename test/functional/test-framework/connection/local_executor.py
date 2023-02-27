#
# Copyright(c) 2019-2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause
#

import subprocess
from datetime import timedelta
from typing import Tuple, Union, List

from connection.base_executor import BaseExecutor
from connection.channel import GenericChannel, LocalChannel, ChannelType
from test_utils.output import Output


class LocalExecutor(BaseExecutor):
    def _execute(self, command: Union[List[str], str], timeout: timedelta):
        completed_process = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout.total_seconds())

        return Output(completed_process.stdout,
                      completed_process.stderr,
                      completed_process.returncode)

    def _rsync(self, src, dst, delete=False, symlinks=False, checksum=False, exclude_list=[],
               timeout: timedelta = timedelta(seconds=90), dut_to_controller=False):
        options = []

        if delete:
            options.append("--delete")
        if symlinks:
            options.append("--links")
        if checksum:
            options.append("--checksum")

        for exclude in exclude_list:
            options.append(f"--exclude {exclude}")

        completed_process = subprocess.run(
            f'rsync -r {src} {dst} {" ".join(options)}',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout.total_seconds())

        if completed_process.returncode:
            raise Exception(f"rsync failed:\n{completed_process}")
        
    def reboot(self):
        """NOT SUPPORTED - Reboots the target system.
        """
        raise NotImplementedError("LocalExecutor does not support rebooting the target (local) host")
    
    def exec_command(self, command: Union[List[str], str]) -> Tuple[GenericChannel, GenericChannel]:
        """Run the given command and return (stdout, stderr) as channels.
        
        This call is non-blocking.

        TODO: support timeout specification if required
        """
        
        process: subprocess.Popen = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        return LocalChannel(process, ChannelType.STDOUT), LocalChannel(process, ChannelType.STDERR)

