#
# Copyright(c) 2019-2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause
#

import asyncio
import subprocess
from datetime import timedelta
from typing import Tuple, Union, List
import weakref

from connection.base_executor import BaseExecutor
from connection.channel import GenericChannel, LocalChannel, ChannelType
from test_utils.output import Output

def finalize_event_loop(loop):
    if loop.is_closed():
        return
    if loop.is_running():
        loop.stop()
    loop.close()

class LocalExecutor(BaseExecutor):

    _finalizer = None
    _loop = None        # asyncio event loop

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
        
        This call is non-blocking with respect to the given command i.e. it does not block until the
        command is finished.

        TODO: support timeout specification if required
        """
        
        if isinstance(command, list):
            command = " ".join(command)

        if self._loop is None:
            # create a new event loop, and also attach a finalizer to clean it up
            event_loop = asyncio.new_event_loop()
            self._loop = event_loop
            # TODO: consider using asyncio.set_event_loop(event_loop) to avoid the need to pass
            # this loop through to created channels

            # define a finalizer to stop/close the event loop when this object is finalized
            self._finalizer = weakref.finalize(self, finalize_event_loop, event_loop)

        # TODO: add a finalizer to terminate this process if it is still running when this object is finalized
        # need to figure out how to ensure the process terminator is called before the event loop finalizer
        # above however, as if the event loop is closed before the process exits and the process is still writing
        # to stdout/stderr, one or more exceptions will be thrown
        process = self._loop.run_until_complete(
            asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
        )

        return LocalChannel(self._loop, process, ChannelType.STDOUT), LocalChannel(self._loop, process, ChannelType.STDERR)

