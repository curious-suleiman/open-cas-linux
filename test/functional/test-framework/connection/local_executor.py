#
# Copyright(c) 2019-2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause
#

import asyncio
import subprocess
from datetime import timedelta
from typing import Tuple, Union, List
import weakref
import pty
import os

from connection.base_executor import BaseExecutor
from connection.channel import GenericChannel, LocalChannel, ChannelType
from test_utils.output import Output
from core.test_run import TestRun

def finalize_event_loop(loop, loop_dependent_processes,
                        open_file_descriptors):
    # send every process a termination signal first, then
    # await each process in order
    if loop_dependent_processes is not None:
        for p in loop_dependent_processes:
            if p.returncode is None: # process is still running
                p.terminate()
        for p in loop_dependent_processes:
            p_stdout, p_stderr = loop.run_until_complete(p.communicate())
            # TEMP: dump any output received from the process during termination
            if p_stdout is not None and len(p_stdout) > 0:
                TestRun.LOGGER.debug("Received output on process stdout during finalization:")
                for line in p_stdout:
                    TestRun.LOGGER.debug(f"    {line}\n")
            if p_stderr is not None and len(p_stderr) > 0:
                TestRun.LOGGER.debug("Received output on process stderr during finalization:")
                for line in p_stderr:
                    TestRun.LOGGER.debug(f"    {line}\n")

    if open_file_descriptors is not None:
        # close all open file descriptors
        for fd in open_file_descriptors:
            os.close(fd)

    if loop.is_closed():
        return
    if loop.is_running():
        loop.stop()
    loop.close()

class LocalExecutor(BaseExecutor):

    _finalizer = None
    _loop = None        # asyncio event loop
    _loop_dependent_processes = None
    _open_file_descriptors = None

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
            # TODO: consider using asyncio.set_event_loop(event_loop) to avoid the need to pass
            # this loop through to created channels
            self._loop = event_loop
            
            # This double assignment is done so that this object has a reference to the list
            # of loop dependend processes that is sent to the finalizer, but does not own it
            # This avoids encoding a dependency on this object in the finalizer arguments, which
            # results in the object being impossible to clean up
            loop_dependent_processes = []
            self._loop_dependent_processes = loop_dependent_processes
            open_file_descriptors = []
            self._open_file_descriptors = open_file_descriptors

            # define a finalizer to stop/close the event loop when this object is finalized
            self._finalizer = weakref.finalize(self, finalize_event_loop, event_loop,
                                               loop_dependent_processes,
                                               open_file_descriptors)

        # construct pseudo-terminals for stdin/stdout/stderr
        parent_in, child_in = pty.openpty()
        parent_out, child_out = pty.openpty()
        parent_err, child_err = pty.openpty()

        process = self._loop.run_until_complete(
            asyncio.create_subprocess_shell(
                command,
                stdin=child_in,
                stdout=child_out,
                stderr=child_err
            )
        )
        # not interested in sending any input, so close these fds to ensure the child process
        # gets EOF on stdin
        # TODO: review whether this is necessary/accurate
        os.close(parent_in)
        os.close(child_in)
        self._loop_dependent_processes.append(process)
        self._open_file_descriptors.extend((parent_out, child_out, parent_err, child_err))

        return (LocalChannel(self._loop, process, file_descriptor=parent_out),
                LocalChannel(self._loop, process, file_descriptor=parent_err))
