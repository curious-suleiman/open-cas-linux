#
# Copyright(c) 2022 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause
#

import re
from datetime import timedelta
import textwrap

from core.test_run import TestRun
from test_utils.os_utils import wait


def check_progress_bar(command: str, progress_bar_expected: bool = True):
    TestRun.LOGGER.info(f"Check progress for command: {command}")
    
    stdout, stderr = TestRun.executor.exec_command(command)
    #TEMP: substituting command with a simple python block to test process output reading
    # py_cmd = """
    # import time
    # for i in range(10):
    #     print(i)
    #     time.sleep(5)
    # """
    # stdout, stderr = TestRun.executor.exec_command(f"python -c '{textwrap.dedent(py_cmd)}'")

    # TEMP: extend timeout for waiting for output to appear
    if not wait(lambda: stdout.ready(), timedelta(seconds=120), timedelta(seconds=1)):
        if not progress_bar_expected:
            TestRun.LOGGER.info("Progress bar did not appear when output was redirected to a file.")
            return
        else:
            TestRun.fail("Progress bar did not appear in 10 seconds.")
    else:
        if not progress_bar_expected:
            TestRun.fail("Progress bar appeared when output was redirected to a file.")

    percentage = 0
    while True:
        output = stdout.read(1024).decode('utf-8')
        search = re.search(r'\d+.\d+', output)
        last_percentage = percentage
        if search:
            TestRun.LOGGER.info(output)
            percentage = float(search.group())
            if last_percentage > percentage:
                TestRun.fail(f"Progress decrease from {last_percentage}% to {percentage}%.")
            elif percentage < 0:
                TestRun.fail(f"Progress must be greater than 0%. Actual: {percentage}%.")
            elif percentage > 100:
                TestRun.fail(f"Progress cannot be greater than 100%. Actual: {percentage}%.")
        elif (stdout.closed() or not output) and last_percentage > 0:
            TestRun.LOGGER.info("Progress complete.")
            break
        elif stdout.closed() and last_percentage == 0:
            TestRun.LOGGER.warning("Process finished without reporting any progress output")
            # TEMP: dump the error channel to see if there's anything on there
            TestRun.LOGGER.debug("Checking if there was anything reported on the error channel...")
            err_content = bytearray()
            next_chunk = stderr.read(1024)
            if len(next_chunk) > 0:
                # read until EOF
                while True:
                    err_content.extend(next_chunk)
                    next_chunk = stderr.read(1024)
                    if len(next_chunk) == 0:
                        break
                    err_content.extend(next_chunk)
                TestRun.LOGGER.debug("Process stderr content follows:")
                for line in err_content.decode('utf-8').splitlines():
                    TestRun.LOGGER.debug(f"    {line}")
            else:
                TestRun.LOGGER.debug("No content found on error channel.")
            TestRun.fail("Process has exited but progress doesn't complete.")
