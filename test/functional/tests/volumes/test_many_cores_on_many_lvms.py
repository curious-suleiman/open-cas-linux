#
# Copyright(c) 2022 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause
#
import datetime
import pytest

from api.cas.init_config import InitConfig, opencas_conf_path
from storage_devices.lvm import Lvm, LvmConfiguration
from api.cas import casadm
from core.test_run import TestRun
from storage_devices.disk import DiskType, DiskTypeSet, DiskTypeLowerThan
from test_tools.fio.fio import Fio
from test_tools.fio.fio_param import ReadWrite, IoEngine, VerifyMethod
from test_utils.size import Size, Unit

# [CSU] Modified to allow non-SSD to be used for core device
@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_many_cores_on_many_lvms():
    """
        title: Test for CAS creation with lvms as cores: 1 cache, 16 lvms, 16 cores.
        description: |
          Validation of LVM support, CAS with 1 cache and 16 lvms as 16 cores.
        pass_criteria:
          - LVMs created successfully.
          - CAS devices created successfully.
          - FIO with verification ran successfully.
          - Configuration after reboot match configuration before.
    """
    with TestRun.step("Prepare devices."):
        cache_device = TestRun.disks['cache']
        core_device = TestRun.disks['core']
        cache_device.create_partitions([Size(1, Unit.GibiByte)])
        core_device.create_partitions([Size(1, Unit.GibiByte)])
        cache_dev = cache_device.partitions[0]
        core_dev = core_device.partitions[0]

    with TestRun.step("Create LVMs."):
        # backup the existing LVM config
        TestRun.lvm_config_backup = LvmConfiguration.backup_current_config()

        config = LvmConfiguration(lvm_filters=[],
                                  pv_num=1,
                                  vg_num=1,
                                  lv_num=16,
                                  cache_num=1,
                                  cas_dev_num=16)

        lvms, TestRun.lvm_map = Lvm.create_specific_lvm_configuration([core_dev], config, lvm_as_core=True)
        if lvms is None:
            TestRun.fail("Could not create target LVM configuration for test, cannot continue")

    with TestRun.step("Create CAS device."):
        cache = casadm.start_cache(cache_dev, force=True)
        cores = []
        for lvm in lvms:
            cores.append(cache.add_core(lvm))

    with TestRun.step("Run FIO with verification on LVM."):
        fio_run = (Fio().create_command()
                   .read_write(ReadWrite.randrw)
                   .io_engine(IoEngine.sync)
                   .io_depth(1)
                   .time_based()
                   .run_time(datetime.timedelta(seconds=180))
                   .do_verify()
                   .verify(VerifyMethod.md5)
                   .block_size(Size(1, Unit.Blocks4096)))
        for lvm in cores:
            fio_run.add_job().target(lvm).size(lvm.size)
        fio_run.run()

    with TestRun.step("Flush buffers"):
        for core in cores:
            TestRun.executor.run_expect_success(f"hdparm -f {core.path}")

    # Only run reboot/config persistence phase if the executor supports remote operations
    if TestRun.executor.is_remote():
        with TestRun.step("Create init config from running configuration"):
            config_before_reboot, devices_before = get_test_configuration()

        with TestRun.step("Reboot system."):
            TestRun.executor.reboot()

        with TestRun.step("Validate running configuration"):
            config_after_reboot, devices_after = get_test_configuration()

            if config_after_reboot == config_before_reboot:
                TestRun.LOGGER.info("Configuration is as expected")
            else:
                TestRun.LOGGER.info(f"config before reboot: {config_before_reboot}")
                TestRun.LOGGER.info(f"config after reboot: {config_after_reboot}")
                TestRun.LOGGER.error("Configuration changed after reboot")

            if devices_after == devices_before:
                TestRun.LOGGER.info("Device list is as expected")
            else:
                TestRun.LOGGER.info(f"Devices before: {devices_before}")
                TestRun.LOGGER.info(f"Devices after: {devices_after}")
                TestRun.LOGGER.error("Device list changed after reboot")

        with TestRun.step("Run FIO with verification on LVM."):
            fio_run.run()

    with TestRun.step("Remove CAS devices."):
        casadm.remove_all_detached_cores()
        casadm.stop_all_caches()

    with TestRun.step("Cleanup created LVMs and restore LVM config"):
        Lvm.remove_specific_lvm_configuration(TestRun.lvm_map)
        TestRun.lvm_map = None
        LvmConfiguration.restore_config(TestRun.lvm_config_backup)
        TestRun.lvm_config_backup = None


def get_block_devices_list():
    cmd = "lsblk -l | awk '{{print $1}}' | grep -v loop"
    devices = TestRun.executor.run_expect_success(cmd).stdout
    devices_list = devices.splitlines()
    devices_list.sort()

    return devices_list


def get_test_configuration():
    InitConfig.create_init_config_from_running_configuration()
    config_output = TestRun.executor.run(f"cat {opencas_conf_path}")
    devices = get_block_devices_list()

    return config_output.stdout, devices
