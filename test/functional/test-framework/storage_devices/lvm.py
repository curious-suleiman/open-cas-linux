#
# Copyright(c) 2022 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause
#
import threading

from typing import Union, List, Tuple, Dict
from datetime import datetime
import os.path as ospath

from api.cas.core import Core
from core.test_run import TestRun
from storage_devices.device import Device
from storage_devices.disk import Disk, NvmeDisk
from storage_devices.partition import Partition
from test_tools.fs_utils import readlink
from test_utils.disk_finder import resolve_to_by_id_link
from test_utils.filesystem.symlink import Symlink
from test_utils.size import Size

LVM_CONFIG_PATH = "/etc/lvm/lvm.conf"
FILTER_PROTOTYPE_REGEX = r"^\sfilter\s=\s\["
TYPES_PROTOTYPE_REGEX = r"^\stypes\s=\s\["
GLOBAL_FILTER_PROTOTYPE_REGEX = r"^\sglobal_filter\s=\s\["
TAB = "\\\\t"


class LvmConfiguration:
    def __init__(
            self,
            lvm_filters: List = None,
            pv_num: int = None,
            vg_num: int = None,
            lv_num: int = None,
            cache_num: int = None,
            cas_dev_num: int = None
    ):
        self.lvm_filters = lvm_filters
        self.pv_num = pv_num
        self.vg_num = vg_num
        self.lv_num = lv_num
        self.cache_num = cache_num
        self.cas_dev_num = cas_dev_num

    @staticmethod
    def __read_definition_from_lvm_config(
            prototype_regex: str
    ):
        cmd = f"grep '{prototype_regex}' {LVM_CONFIG_PATH}"
        output = TestRun.executor.run(cmd).stdout

        return output

    @classmethod
    def __add_block_dev_to_lvm_config(
            cls,
            block_device_type: str,
            number_of_partitions: int = 16
    ):
        types_definition = cls.read_types_definition_from_lvm_config()

        if types_definition:
            if block_device_type in types_definition:
                TestRun.LOGGER.info(f"Device type '{block_device_type}' already present in config")
                return

            TestRun.LOGGER.info(f"Add block device type to existing list")
            new_type_prefix = f"types = [\"{block_device_type}\", {number_of_partitions}, "

            config_update_cmd = f"sed -i 's/{TYPES_PROTOTYPE_REGEX}/\t{new_type_prefix}/g'" \
                                f" {LVM_CONFIG_PATH}"
        else:
            TestRun.LOGGER.info(f"Create new types variable")
            new_types = f"types = [\"{block_device_type}\", {number_of_partitions}]"
            characteristic_line = f"# Configuration option devices\\/sysfs_scan."
            config_update_cmd = f"sed -i /'{characteristic_line}'/i\\ '{TAB}{new_types}' " \
                                f"{LVM_CONFIG_PATH}"

        TestRun.LOGGER.info(f"Adding {block_device_type} ({number_of_partitions} partitions) "
                            f"to supported types in {LVM_CONFIG_PATH}")
        TestRun.executor.run(config_update_cmd)

    @classmethod
    def __add_filter_to_lvm_config(
            cls,
            filter: str
    ):
        if filter is None:
            TestRun.LOGGER.error("Lvm filter for lvm config not provided.")

        filters_definition = cls.read_filter_definition_from_lvm_config()

        if filters_definition:
            if filter in filters_definition:
                TestRun.LOGGER.info(f"Filter definition '{filter}' already present in config")
                return

            new_filter_formatted = filter.replace("/", "\\/")
            new_filter_prefix = f"filter = [ \"{new_filter_formatted}\", "

            TestRun.LOGGER.info("Adding filter to existing list")
            config_update_cmd = f"sed -i 's/{FILTER_PROTOTYPE_REGEX}/\t{new_filter_prefix}/g'" \
                                f" {LVM_CONFIG_PATH}"
        else:
            TestRun.LOGGER.info("Create new filter variable")
            new_filter = f"filter = [\"{filter}\"]"
            characteristic_line = "# Configuration option devices\\/global_filter."
            config_update_cmd = f"sed -i /'{characteristic_line}'/i\\ '{TAB}{new_filter}' " \
                                f"{LVM_CONFIG_PATH}"

        TestRun.LOGGER.info(f"Adding filter '{filter}' to {LVM_CONFIG_PATH}")
        TestRun.executor.run(config_update_cmd)

    @classmethod
    def read_types_definition_from_lvm_config(cls):
        return cls.__read_definition_from_lvm_config(TYPES_PROTOTYPE_REGEX)

    @classmethod
    def read_filter_definition_from_lvm_config(cls):
        return cls.__read_definition_from_lvm_config(FILTER_PROTOTYPE_REGEX)

    @classmethod
    def read_global_filter_definition_from_lvm_config(cls):
        return cls.__read_definition_from_lvm_config(GLOBAL_FILTER_PROTOTYPE_REGEX)

    @classmethod
    def add_block_devices_to_lvm_config(
            cls,
            device_type: str
    ):
        if device_type is None:
            TestRun.LOGGER.error("No device provided.")

        cls.__add_block_dev_to_lvm_config(device_type)

    @classmethod
    def add_filters_to_lvm_config(
            cls,
            filters: List   # [CSU] not currently sure what type this should be
    ):
        if filters is None:
            TestRun.LOGGER.error("Lvm filters for lvm config not provided.")

        for f in filters:
            cls.__add_filter_to_lvm_config(f)

    @classmethod
    def configure_dev_types_in_config(
            cls,
            devices: Union[List[Device], Device]
    ):
        if isinstance(devices, list):
            devs = []
            for device in devices:
                dev = device.parent_device if isinstance(device, Partition) else device
                devs.append(dev)

            if any(isinstance(dev, Core) for dev in devs):
                cls.add_block_devices_to_lvm_config("cas")
            if any(isinstance(dev, NvmeDisk) for dev in devs):
                cls.add_block_devices_to_lvm_config("nvme")
        else:
            dev = devices.parent_device if isinstance(devices, Partition) else devices
            if isinstance(dev, Core):
                cls.add_block_devices_to_lvm_config("cas")
            if isinstance(dev, NvmeDisk):
                cls.add_block_devices_to_lvm_config("nvme")

    @classmethod
    def configure_filters(
            cls,
            lvm_filters: List,  # [CSU] not currently sure what type this should be
            devices: Union[List[Device], Device]
    ):
        if lvm_filters:
            TestRun.LOGGER.info("Preparing configuration for LVMs - filters.")
            LvmConfiguration.add_filters_to_lvm_config(lvm_filters)

        cls.configure_dev_types_in_config(devices)

    @staticmethod
    def remove_global_filter_from_config():
        cmd = f"sed -i '/{GLOBAL_FILTER_PROTOTYPE_REGEX}/d' {LVM_CONFIG_PATH}"
        TestRun.executor.run(cmd)

    @staticmethod
    def remove_filters_from_config():
        cmd = f"sed -i '/{FILTER_PROTOTYPE_REGEX}/d' {LVM_CONFIG_PATH}"
        TestRun.executor.run(cmd)

    @staticmethod
    def backup_current_config() -> str:
        # create a timestamp to the second eg 20230222151932
        # derive the name of the backup config file as {LVM_CONFIG_PATH}.bak.{timestamp}
        # check that there is not already a file with the same name - if so, regenerate timestamp
        # and try again
        # copy the existing config to the backup file
        # return the name of the backup file for later restoration
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        backup_file = f"{LVM_CONFIG_PATH}.bak.{timestamp}"
        while ospath.exists(backup_file):
            timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
            backup_file = f"{LVM_CONFIG_PATH}.bak.{timestamp}"

        TestRun.LOGGER.info(f"Backing up LVM config to {backup_file}...")
        cmd = f"cp -p {LVM_CONFIG_PATH} {backup_file}"
        TestRun.executor.run(cmd)

        return backup_file

    @staticmethod
    def restore_config(backup_file):
        TestRun.LOGGER.info(f"Restoring LVM config from {backup_file}...")
        cmd = f"cp -p {backup_file} {LVM_CONFIG_PATH} && rm -f {backup_file}"
        TestRun.executor.run(cmd)

class VolumeGroup:
    __unique_vg_id = 0
    __lock = threading.Lock()

    def __init__(self, name: str = None):
        self.name = name

    def __eq__(self, other):
        try:
            return self.name == other.name
        except AttributeError:
            return False

    @classmethod
    def __get_vg_name(cls, prefix: str = "vg"):
        with cls.__lock:
            cls.__unique_vg_id += 1
            return f"{prefix}{cls.__unique_vg_id}"

    @staticmethod
    def get_all_volume_groups():
        output_lines = TestRun.executor.run("pvscan").stdout.splitlines()

        volume_groups = {}
        for line in output_lines:
            if "PV" not in line:
                continue

            line_elements = line.split()
            pv = line_elements[line_elements.index("PV") + 1]
            vg = ""
            if "VG" in line:
                vg = line_elements[line_elements.index("VG") + 1]

            if vg not in volume_groups:
                volume_groups[vg] = []
            volume_groups[vg].append(pv)

        return volume_groups

    @staticmethod
    def create_vg(vg_name: str, device_paths: str):
        if not vg_name:
            raise ValueError("Name needed for VG creation.")
        if not device_paths:
            raise ValueError("Device paths needed for VG creation.")

        # Note that if any device in device_paths was not already configured as a PV prior
        # to this call, it will be automatically configured as a PV with the default
        # values by the vgcreate command
        cmd = f"vgcreate --yes {vg_name} {device_paths} "
        TestRun.executor.run_expect_success(cmd)

    @classmethod
    def is_vg_already_present(cls, dev_number: int, device_paths: str = None):
        if not device_paths:
            TestRun.LOGGER.exception("No devices provided.")

        volume_groups = cls.get_all_volume_groups()

        for vg in volume_groups:
            for pv in volume_groups[vg]:
                if len(volume_groups[vg]) == dev_number and pv in device_paths:
                    return cls(vg)

        for vg in volume_groups:
            for pv in volume_groups[vg]:
                if pv in device_paths:
                    TestRun.LOGGER.warning(f"Some devices are used in other LVM volume group")
        return False

    @classmethod
    def create(cls, device_paths: str = None):
        vg_name = cls.__get_vg_name()

        VolumeGroup.create_vg(vg_name, device_paths)

        volume_groups = VolumeGroup.get_all_volume_groups()

        if vg_name in volume_groups:
            return cls(vg_name)
        else:
            TestRun.LOGGER.error("Had not found newly created VG.")

    @staticmethod
    def remove(vg_name: str):
        if not vg_name:
            raise ValueError("Name needed for VG remove operation.")

        cmd = f"vgremove {vg_name}"
        return TestRun.executor.run(cmd)

    @staticmethod
    def get_logical_volumes_path(vg_name: str):
        cmd = f"lvdisplay | grep /dev/{vg_name}/ | awk '{{print $3}}'"
        paths = TestRun.executor.run(cmd).stdout.splitlines()

        return paths


class Lvm(Disk):
    __unique_lv_id = 0
    __lock = threading.Lock()

    def __init__(
            self,
            path_dm: str,  # device mapper path
            volume_group: VolumeGroup,
            volume_name: str = None
    ):
        Device.__init__(self, resolve_to_by_id_link(path_dm))
        self.device_name = path_dm.split('/')[-1]
        self.volume_group = volume_group
        self.volume_name = volume_name

    def __eq__(self, other):
        try:
            return self.device_name == other.device_name and \
                self.volume_group == other.volume_group and \
                self.volume_name == other.volume_name
        except AttributeError:
            return False

    @classmethod
    def __get_unique_lv_name(cls, prefix: str = "lv"):
        with cls.__lock:
            cls.__unique_lv_id += 1
            return f"{prefix}{cls.__unique_lv_id}"

    @classmethod
    def __create(
            cls,
            name: str,
            volume_size_cmd: str,
            volume_group: VolumeGroup
    ):
        TestRun.LOGGER.info(f"Creating LV '{name}'.")
        cmd = f"lvcreate {volume_size_cmd} --name {name} {volume_group.name} --yes"
        TestRun.executor.run_expect_success(cmd)

        volumes = cls.discover_logical_volumes()
        for volume in volumes:
            if name == volume.volume_name:
                return volume

    @classmethod
    def configure_global_filter(
            cls,
            dev_first: Device,
            lv_amount: int,
            pv_devs: Union[List[Device], Device]
    ):
        device_first = dev_first.parent_device if isinstance(dev_first, Partition) else dev_first
        if lv_amount > 1 and isinstance(device_first, Core):

            global_filter_def = LvmConfiguration.read_global_filter_definition_from_lvm_config()
            if not isinstance(pv_devs, list):
                pv_devs = [pv_devs]

            if global_filter_def:
                TestRun.LOGGER.info("Configure 'global filter' variable")
                links = []
                for pv_dev in pv_devs:
                    link = pv_dev.get_device_link("/dev/disk/by-id")
                    links.append(str(link))

                for link in links:
                    if link in global_filter_def:
                        TestRun.LOGGER.info(f"Global filter definition already contains '{link}'")
                        continue

                    new_link_formatted = link.replace("/", "\\/")
                    new_global_filter_prefix = f"global_filter = [ \"r|{new_link_formatted}|\", "

                    TestRun.LOGGER.info(f"Adding global filter '{link}' to existing list")
                    config_update_cmd = f"sed -i 's/{GLOBAL_FILTER_PROTOTYPE_REGEX}/\t" \
                                        f"{new_global_filter_prefix}/g' {LVM_CONFIG_PATH}"
                    TestRun.executor.run(config_update_cmd)
            else:
                for pv_dev in pv_devs:
                    link = pv_dev.get_device_link("/dev/disk/by-id")
                    global_filter = f"\"r|{link}|\""
                    global_filter += ", "
                global_filter = global_filter[:-2]

                TestRun.LOGGER.info("Create new 'global filter' variable")

                new_global = f"global_filter = [{global_filter}]"
                characteristic_line = "# Configuration option devices\\/types."
                config_update_cmd = f"sed -i /'{characteristic_line}'/i\\ " \
                                    f"'{TAB}{new_global}' {LVM_CONFIG_PATH}"

                TestRun.LOGGER.info(f"Adding global filter '{global_filter}' to {LVM_CONFIG_PATH}")
                TestRun.executor.run(config_update_cmd)

            TestRun.LOGGER.info("Remove 'filter' in order to 'global_filter' to be used")
            if LvmConfiguration.read_filter_definition_from_lvm_config():
                LvmConfiguration.remove_filters_from_config()

    @classmethod
    def create_specific_lvm_configuration(
            cls,
            devices: Union[List[Device], Device],
            lvm_configuration: LvmConfiguration,
            lvm_as_core: bool = False
    ) -> Tuple[List['Lvm'], Dict]:
        pv_per_vg = int(lvm_configuration.pv_num / lvm_configuration.vg_num)
        lv_per_vg = int(lvm_configuration.lv_num / lvm_configuration.vg_num)
        lv_size_percentage = int(100 / lv_per_vg)

        LvmConfiguration.configure_filters(lvm_configuration.lvm_filters, devices)

        lvm_map: dict = None  # this will be iteratively updated as required by the cls.create() call further down
        created_lvs: List['Lvm'] = []

        for vg_iter in range(lvm_configuration.vg_num):
            if isinstance(devices, list):
                pv_devs = []
                start_range = vg_iter * pv_per_vg
                end_range = start_range + pv_per_vg
                for i in range(start_range, end_range):
                    pv_devs.append(devices[i])
                device_first = devices[0]
            else:
                pv_devs = devices
                device_first = devices

            for _ in range(lv_per_vg):
                lv, lvm_map = cls.create(lv_size_percentage, pv_devs, lvm_map=lvm_map)
                if lv is None: # LV failed to create
                    # return an up-to-date lvm_map so that the caller knows which devices
                    # were newly created as part of this LV creation call
                    # this allows correct teardown to continue even if the desired
                    # LVM configuration was only partially created
                    return None, lvm_map
                created_lvs.append(lv)

            if lvm_as_core:
                cls.configure_global_filter(device_first, lv_per_vg, pv_devs)

        # return logical_volumes
        # TODO: ensure callers of this method are updated to match the new return value
        return created_lvs, lvm_map

    @classmethod
    def create(
            cls,
            volume_size_or_percent: Union[Size, int],
            devices: Union[List[Device], Device],
            name: str = None,
            lvm_map: Dict = None
    ) -> Tuple['Lvm', Dict]:
        if isinstance(volume_size_or_percent, Size):
            size_cmd = f"--size {volume_size_or_percent.get_value()}B"
        elif isinstance(volume_size_or_percent, int):
            size_cmd = f"--extents {volume_size_or_percent}%VG"
        else:
            TestRun.LOGGER.error("Incorrect type of the first argument (volume_size_or_percent).")

        if not name:
            name = cls.__get_unique_lv_name()

        devices_paths = cls.get_devices_path(devices)
        device_path_list = devices_paths.split(" ") if isinstance(devices, list) else [devices_paths]
        dev_number = len(device_path_list)

        if lvm_map is None:
            lvm_map = cls.discover_lvm_map()
        
        vg = VolumeGroup.is_vg_already_present(dev_number, devices_paths)
        if vg:
            lvm_map_vg = lvm_map['vgs'][vg.name]
        else:
            vg = VolumeGroup.create(devices_paths)
            TestRun.LOGGER.info(f"Adding new VG {vg.name} to LVM map")
            lvm_map['vgs'][vg.name] = lvm_map_vg = {
                'created': True,
                'pvs': [],
                'lvs': {}
            }

        # wrap the LV creation call so that if it fails this method can return
        # a result that signals to the caller that there was a failure, as well as
        # an up-to-date lvm_map so that the caller knows which devices were newly
        # created as part of this LV creation call
        try:
            lv = cls.__create(name, size_cmd, vg)
        except Exception as creation_exception: # TODO: more specific exception
            TestRun.LOGGER.error(f'Failed to create LV with name {name}: {creation_exception}')
            return None, lvm_map
        
        # insert the new LV into the lvm_map
        TestRun.LOGGER.info(f"Adding new LV {lv.volume_name} to LVM map")
        lvm_map_vg['lvs'][lv.volume_name] = {
            'created': True
        }
        for device_path in device_path_list:
            if device_path not in lvm_map['pvs']:
                TestRun.LOGGER.info(f"Adding new PV {device_path} to LVM map")
                lvm_map['pvs'][device_path] = {
                    'created': True,
                    'vgs': []
                }
            if device_path not in lvm_map_vg['pvs']:
                lvm_map_vg['pvs'].append(device_path)
                lvm_map['pvs'][device_path]['vgs'].append(vg.name)

        return lv, lvm_map

    @staticmethod
    def get_devices_path(devices: Union[List[Device], Device]):
        if isinstance(devices, list):
            return " ".join([Symlink(dev.path).get_target() for dev in devices])
        else:
            return Symlink(devices.path).get_target()

    @classmethod
    def discover_logical_volumes(cls):
        TestRun.LOGGER.info("Looking for logical volumes")
        TestRun.LOGGER.info("Getting all volume groups")
        vol_groups = VolumeGroup.get_all_volume_groups()
        TestRun.LOGGER.info(f"{len(vol_groups)} VGs found")
        volumes = []
        TestRun.LOGGER.info("Getting all LVs for each volume group")
        for vg in vol_groups:
            TestRun.LOGGER.info(f"Processing VG {vg}")
            lv_discovered = VolumeGroup.get_logical_volumes_path(vg)
            if lv_discovered:
                TestRun.LOGGER.info(f"Discovered {len(lv_discovered)} LVs")
                for lv_path in lv_discovered:
                    TestRun.LOGGER.info(f"Activating LV {lv_path}")
                    cls.make_sure_lv_is_active(lv_path)
                    lv_name = lv_path.split('/')[-1]
                    TestRun.LOGGER.info(f"Appending LV {lv_path}")
                    volumes.append(
                        cls(
                            readlink(lv_path),
                            VolumeGroup(vg),
                            lv_name
                        )
                    )
            else:
                TestRun.LOGGER.info(f"No LVMs present in VG {vg}.")

        return volumes

    @classmethod
    def discover_lvm_map(cls) -> Dict:
        # Construct PV : VG : LV map for newly-created LVM elements
        # Note that if any of the given devices are not already configured as PVs,
        # they will automatically be configured as PVs during VG creation
        # The purpose of this map is to inform the teardown process later as to which
        # LVM elements were created for the purpose of the test and so can be
        # safely removed without mangling the underlying system
        # The format of this map is as follows:
        #   lvm_map = {
        #       'pvs': {
        #           PV: {
        #               'created': boolean, # whether the PV was created for the test (True), or already existed (False)
        #               'vgs': [VG0, VG1, ..., VGn]  # names of associated VGs
        #           }
        #       },
        #       'vgs': {
        #           VG: {
        #               'created': boolean, # as above, but for this VG
        #               'pvs': [PV0, PV1, ..., PVn],  # names of associated PVs
        #               'lvs': {
        #                   LV: {
        #                       'created': boolean, # as above, but for this LV
        #                   }
        #               }
        #           }
        #       }
        #   }
        lvm_map = {
            'pvs': {},
            'vgs': {}
        }

        # first: discover existing VGs/PVs
        # if any existing PV is found, add it to the map with 'created' = false
        # for each PV added to the map, add any VGs currently linked to that PV to the map with
        # 'created' = false
        current_vgs = VolumeGroup.get_all_volume_groups()
        for vg_name, associated_pvs in current_vgs.items():
            for associated_pv in associated_pvs:
                try:
                    lvm_map_pv = lvm_map['pvs'][associated_pv]
                except KeyError:
                    TestRun.LOGGER.info(f"Adding existing PV {associated_pv} to LVM map")
                    lvm_map['pvs'][associated_pv] = lvm_map_pv = {
                        'created': False,
                        'vgs': {}
                    }
                try:
                    lvm_map_vg = lvm_map['vgs'][vg_name]
                except KeyError:
                    TestRun.LOGGER.info(f"Adding existing VG {vg_name} to LVM map")
                    lvm_map['vgs'][vg_name] = lvm_map_vg = {
                        'created': False,
                        'pvs': [],
                        'lvs': {}
                    }

                TestRun.LOGGER.info(f"Associating PV {associated_pv} with VG {vg_name} in LVM map")
                lvm_map_vg['pvs'].append(associated_pv)
                lvm_map_pv['vgs'].append(vg_name)

        return lvm_map

    @classmethod
    def discover(cls):
        TestRun.LOGGER.info("Discover LVMs in system...")
        return cls.discover_logical_volumes()

    @staticmethod
    def remove(lv_name: str, vg_name: str):
        if not lv_name:
            raise ValueError("LV name needed for LV remove operation.")
        if not vg_name:
            raise ValueError("VG name needed for LV remove operation.")

        cmd = f"lvremove -f {vg_name}/{lv_name}"
        return TestRun.executor.run(cmd)

    @staticmethod
    def remove_pv(pv_name: str):
        if not pv_name:
            raise ValueError("Name needed for PV remove operation.")

        cmd = f"pvremove {pv_name}"
        return TestRun.executor.run(cmd)

    @classmethod
    def remove_specific_lvm_configuration(
            cls,
            lvm_map     # see create_specific_lvm_configuration for a description of this structure
    ):
        """Removes all LVs in the given configuration, as well as associated VGs/PVs (if possible).
        
        """
        
        # TODO: define lvm_map as a specific class rather than nested dict

        # For each VG in lvm_map['vgs']:
        #       For each LV in VG['lvs']:
        #           if LV['created'] == True, remove the LV and remove the LV name from VG['lvs'] once confirmed
        #       If VG['created'] == True AND VG['lvs'] is empty:
        #           remove the VG
        #           For each PV in VG['pvs']:
        #               remove the VG name from lvm_map['pvs'][PV]
        #           remove the VG name from lvm_map['vgs'] once confirmed
        # For each PV in lvm_map['pvs']:
        #       If PV['created'] == True AND PV['vgs'] is empty:
        #           remove the PV and remove the PV name from lvm_map['pvs'] once confirmed
        # 
        # Any remaining PVs/VGs/LVs were either not created for the purposes of the test, or cannot be removed without
        # breaking a PV/VG/LV dependency that was present before the test 

        vgs_removed = []
        for vg_name, vg in lvm_map['vgs'].items():
            TestRun.LOGGER.info(f'LVM cleanup: processing VG {vg_name}')
            lvs_removed = []
            for lv_name, lv in vg['lvs'].items():
                if lv['created']:
                    TestRun.LOGGER.info(f'Removing created LV {lv_name}')
                    cls.remove(lv_name, vg_name)
                    lvs_removed.append(lv_name)
                else:
                    TestRun.LOGGER.info(f'Skipping removing existing LV {lv_name}')
            for lv_removed in lvs_removed:
                del vg['lvs'][lv_removed]
            if vg['created']:
                if len(vg['lvs']) == 0:
                    TestRun.LOGGER.info(f'Removing created VG {vg_name}')
                    TestRun.executor.run(f"vgchange -an {vg_name}")
                    VolumeGroup.remove(vg_name)
                    for associated_pv in vg['pvs']:
                        TestRun.LOGGER.info(f'Removing created VG {vg_name} from associated PV {associated_pv}')
                        del lvm_map['pvs'][associated_pv]['vgs'][lvm_map['pvs'][associated_pv]['vgs'].index(vg_name)]
                    vgs_removed.append(vg_name)
                else:
                    TestRun.LOGGER.info(f'Skipping removing created VG {vg_name} - one or more LVs still remaining in group')
            else:
                TestRun.LOGGER.info(f'Skipping removing existing VG {vg_name}')

        for vg_removed in vgs_removed:
            del lvm_map['vgs'][vg_removed]
        
        pvs_removed = []
        for pv_name, pv in lvm_map['pvs'].items():
            if pv['created']:
                if len(pv['vgs']) == 0:
                    TestRun.LOGGER.info(f'Removing created PV {pv_name}')
                    cls.remove_pv(pv_name)
                    pvs_removed.append(pv_name)
                else:
                    TestRun.LOGGER.info(f'Skipping removing created PV {pv_name} - one or more VGs still associated with PV')
            else:
                TestRun.LOGGER.info(f'Skipping removing existing PV {pv_name}')
        
        for pv_removed in pvs_removed:
            del lvm_map['pvs'][pv_removed]

        return True

    @classmethod
    def _remove_all(cls):
        """Removes _all_ LVs, VGs, and PVs in the system.

        Note: DO NOT USE - use remove_specific_lvm_configuration(lvm_configuration) instead. See
        discussion below.
        
        If the currently running system relies on one or more LVs (for example, root/swap LVs) then
        this command will either hang on the 'vgchange -ay {vg_name}' command, or it will succeed
        and the current system will stop functioning and require a reboot.

        If a test requires the creation, use, and cleanup of PVs/VGs/LVs, then it should:
            - Get a list of existing PVs/VGs/LVs during test setup
            - Run whatever actions it needs to
            - Cleanup any PVs/VGs/LVs that were not in the original list discovered during test
            setup

        A test should not try to wipe all PVs/VGs/LVs during test setup to ensure a clean slate. If
        a previous test created PVs/VGs/LVs and did not clean them up properly, this is a fault of
        the _cleanup_ process, not a problem for the next test's setup process to try to resolve.

        Formerly remove_all(cls) - method renamed to force breakage for callers
        """

        # [CSU] don't nuke the current system
        TestRun.LOGGER.info("NOTE: LV/VG/PV removal SKIPPED to avoid removing root/swap LVs")
        
        cmd = "lvdisplay | grep 'LV Path' | awk '{print $3}'"
        lvm_paths = TestRun.executor.run(cmd).stdout.splitlines()
        for lvm_path in lvm_paths:
            lv_name = lvm_path.split('/')[-1]
            vg_name = lvm_path.split('/')[-2]
            TestRun.LOGGER.info(f"NOTE: skipping removal of LV {vg_name}/{lv_name}")
            #cls.remove(lv_name, vg_name)

        cmd = "vgdisplay | grep 'VG Name' | awk '{print $3}'"
        vg_names = TestRun.executor.run(cmd).stdout.splitlines()
        for vg_name in vg_names:
            TestRun.LOGGER.info(f"NOTE: skipping removal of VG {vg_name}")
            #TestRun.executor.run(f"vgchange -an {vg_name}")
            #VolumeGroup.remove(vg_name)

        cmd = "pvdisplay | grep 'PV Name' | awk '{print $3}'"
        pv_names = TestRun.executor.run(cmd).stdout.splitlines()
        for pv_name in pv_names:
            TestRun.LOGGER.info(f"NOTE: skipping removal of PV {pv_name}")
            #cls.remove_pv(pv_name)

        TestRun.LOGGER.info("Successfully removed all LVMs.")

    @staticmethod
    def make_sure_lv_is_active(lv_path: str):
        cmd = "lvscan"
        output_lines = TestRun.executor.run_expect_success(cmd).stdout.splitlines()

        for line in output_lines:
            if "inactive " in line and lv_path in line:
                cmd = f"lvchange -ay {lv_path}"
                TestRun.executor.run_expect_success(cmd)
