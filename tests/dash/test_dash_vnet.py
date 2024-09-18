from dash_api.appliance_pb2 import *
from dash_api.vnet_pb2 import *
from dash_api.eni_pb2 import *
from dash_api.eni_route_pb2 import *
from dash_api.route_pb2 import *
from dash_api.route_group_pb2 import *
from dash_api.route_rule_pb2 import *
from dash_api.vnet_mapping_pb2 import *
from dash_api.route_type_pb2 import *
from dash_api.types_pb2 import *

from dash_db import *
from dash_configs import *

import time
import uuid
import ipaddress
import socket

from dvslib.sai_utils import assert_sai_attribute_exists

DVS_ENV = ["HWSKU=DPU-2P"]
NUM_PORTS = 2

class TestDash(object):
    def test_appliance(self, dash_db: DashDB):
        self.appliance_id = "100"
        self.sip = "10.0.0.1"
        self.vm_vni = "4321"
        pb = Appliance()
        pb.sip.ipv4 = socket.htonl(int(ipaddress.ip_address(self.sip)))
        pb.vm_vni = int(self.vm_vni)
        dash_db.create_appliance(self.appliance_id, {"pb": pb.SerializeToString()})

        direction_keys = dash_db.wait_for_asic_db_keys(ASIC_DIRECTION_LOOKUP_TABLE)
        dl_attrs = dash_db.get_asic_db_entry(ASIC_DIRECTION_LOOKUP_TABLE, direction_keys[0])
        assert_sai_attribute_exists("SAI_DIRECTION_LOOKUP_ENTRY_ATTR_ACTION", dl_attrs, "SAI_DIRECTION_LOOKUP_ENTRY_ACTION_SET_OUTBOUND_DIRECTION")

        vip_keys = dash_db.wait_for_asic_db_keys(ASIC_VIP_TABLE)
        vip_attrs = dash_db.get_asic_db_entry(ASIC_VIP_TABLE, vip_keys[0])
        assert_sai_attribute_exists("SAI_VIP_ENTRY_ATTR_ACTION", vip_attrs, "SAI_VIP_ENTRY_ACTION_ACCEPT")

    def test_vnet(self, dash_db: DashDB):
        self.vnet = "Vnet1"
        self.vni = "45654"
        self.guid = "559c6ce8-26ab-4193-b946-ccc6e8f930b2"
        pb = Vnet()
        pb.vni = int(self.vni)
        pb.guid.value = bytes.fromhex(uuid.UUID(self.guid).hex)
        dash_db.create_vnet(self.vnet, {"pb": pb.SerializeToString()})

        vnet_keys = dash_db.wait_for_asic_db_keys(ASIC_VNET_TABLE)
        self.vnet_oid = vnet_keys[0]
        vnet_attr = dash_db.get_asic_db_entry(ASIC_VNET_TABLE, self.vnet_oid)
        assert_sai_attribute_exists("SAI_VNET_ATTR_VNI", vnet_attr, self.vni)

    def test_eni(self, dash_db: DashDB):
        self.vnet = "Vnet1"
        self.mac_string = "F4939FEFC47E"
        self.mac_address = "F4:93:9F:EF:C4:7E"
        self.eni_id = "497f23d7-f0ac-4c99-a98f-59b470e8c7bd"
        self.underlay_ip = "25.1.1.1"
        self.admin_state = "enabled"
        pb = Eni()
        pb.eni_id = self.eni_id
        pb.mac_address = bytes.fromhex(self.mac_address.replace(":", ""))
        pb.underlay_ip.ipv4 = socket.htonl(int(ipaddress.ip_address(self.underlay_ip)))
        pb.admin_state = State.STATE_ENABLED
        pb.vnet = self.vnet
        dash_db.create_eni(self.mac_string, {"pb": pb.SerializeToString()})
        
        vnets = dash_db.wait_for_asic_db_keys(ASIC_VNET_TABLE)
        self.vnet_oid = vnets[0]
        enis = dash_db.wait_for_asic_db_keys(ASIC_ENI_TABLE)
        self.eni_oid = enis[0]
        attrs = dash_db.get_asic_db_entry(ASIC_ENI_TABLE, self.eni_oid)

        assert_sai_attribute_exists("SAI_ENI_ATTR_VNET_ID", attrs, str(self.vnet_oid))
        assert_sai_attribute_exists("SAI_ENI_ATTR_ADMIN_STATE", attrs, "true")

        eni_addr_maps = dash_db.wait_for_asic_db_keys(ASIC_ENI_ETHER_ADDR_MAP_TABLE)
        attrs = dash_db.get_asic_db_entry(ASIC_ENI_ETHER_ADDR_MAP_TABLE, eni_addr_maps[0])
        assert_sai_attribute_exists("SAI_ENI_ETHER_ADDRESS_MAP_ENTRY_ATTR_ENI_ID", attrs, str(self.eni_oid))

        # test admin state update
        pb.admin_state = State.STATE_DISABLED
        dash_db.create_eni(self.mac_string, {"pb": pb.SerializeToString()})
        dash_db.wait_for_asic_db_field(ASIC_ENI_TABLE, self.eni_oid, "SAI_ENI_ATTR_ADMIN_STATE", "false")

    def test_vnet_map(self, dash_db: DashDB):
        self.vnet = "Vnet1"
        self.ip1 = "10.1.1.1"
        self.ip2 = "10.1.1.2"
        self.mac_address = "F4:93:9F:EF:C4:7E"
        self.routing_type = "vnet_encap"
        self.underlay_ip = "101.1.2.3"
        route_type_msg = RouteType()
        route_action = RouteTypeItem()
        route_action.action_name = "action1"
        route_action.action_type = ACTION_TYPE_STATICENCAP
        route_action.encap_type = ENCAP_TYPE_NVGRE
        route_type_msg.items.append(route_action)
        dash_db.create_routing_type(self.routing_type, {"pb": route_type_msg.SerializeToString()})
        pb = VnetMapping()
        pb.mac_address = bytes.fromhex(self.mac_address.replace(":", ""))
        pb.action_type = RoutingType.ROUTING_TYPE_VNET_ENCAP
        pb.underlay_ip.ipv4 = socket.htonl(int(ipaddress.ip_address(self.underlay_ip)))

        dash_db.create_vnet_mapping(self.vnet, self.ip1, {"pb": pb.SerializeToString()})
        dash_db.create_vnet_mapping(self.vnet, self.ip2, {"pb": pb.SerializeToString()})

        vnet_ca_to_pa_maps = dash_db.wait_for_asic_db_keys(ASIC_OUTBOUND_CA_TO_PA_TABLE, min_keys=2)
        attrs = dash_db.get_asic_db_entry(ASIC_OUTBOUND_CA_TO_PA_TABLE, vnet_ca_to_pa_maps[0])
        assert_sai_attribute_exists("SAI_OUTBOUND_CA_TO_PA_ENTRY_ATTR_UNDERLAY_DIP", attrs, self.underlay_ip)
        assert_sai_attribute_exists("SAI_OUTBOUND_CA_TO_PA_ENTRY_ATTR_OVERLAY_DMAC", attrs, self.mac_address)
        assert_sai_attribute_exists("SAI_OUTBOUND_CA_TO_PA_ENTRY_ATTR_DASH_ENCAPSULATION", attrs, "SAI_DASH_ENCAPSULATION_NVGRE")

        vnet_pa_validation_maps = dash_db.wait_for_asic_db_keys(ASIC_PA_VALIDATION_TABLE)
        pa_validation_attrs = dash_db.get_asic_db_entry(ASIC_PA_VALIDATION_TABLE, vnet_pa_validation_maps[0])
        assert_sai_attribute_exists("SAI_PA_VALIDATION_ENTRY_ATTR_ACTION", pa_validation_attrs, "SAI_PA_VALIDATION_ENTRY_ACTION_PERMIT")

    def test_outbound_routing(self, dash_db: DashDB):
        pb = RouteGroup()
        self.group_id = ROUTE_GROUP1
        dash_db.create_route_group(self.group_id, {"pb": pb.SerializeToString()})

        outbound_routing_group_entries = dash_db.wait_for_asic_db_keys(ASIC_OUTBOUND_ROUTING_GROUP_TABLE)

        self.vnet = "Vnet1"
        self.ip = "10.1.0.0/24"
        self.action_type = "vnet_direct"
        self.overlay_ip = "10.0.0.6"
        pb = Route()
        pb.action_type = RoutingType.ROUTING_TYPE_VNET_DIRECT
        pb.vnet_direct.vnet = self.vnet
        pb.vnet_direct.overlay_ip.ipv4 = socket.htonl(int(ipaddress.ip_address(self.overlay_ip)))
        dash_db.create_route(self.group_id, self.ip, {"pb": pb.SerializeToString()})

        outbound_routing_entries = dash_db.wait_for_asic_db_keys(ASIC_OUTBOUND_ROUTING_TABLE)
        routing_attrs = dash_db.get_asic_db_entry(ASIC_OUTBOUND_ROUTING_TABLE, outbound_routing_entries[0])
        assert_sai_attribute_exists("SAI_OUTBOUND_ROUTING_ENTRY_ATTR_ACTION", routing_attrs, "SAI_OUTBOUND_ROUTING_ENTRY_ACTION_ROUTE_VNET_DIRECT")
        assert_sai_attribute_exists("SAI_OUTBOUND_ROUTING_ENTRY_ATTR_OVERLAY_IP", routing_attrs, self.overlay_ip)
        assert_sai_attribute_exists("SAI_OUTBOUND_ROUTING_ENTRY_ATTR_DST_VNET_ID", routing_attrs)

        pb = EniRoute()
        pb.group_id = self.group_id
        self.mac_string = "F4939FEFC47E"
        dash_db.create_eni_route(self.mac_string, {"pb": pb.SerializeToString()})

        enis = dash_db.wait_for_asic_db_keys(ASIC_ENI_TABLE)
        dash_db.wait_for_asic_db_field(ASIC_ENI_TABLE, enis[0], "SAI_ENI_ATTR_OUTBOUND_ROUTING_GROUP_ID", outbound_routing_group_entries[0])

    def test_inbound_routing(self, dash_db: DashDB):
        self.mac_string = "F4939FEFC47E"
        self.vnet = "Vnet1"
        self.vni = "3251"
        self.ip = "10.1.1.1"
        self.action_type = "decap"
        self.pa_validation = "true"
        self.priority = "1"
        self.protocol = "0"
        pb = RouteRule()
        pb.pa_validation = True
        pb.priority = int(self.priority)
        pb.protocol = int(self.protocol)
        pb.vnet = self.vnet

        dash_db.create_inbound_routing(self.mac_string, self.vni, self.ip, {"pb": pb.SerializeToString()})

        inbound_routing_entries = dash_db.wait_for_asic_db_keys(ASIC_INBOUND_ROUTING_TABLE)
        attrs = dash_db.get_asic_db_entry(ASIC_INBOUND_ROUTING_TABLE, inbound_routing_entries[0])
        assert_sai_attribute_exists("SAI_INBOUND_ROUTING_ENTRY_ATTR_ACTION", attrs, "SAI_INBOUND_ROUTING_ENTRY_ACTION_TUNNEL_DECAP_PA_VALIDATE")

    def test_cleanup(self, dash_db: DashDB):
        self.vnet = "Vnet1"
        self.mac_string = "F4939FEFC47E"
        self.group_id = ROUTE_GROUP1
        self.vni = "3251"
        self.sip = "10.1.1.1"
        self.dip = "10.1.0.0/24"
        self.ip2 = "10.1.1.2"
        self.appliance_id = "100"
        self.routing_type = "vnet_encap"
        dash_db.remove_inbound_routing(self.mac_string, self.vni, self.sip)
        dash_db.remove_eni_route(self.mac_string)
        dash_db.remove_route(self.group_id, self.dip)
        dash_db.remove_route_group(self.group_id)
        dash_db.remove_vnet_mapping(self.vnet, self.sip)
        dash_db.remove_vnet_mapping(self.vnet, self.ip2)
        dash_db.remove_routing_type(self.routing_type)
        dash_db.remove_eni(self.mac_string)
        dash_db.remove_vnet(self.vnet)
        dash_db.remove_appliance(self.appliance_id)

# Add Dummy always-pass test at end as workaroud
# for issue when Flaky fail on final test it invokes module tear-down
# before retrying
def test_nonflaky_dummy():
    pass
