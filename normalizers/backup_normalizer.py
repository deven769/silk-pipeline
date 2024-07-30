from typing import Dict, Any, List
from datetime import datetime
import pdb

class Normalizer:
    @staticmethod
    def normalize_qualys_host(host: Dict[str, Any]) -> Dict[str, Any]:
        # Extract EC2 instance information
        ec2_info = next((source['Ec2AssetSourceSimple'] for source in host.get('sourceInfo', {}).get('list', []) 
                         if 'Ec2AssetSourceSimple' in source), {})

        def convert_number_long(obj):
            if isinstance(obj, dict):
                new_dict = {}
                for k, v in obj.items():
                    if k == '$numberLong':
                        return int(v)
                    else:
                        new_key = k[1:] if k.startswith('$') else k
                        new_dict[new_key] = convert_number_long(v)
                return new_dict
            elif isinstance(obj, list):
            	return [convert_number_long(item) for item in obj]
            return obj

        volumes = [
            {
                "name": volume["HostAssetVolume"].get("name"),
                "size": convert_number_long(volume["HostAssetVolume"].get("size")),
                "free": convert_number_long(volume["HostAssetVolume"].get("free")),
            }
            for volume in host.get("volume", {}).get("list", [])
        ]

        # print(volumes)
        # pdb.set_trace()


        return {
            "hostname": host.get("name") or host.get("dnsHostName") or host.get("fqdn"),
            "ip_address": host.get("address"),
            "os": host.get("os"),
            "last_seen": datetime.fromisoformat(host["lastVulnScan"]["$date"].rstrip("Z")),
            "source": "qualys",
            "cloud_provider": host.get("cloudProvider"),
            "manufacturer": host.get("manufacturer"),
            "model": host.get("model"),
            "network_interfaces": [
                {
                    "name": interface["HostAssetInterface"].get("interfaceName"),
                    "mac_address": interface["HostAssetInterface"].get("macAddress"),
                    "ip_address": interface["HostAssetInterface"].get("address"),
                }
                for interface in host.get("networkInterface", {}).get("list", [])
            ],
            "open_ports": [
                {
                    "port": port["HostAssetOpenPort"].get("port"),
                    "protocol": port["HostAssetOpenPort"].get("protocol"),
                    "service": port["HostAssetOpenPort"].get("serviceName"),
                }
                for port in host.get("openPort", {}).get("list", [])
            ],
            "software": [
                {
                    "name": software["HostAssetSoftware"].get("name"),
                    "version": software["HostAssetSoftware"].get("version"),
                }
                for software in host.get("software", {}).get("list", [])
            ],
            "ec2_instance_id": ec2_info.get("instanceId"),
            "ec2_instance_type": ec2_info.get("instanceType"),
            "ec2_region": ec2_info.get("region"),
            "ec2_vpc_id": ec2_info.get("vpcId"),
            "ec2_subnet_id": ec2_info.get("subnetId"),
            "ec2_availability_zone": ec2_info.get("availabilityZone"),
            "ec2_private_ip": ec2_info.get("privateIpAddress"),
            "ec2_public_ip": ec2_info.get("publicIpAddress"),
            "ec2_account_id": ec2_info.get("accountId"),
            "total_memory": host.get("totalMemory"),
            "volumes":volumes,
            "vulnerabilities": [
                {
                    "qid": vuln["HostAssetVuln"].get("qid"),
                    "first_found": vuln["HostAssetVuln"].get("firstFound"),
                    "last_found": vuln["HostAssetVuln"].get("lastFound"),
                }
                for vuln in host.get("vuln", {}).get("list", [])
            ],
            "tags": [tag["TagSimple"].get("name") for tag in host.get("tags", {}).get("list", [])],
        }

    @staticmethod
    def normalize_crowdstrike_host(host: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "hostname": host.get("hostname"),
            "ip_address": host.get("local_ip"),
            "external_ip": host.get("external_ip"),
            "os": host.get("os_version"),
            "last_seen": datetime.fromisoformat(host["last_seen"].rstrip('Z')),
            "first_seen": datetime.fromisoformat(host["first_seen"].rstrip('Z')),
            "source": "crowdstrike",
            "device_id": host.get("device_id"),
            "cid": host.get("cid"),
            "agent_version": host.get("agent_version"),
            "bios_manufacturer": host.get("bios_manufacturer"),
            "bios_version": host.get("bios_version"),
            "mac_address": host.get("mac_address"),
            "instance_id": host.get("instance_id"),
            "service_provider": host.get("service_provider"),
            "service_provider_account_id": host.get("service_provider_account_id"),
            "platform_name": host.get("platform_name"),
            "kernel_version": host.get("kernel_version"),
            "system_manufacturer": host.get("system_manufacturer"),
            "system_product_name": host.get("system_product_name"),
            "tags": host.get("tags", []),
            "groups": host.get("groups", []),
            "zone_group": host.get("zone_group"),
            "status": host.get("status"),
            "policies": host.get("policies", []),
            "device_policies": host.get("device_policies", {}),
        }

    @classmethod
    def normalize_hosts(cls, hosts: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
        if source == "qualys":
            # print(hosts)
            # import pdb; pdb.set_trace()
            return [cls.normalize_qualys_host(host) for host in hosts]

        elif source == "crowdstrike":
            # print(hosts)
            # import pdb; pdb.set_trace()
            return [cls.normalize_crowdstrike_host(host) for host in hosts]
        else:
            raise ValueError(f"Unknown source: {source}")