from typing import Dict, Any, List
from datetime import datetime
import pytz  # Ensure you have pytz installed

class Normalizer:
    @staticmethod
    def _convert_to_iso(date_value: Any) -> str:
        """Convert various date formats to ISO 8601 format with UTC timezone."""
        if isinstance(date_value, str):
            try:
                # Handle ISO 8601 format with or without 'Z' at the end
                date_value = datetime.fromisoformat(date_value.rstrip('Z')).replace(tzinfo=pytz.UTC)
            except ValueError:
                try:
                    # Handle common formats that may not be ISO 8601
                    date_value = datetime.strptime(date_value, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.UTC)
                except ValueError:
                    print(f"Invalid date format: {date_value}")
                    return date_value
        elif isinstance(date_value, datetime):
            # Directly convert datetime to ISO 8601 format
            return date_value.astimezone(pytz.UTC).isoformat()
        elif isinstance(date_value, (int, float)):
            # Handle timestamps (assumed to be in seconds since epoch)
            return datetime.fromtimestamp(date_value, pytz.UTC).isoformat()
        
        # Ensure all dates are converted to UTC and then to ISO 8601 format
        return date_value.astimezone(pytz.UTC).isoformat() + 'Z'

    @staticmethod
    def normalize_qualys_host(host: Dict[str, Any]) -> Dict[str, Any]:
        ec2_info = next((source['Ec2AssetSourceSimple'] for source in host.get('sourceInfo', {}).get('list', []) 
                         if 'Ec2AssetSourceSimple' in source), {})
        return {
            "host_id": host.get("agentInfo", {}).get("agentId"),
            "hostname": host.get("dnsHostName") or host.get("fqdn"),
            "external_ip": ec2_info.get("publicIpAddress"),
            "local_ip": host.get("address"),
            "mac_address": host.get("networkInterface", {}).get("list", [{}])[0].get("HostAssetInterface", {}).get("macAddress"),
            "platform": host.get("agentInfo", {}).get("platform"),
            "os_version": host.get("os"),
            "cpu": host.get("processor", {}).get("list", [{}])[0].get("HostAssetProcessor", {}).get("name"),
            "status": host.get("agentInfo", {}).get("status"),
            "first_seen": Normalizer._convert_to_iso(host.get("created")),
            "last_seen": Normalizer._convert_to_iso(host.get("agentInfo", {}).get("lastCheckedIn", {}).get("$date")),#Normalizer._convert_to_iso(host.get("modified")),
            # "last_checked_in": Normalizer._convert_to_iso(host.get("agentInfo", {}).get("lastCheckedIn", {}).get("$date")),
            "tags": [tag["TagSimple"].get("name") for tag in host.get("tags", {}).get("list", [])],
        }

    @staticmethod
    def normalize_crowdstrike_host(host: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "host_id": host.get("device_id"),
            "hostname": host.get("hostname"),
            "external_ip": host.get("external_ip"),
            "local_ip": host.get("local_ip"),
            "mac_address": host.get("mac_address"),
            "platform": host.get("platform_name"),
            "os_version": host.get("os_version"),
            "cpu": host.get("cpu_signature"),
            "status": host.get("status"),
            "first_seen": Normalizer._convert_to_iso(host.get("first_seen")),
            "last_seen": Normalizer._convert_to_iso(host.get("last_seen")),
            # "last_checked_in": Normalizer._convert_to_iso(host.get("agent_local_time")),
            "tags": host.get("tags", []),
        }

    @classmethod
    def normalize_hosts(cls, hosts: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
        if source == "qualys":
            normalized_hosts = [cls.normalize_qualys_host(host) for host in hosts]
        elif source == "crowdstrike":
            normalized_hosts = [cls.normalize_crowdstrike_host(host) for host in hosts]
        else:
            raise ValueError(f"Unknown source: {source}")
        
        return normalized_hosts
