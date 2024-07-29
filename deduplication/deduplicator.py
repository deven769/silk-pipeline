from typing import List, Dict, Any
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
import pytz
from dateutil.parser import parse as date_parse

class Deduplicator:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.hosts_collection = self.db["hosts"]

    def deduplicate_and_merge(self, hosts: List[Dict[str, Any]]) -> None:
        for host in hosts:
            existing_host = self.hosts_collection.find_one({
                "$or": [
                    {"hostname": host["hostname"]},
                    {"external_ip": host["external_ip"]},
                    {"local_ip": host["local_ip"]},
                    {"mac_address": host.get("mac_address")},
                ]
            })

            if existing_host:
                merged_host = self._merge_hosts(existing_host, host)
                self.hosts_collection.replace_one({"_id": existing_host["_id"]}, merged_host)
            else:
                self.hosts_collection.insert_one(host)
                
    # def deduplicate_and_merge(self, hosts: List[Dict[str, Any]], batch_size: int = 1000) -> None:
    #     operations = []
    #     for host in hosts:
    #         query = {
    #             "$or": [
    #                 {"hostname": host["hostname"]},
    #                 {"external_ip": host["external_ip"]},
    #                 {"local_ip": host["local_ip"]},
    #                 {"mac_address": host.get("mac_address")},
    #             ]
    #         }

    #         existing_host = self.hosts_collection.find_one(query)

    #         if existing_host:
    #             merged_host = self._merge_hosts(existing_host, host)
    #             operations.append(UpdateOne({"_id": existing_host["_id"]}, {"$set": merged_host}))
    #         else:
    #             # Remove the '_id' key if it exists
    #             if "_id" in host:
    #                 del host["_id"]
    #             operations.append(UpdateOne(query, {"$setOnInsert": host}, upsert=True))

    #         if len(operations) >= batch_size:
    #             self._execute_batch(operations)
    #             operations = []

    #     if operations:
    #         self._execute_batch(operations)

    # def _execute_batch(self, operations: List[UpdateOne]) -> None:
    #     try:
    #         self.hosts_collection.bulk_write(operations)
    #     except Exception as e:
    #         print(f"Error performing bulk write: {e}")

    def _merge_hosts(self, host1: Dict[str, Any], host2: Dict[str, Any]) -> Dict[str, Any]:
        merged = host1.copy()
        for key, value in host2.items():
            if key in ["last_seen", "first_seen"]:
                merged[key] = self._max_date(host1.get(key), host2.get(key))
            elif key == "source":
                merged[key] = f"{host1.get('source', '')},{host2['source']}".strip(',')
            elif key in ["tags"]:
                merged[key] = self._merge_lists(merged.get(key, []), value)
            else:
                # Prefer non-null values
                merged[key] = value if value is not None else merged.get(key)
        return merged

    def _max_date(self, date1: Any, date2: Any) -> datetime:
        """Convert strings to datetime objects and return the maximum date."""
        date1 = self._convert_to_utc(date1)
        date2 = self._convert_to_utc(date2)
        return max(date1, date2)

    def _convert_to_utc(self, date: Any) -> datetime:
        """Convert a date to a UTC datetime object."""
        if isinstance(date, str):
            try:
                # Remove redundant 'Z' if there is already a timezone offset
                if date.endswith('Z') and '+' in date:
                    date = date.rstrip('Z')
                date = date_parse(date).astimezone(pytz.UTC)
            except ValueError:
                raise ValueError(f"Invalid date format: {date}")
        elif isinstance(date, datetime):
            if date.tzinfo is None:
                date = date.replace(tzinfo=pytz.UTC)
            else:
                date = date.astimezone(pytz.UTC)
        return date

    def _merge_lists(self, list1: List[Any], list2: List[Any]) -> List[Any]:
        merged = list1.copy()
        for item in list2:
            if item not in merged:
                merged.append(item)
        return merged

    def get_os_distribution(self) -> Dict[str, int]:
        pipeline = [
            {"$group": {"_id": "$os_version", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        result = self.hosts_collection.aggregate(pipeline)
        return {doc["_id"]: doc["count"] for doc in result}

    def get_host_age_distribution(self) -> Dict[str, int]:
        thirty_days_ago = datetime.utcnow().replace(tzinfo=pytz.UTC) - timedelta(days=30)
        old_hosts = self.hosts_collection.count_documents({"last_seen": {"$lt": thirty_days_ago}})
        new_hosts = self.hosts_collection.count_documents({"last_seen": {"$gte": thirty_days_ago}})
        return {"old_hosts": old_hosts, "new_hosts": new_hosts}

    def get_cloud_provider_distribution(self) -> Dict[str, int]:
        pipeline = [
            {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        result = self.hosts_collection.aggregate(pipeline)
        return {doc["_id"]: doc["count"] for doc in result}
