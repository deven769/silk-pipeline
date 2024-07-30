import asyncio
from typing import Optional, List, Dict, Any
from fetchers.qualys_fetcher import QualysFetcher
from fetchers.crowdstrike_fetcher import CrowdstrikeFetcher
from normalizers.normalizer import Normalizer
from deduplication.deduplicator import Deduplicator
from visualizer.visualizer import Visualizer
import aiohttp

async def fetch_hosts(fetcher, session, skip: Optional[int], limit: Optional[int]):
    try:
        return await fetcher.fetch_hosts(session, skip=skip, limit=limit)
    except aiohttp.ClientResponseError as e:
        print(f"Error fetching hosts: {e}")
        return []

async def fetch_all_hosts(fetchers, session, skip: Optional[int], limit: Optional[int]):
    tasks = [fetch_hosts(fetcher, session, skip, limit) for fetcher in fetchers]
    return await asyncio.gather(*tasks)

async def main(
    skip: Optional[int] = None,
    limit: Optional[int] = None,
    db_url: str = "",
    db: str = "",
    api_key: str = "",
) -> None:

    qualys_fetcher = QualysFetcher(api_key)
    crowdstrike_fetcher = CrowdstrikeFetcher(api_key)

    async with aiohttp.ClientSession() as session:
        try:
            fetchers = [qualys_fetcher, crowdstrike_fetcher]
            qualys_hosts, crowdstrike_hosts = await fetch_all_hosts(fetchers, session, skip, limit)
        except Exception as e:
            print(f"Error fetching hosts: {e}")
            return

    # Normalize common data for qualys and crowdstrike
    normalizer = Normalizer()
    normalized_qualys_hosts: List[Dict[str, Any]] = normalizer.normalize_hosts(qualys_hosts, "qualys")
    normalized_crowdstrike_hosts: List[Dict[str, Any]] = normalizer.normalize_hosts(crowdstrike_hosts, "crowdstrike")
    # import pdb; pdb.set_trace();

    # Deduplicate and merge hosts
    deduplicator = Deduplicator(db_url, db)
    all_hosts: List[Dict[str, Any]] = normalized_qualys_hosts + normalized_crowdstrike_hosts
    deduplicator.deduplicate_and_merge(all_hosts)

    # Generate visualizations asynchronously
    visualizer = Visualizer(output_dir="output")
    
    os_distribution = deduplicator.get_os_distribution()
    host_age_distribution = deduplicator.get_host_age_distribution()
    cloud_provider_distribution = deduplicator.get_cloud_provider_distribution()
    # import pdb; pdb.set_trace();

    visualizer.visualize_os_distribution(os_distribution)
    visualizer.visualize_host_age_distribution(host_age_distribution)
    visualizer.visualize_cloud_provider_distribution(cloud_provider_distribution)

if __name__ == "__main__":
    print(
        "---------------------------------------------------------------------------------\n"
        "Silk Pipeline is a project designed to fetch host "
        "data from Qualys and CrowdStrike, merge it into a single "
        "database, and remove duplicates based on hostname, IP address, "
        "and other criteria. Currently, only common fields are normalized, but additional fields can be included based on use case.\n"
        "There is also backup_normalizer.py which normalize more fields but I havent used that in this project."
        "-----------------------------------------------------------------------------------\n"
    )

    kwargs = {
        "skip": 1,
        "limit": 2,
        "db_url": "mongodb://127.0.0.1:27017/",
        "db": "silk_db",
        "api_key": "armis-login@armis.com_60974105-5053-4267-b16e-392e8165c89a",
    }
    
    asyncio.run(main(**kwargs))

    print('View output in the output directory, os_distribution.png and host_age_distribution.png.')
