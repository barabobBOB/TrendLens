import logging
import aiohttp
import asyncio
import uvloop
import xmltodict
import pandas as pd
import re
from io import BytesIO
from datetime import datetime, timezone

from dags.src.config.logger.logger_setup import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class MediumCrawler:
    def __init__(self):
        self.companies = [
            "daangn",
            "musinsa-tech",
            "watcha",
            "coupang-engineering"
        ]

    async def fetch(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.error(f"Failed to fetch {url}: {response}")

    def create_rss_feed(self, company):
        return f"https://medium.com/feed/{company}"

    async def crawl(self):
        tasks = [self.fetch(self.create_rss_feed(company)) for company in self.companies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            data = await self.parser(await self.clean_data(result), "2024-11-20T07:17:53.534Z")
            await self.to_parquet(data)

    async def clean_data(self, data):
        return re.sub(r'[^\x20-\x7E\uAC00-\uD7A3\u1100-\u11FF]', '', data)

    async def parser(self, data, previous_datetime):
        last_data = []
        data = xmltodict.parse(data)
        items = data['rss']['channel']['item']
        if previous_datetime is not None:
            for item in items:
                if item['atom:updated'] >= previous_datetime:
                    last_data.append(item)
            return last_data
        if previous_datetime is None:
            return items

        return last_data

    async def to_parquet(self, data):
        df = pd.DataFrame(data)
        utc_date_string = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        df.to_parquet(f"./datalake/data/{utc_date_string}/{}.parquet", engine="pyarrow", index=False)

if __name__ == '__main__':
    asyncio.run(MediumCrawler().crawl())
