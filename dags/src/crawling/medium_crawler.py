import logging
import aiohttp
import asyncio
import uvloop
import xmltodict
import pandas as pd
import re
import uuid

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
        self.filename = []

    async def fetch(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.error(f"Failed to fetch {url}: {response}")

    def create_rss_feed(self, company):
        return f"https://medium.com/feed/{company}"

    async def crawl(self, previous_datetime):
        tasks = [self.fetch(self.create_rss_feed(company)) for company in self.companies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            data = await self.parser(await self.clean_data(result), previous_datetime)
            await self.to_parquet(data)

        logger.debug(self.filename)

    async def clean_data(self, data):
        return re.sub(r'[^\x20-\x7E\uAC00-\uD7A3\u1100-\u11FF]', '', data)

    async def parser(self, data, previous_datetime):
        data_list = []
        data = xmltodict.parse(data)
        items = data['rss']['channel']['item']
        if previous_datetime is not None:
            for item in items:
                if item['atom:updated'] >= previous_datetime:
                    data_list.append(item)
            return data_list
        if previous_datetime is None:
            return items
        return data_list

    async def to_parquet(self, data):
        df = pd.DataFrame(data)
        utc_date_string = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        random_text = uuid.uuid4()
        df.to_parquet(f"./datalake/data/{utc_date_string}-{random_text}.parquet", engine="pyarrow", index=False)
        self.filename.append(f'{utc_date_string}-{random_text}.parquet')

if __name__ == '__main__':
    asyncio.run(MediumCrawler().crawl(""))
