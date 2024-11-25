import logging

import aiohttp
import asyncio
import uvloop
import xmltodict
import pandas as pd
import re
import uuid

from datetime import datetime, timezone

from src.config.logger.logger_setup import setup_logging

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

    async def fetch(self, url: str) -> str:
        """
        API GET 요청을 비동기로 수행합니다.

        Args:
            url (str): 요청을 보낼 API의 URL.

        Returns:
            str: API 응답 문자열.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.error(f"Failed to fetch {url}: {response}")

    def create_rss_feed_url(self, company: str) -> str:
        """
        RSS Feed URL을 생성합니다.

        Args:
            company (str): 기술 블로그 상의 회사명.

        Returns:
            str: RSS Feed URL.
        """
        return f"https://medium.com/feed/{company}"

    async def crawl(self, previous_datetime: str) -> list[str]:
        """
        RSS 피드를 기반으로 데이터를 크롤링합니다.

        Args:
            previous_datetime (str): 이전 실행 날짜.

        Returns:
            list[str]: 저장된 데이터 파일 이름 리스트.
        """
        tasks = [self.fetch(self.create_rss_feed_url(company)) for company in self.companies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            data = await self.parser(await self.clean_data(result), previous_datetime)
            await self.to_parquet(data)
        logger.debug(self.filename)
        return self.filename

    async def clean_data(self, data: str) -> str:
        """
        RSS 피드 XML 응답에서 특수 문자와 매핑되지 않는 문자를 제거합니다.

        Args:
            data (str): 처리할 XML 데이터 문자열.

        Returns:
            str: 특수 문자가 제거된 데이터 문자열.
        """
        return re.sub(r'[^\x20-\x7E\uAC00-\uD7A3\u1100-\u11FF]', '', data)

    async def parser(self, data: str, previous_datetime: str) -> list[dict]:
        """
        주어진 날짜 기준으로 수집할 데이터를 필터링합니다.

        Args:
            data (str): XML 데이터 문자열.
            previous_datetime (str): 이전 실행 날짜.

        Returns:
            list[dict]: 필터링된 데이터의 리스트.
        """
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

    async def to_parquet(self, data: list[dict]) -> None:
        """
        데이터를 파케이 형식으로 변환하여 저장합니다.

        Args:
            data (list[dict]): 저장할 데이터 리스트.

        Returns:
            None
        """
        df = pd.DataFrame(data)
        utc_date_string = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        random_text = uuid.uuid4()
        df.to_parquet(f"./datalake/data/{utc_date_string}-{random_text}.parquet", engine="pyarrow", index=False)
        self.filename.append(f'{utc_date_string}-{random_text}.parquet')


if __name__ == '__main__':
    asyncio.run(MediumCrawler().crawl(""))
