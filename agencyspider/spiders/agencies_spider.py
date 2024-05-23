from typing import Any, Iterable
import scrapy
from scrapy import Request
from ..items import AgencyspiderItem
import logging
import json


class AgenciesSpiderSpider(scrapy.Spider):
    name = "agencies_spider"
    realestate_header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"}
    allowed_domains = ["realestate.co.nz"]
    start_urls = ["https://platform.realestate.co.nz/search/v1/offices?page[offset]=0&page[limit]=1000"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def start_requests(self):
        # return super().start_requests()
        for url in self.start_urls:
            yield Request(url=url, headers=self.realestate_header, callback=self.parse)

    def parse(self, response):
        # pass
        res_data = json.loads(response.text)
        res_meta_data = res_data.get('meta')
        agency_attributes_data = res_data.get('data')
        # agency_attributes = res_attr_data.get('attributes')
        agency_item = AgencyspiderItem()
        for attribute_data in agency_attributes_data:
            agency_attributes = attribute_data.get('attributes')
            agency_item['colloquial_name'] = agency_attributes.get('colloquial-name')
            agency_item['name'] = agency_attributes.get('name')
            agency_item['slug_name'] = agency_attributes.get('slug')
            agency_item['phone'] = agency_attributes.get('phone')
            agency_item['email'] = agency_attributes.get('email')
            agency_item['office_id'] = agency_attributes.get('office-id')
            agency_item['website_url'] = agency_attributes.get('website-url')
            agency_item['agency_websit_logo'] = agency_attributes.get('image-base-url')
            agency_item['physical_address'] = agency_attributes.get('physical-address')
            agency_item['postal_address'] = agency_attributes.get('postal-address')
            yield agency_item
