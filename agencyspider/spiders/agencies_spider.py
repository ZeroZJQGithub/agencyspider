from typing import Any, Iterable
import scrapy
from scrapy import Request
from ..items import AgencyspiderItem
import logging
import json
from urllib import parse
import math
from datetime import datetime
from dateutil import tz
from scrapy.utils.defer import maybe_deferred_to_future

class AgenciesSpiderSpider(scrapy.Spider):
    name = "agencies_spider"
    realestate_header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"}
    allowed_domains = ["realestate.co.nz", "openstreetmap.org"]
    start_urls = ["https://platform.realestate.co.nz/search/v1/offices?page[offset]=0&page[limit]=1000"]
    offices_base_url = "https://platform.realestate.co.nz/search/v1/offices?"
    page_offset = 0
    page_limit = 50
    location_base_search_url = "https://nominatim.openstreetmap.org/search?"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def start_requests(self):
        # return super().start_requests()
        offices_url_params = {
            'page[offset]': self.page_offset,
            'page[limit]': self.page_limit
        }
        offices_url = self.offices_base_url + parse.urlencode(offices_url_params)
        yield Request(url=offices_url, headers=self.realestate_header, callback=self.parse)
        # location_search_params = {
        #     'q': '169 Hurndall Street West, Maungaturoto 0520',
        #     'accept-language': 'en',
        #     'countrycodes':'nz',
        #     'format': 'json',
        #     'addressdetails': 1
        # }
        # location_search_url = self.location_base_search_url + parse.urlencode(location_search_params)
        # yield Request(url=location_search_url, headers=None, callback=self.parse_location)
    
    def parse_location(self, response):
        logging.info(json.loads(response.text))    

    async def parse(self, response):
        # pass
        res_data = json.loads(response.text)
        if res_data is not None:
            nz_tz = tz.gettz('Pacific/Auckland')
            updated_at = datetime.now(tz=nz_tz).strftime('%Y-%m-%d %H:%M:%S')
            res_meta_data = res_data.get('meta')
            agency_attributes_data = res_data.get('data')
            agency_item = AgencyspiderItem()
            for attribute_data in agency_attributes_data:
                relationship_agents = attribute_data.get('relationships').get('agents').get('data')
                agency_attributes = attribute_data.get('attributes')
                agency_item['colloquial_name'] = agency_attributes.get('colloquial-name')
                agency_item['name'] = agency_attributes.get('name')
                agency_item['slug_name'] = agency_attributes.get('slug')
                agency_item['phone'] = agency_attributes.get('phone')
                agency_item['email'] = agency_attributes.get('email')
                agency_item['office_id'] = agency_attributes.get('office-id')
                agency_item['website_url'] = agency_attributes.get('website-url')
                agency_item['agency_websit_logo'] = agency_attributes.get('image-base-url')
                physical_address = agency_attributes.get('physical-address')
                agency_item['physical_address'] = physical_address
                agency_item['postal_address'] = agency_attributes.get('postal-address')
                detail_address = physical_address.get('address1', '') + ',' + physical_address.get('address3', '') + ',' + physical_address.get('city', '')
                detail_address = detail_address.strip(',')
                agency_item['detail_address'] = detail_address
                agency_item['is_live'] = agency_attributes.get('is-live')
                agency_item['city_name'] = physical_address.get('city')
                agency_item['agents'] = [(item.get('id'), 'realestate.co.nz', updated_at) for item in relationship_agents]
                # location_search_params = {
                #     'q': detail_address,
                #     'accept-language': 'en',
                #     'countrycodes':'nz',
                #     'format': 'json',
                #     'addressdetails': 1
                # }
                # location_search_url = self.location_base_search_url + parse.urlencode(location_search_params)
                # location_request = Request(url=location_search_url, headers=self.realestate_header)
                # loaction_deferred = self.crawler.engine.download(location_request)
                # location_response = await maybe_deferred_to_future(loaction_deferred)
                
                # logging.info(location_response)
                
                yield agency_item

            total_resultes = res_meta_data.get('totalResults')
            resultes_per_page = res_meta_data.get('resultsPerPage')
            current_page_number = int(res_meta_data.get('pageNumber'))
            total_page = math.ceil(int(total_resultes)/int(resultes_per_page))
            if current_page_number < total_page:
                offices_url_params = {
                    'page[offset]': self.page_limit * current_page_number,
                    'page[limit]': self.page_limit
                }
                next_page_url = self.offices_base_url + parse.urlencode(offices_url_params)
                yield Request(url=next_page_url, headers=self.realestate_header, callback=self.parse)
                
        else:
            pass
