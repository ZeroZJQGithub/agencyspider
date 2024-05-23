# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AgencyspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # pass
    colloquial_name = scrapy.Field()
    slug_colloquial_name = scrapy.Field()
    name = scrapy.Field()
    slug_name = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    office_id = scrapy.Field()
    website_url = scrapy.Field()
    agency_websit_logo = scrapy.Field()
    agency_homue_logo = scrapy.Field()
    physical_address = scrapy.Field()
    postal_address = scrapy.Field()
    is_live = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()
