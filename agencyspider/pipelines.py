# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql
import logging
from dateutil import tz
import sys
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline
import json
from datetime import datetime
from slugify import slugify

class AgencyspiderPipeline:
    def __init__(self, db_settings=None) -> None:
        self.db_settings = db_settings
        self.insert_items = []

    @classmethod
    def from_crawler(cls, crawler):
        db_settings = {
            'DB_HOST': crawler.settings.get('DB_HOST'),
            'DB_USER': crawler.settings.get('DB_USER'),
            'DB_PASSWORD': crawler.settings.get('DB_PASSWORD'),
            'DB_DATABASE': crawler.settings.get('DB_DATABASE'),
            'DB_PORT': crawler.settings.get('DB_PORT')            
        }
        return cls(db_settings)

    def open_spider(self, spider):
        self.nz_tz = tz.gettz('Pacific/Auckland')
        self.conn = pymysql.connect(
                host=self.db_settings.get('DB_HOST'), 
                user=self.db_settings.get('DB_USER'), 
                password=self.db_settings.get('DB_PASSWORD'), 
                database=self.db_settings.get('DB_DATABASE'), 
                port=self.db_settings.get('DB_PORT')
            )

    def close_spider(self, spider):
        if len(self.insert_items) == 0:
            # self.conn.close()
            pass
        else:
            self.insert_items_to_database(self.insert_items)
                    
        self.conn.close()

    def process_item(self, item, spider):
        created_at = datetime.now(tz=self.nz_tz).strftime('%Y-%m-%d %H:%M:%S')
        item['created_at'] = created_at
        item['updated_at'] = created_at 
        colloquial_name = item.get('colloquial_name', '')
        item['slug_colloquial_name'] = slugify(colloquial_name) if colloquial_name != '' else ''      
        item['physical_address'] = json.dumps(item.get('physical_address', []))
        item['postal_address'] = json.dumps(item.get('postal_address', []))    
        return item
    
    def insert_items_to_database(self, insert_data):
        try:
            sql = "INSERT INTO homue_spider_agencies(colloquial_name, slug_colloquial_name, name, slug_name, phone, email, office_id, website_url, agency_websit_logo, physical_address, postal_address, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)"
            cursor = self.conn.cursor()
            cursor.executemany(sql, insert_data)
            self.conn.commit()
            cursor.close()
            self.insert_items.clear()
            self.item_count = 0
        except:
            print("Insert Into Database Unexpected error:", sys.exc_info()[0])


class HomeImagesPipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        # return super().file_path(request, response, info, item=item)
        image_name = request.url.split("/")[-1]
        return image_name

    def get_media_requests(self, item, info):
        realestate_header = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"}
        photos = item['photos']
        for photo in photos:
            image_url = photo.get('url')
            yield scrapy.Request(url=image_url, headers=realestate_header)
    
    def item_completed(self, results, item, info):
        # return super().item_completed(results, item, info)
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        adapter = ItemAdapter(item)
        adapter['image_paths'] = json.dumps(image_paths)

        item['photos'] = json.dumps(item['photos'])
        global item_images_path
        item_images_path.append((adapter['houseId'], adapter['image_paths']))
        return item