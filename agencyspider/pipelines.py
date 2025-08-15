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
import scrapy
from fuzzywuzzy import process


class AgencyspiderPipeline:
    def __init__(self, db_settings=None) -> None:
        self.db_settings = db_settings
        self.insert_items = []
        self.insert_agents_items = []
        self.item_count = 0
        self.media_url = "https://mediaserver.realestate.co.nz"

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
        
        sql = f"SELECT id, fq_slug FROM nz_district"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        nz_district_results = cursor.fetchall()
        self.nz_districts = [item[1] for item in nz_district_results]
        cursor.close()       

    def close_spider(self, spider):
        if self.item_count > 0:
            self.insert_agency_to_database(self.insert_items)
            
        if len(self.insert_agents_items) > 0:
            self.insert_agents_to_database(self.insert_agents_items)
            
        self.conn.close()

    def process_item(self, item, spider):
        created_at = datetime.now(tz=self.nz_tz).strftime('%Y-%m-%d %H:%M:%S')
        item['created_at'] = created_at
        item['updated_at'] = created_at 
        colloquial_name = item.get('colloquial_name', '')
        item['slug_colloquial_name'] = slugify(colloquial_name) if colloquial_name != '' else ''      
        item['physical_address'] = json.dumps(item.get('physical_address', []))
        item['postal_address'] = json.dumps(item.get('postal_address', []))

        agency_websit_logo = item.get('agency_websit_logo', None)
        if agency_websit_logo is not None:
            item['agency_websit_logo'] = self.media_url + agency_websit_logo + '.scale.x40.jpg'

        is_live = item.get('is_live', None)
        if is_live is not None:
            item['is_live'] = 1 if is_live == True else 0
        else:
            item['is_live'] = 0

        office_id = item.get('office_id', None)
        if office_id is not None:
            item['office_id'] = str(office_id)
        
        detail_address = item.get('detail_address')
        item['detail_address'] = detail_address.strip()
        
        if item.get('city_name') is not None:
            self.match_district, score = process.extractOne(slugify(item.get('city_name')), self.nz_districts)
            logging.info(f"{item.get('detail_address')}: {self.match_district}: {score}")
        else:
            self.match_district = None

        self.insert_items.append((item.get('colloquial_name'), item.get('slug_colloquial_name'), item.get('name'), 
                                  item.get('slug_name'), item.get('phone'), item.get('email'), item.get('office_id'),
                                  item.get('website_url'), item.get('agency_websit_logo'), item.get('physical_address'), 
                                  item.get('postal_address'), item.get('detail_address'), item.get('is_live'), self.match_district, item.get('created_at'), item.get('updated_at')))
        
        agents_insert_data = item.get('agents')
        if len(agents_insert_data) > 0:
            # logging.info(agents_insert_data)
            self.insert_agents_items += agents_insert_data
            
        if len(self.insert_agents_items) > 200:
            self.insert_agents_to_database(self.insert_agents_items)
        
        self.item_count += 1
        if self.item_count >= 100:
            self.insert_agency_to_database(self.insert_items)
        return item
    
    def insert_agency_to_database(self, insert_data):
        try:
            sql = "INSERT INTO homue_spider_agencies(colloquial_name, slug_colloquial_name, name, slug_name, phone, email, office_id, website_url, agency_websit_logo, physical_address, postal_address, detail_address, is_live, district_name, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)"
            cursor = self.conn.cursor()
            cursor.executemany(sql, insert_data)
            self.conn.commit()                
            cursor.close()
            self.insert_items.clear()
            self.item_count = 0
        except:
            print("Insert Agencies Into Database Unexpected error:", sys.exc_info()[0])
            
    def insert_agents_to_database(self, insert_data):
        try:
            sql = "INSERT INTO third_party_agents(agent_id, origin_party, updated_at) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)"
            cursor = self.conn.cursor()
            cursor.executemany(sql, insert_data)
            self.conn.commit()                
            cursor.close()
            self.insert_agents_items.clear()
        except:
            print("Insert Agents Into Database Unexpected error:", sys.exc_info()[0])                    

class AgencyImagesPipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        # return super().file_path(request, response, info, item=item)
        image_name = request.url.split("/")[-1]
        return image_name

    def get_media_requests(self, item, info):
        realestate_header = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"}
        photo_url = item.get('agency_websit_logo', None)
        if photo_url is not None:
            yield scrapy.Request(url=photo_url, headers=realestate_header)
    
    def item_completed(self, results, item, info):
        # adapter = ItemAdapter(item)
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            #  item['agency_homue_logo'] = None
            # raise DropItem("Item contains no images")
            pass
        else:
            item['agency_homue_logo'] = image_paths[0]
        return item
  