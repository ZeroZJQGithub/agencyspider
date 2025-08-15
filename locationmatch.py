import urllib3
import json
import pymysql
import os
from scrapy.utils.project import get_project_settings
import sys
import logging
import scrapy
from scrapy import Request
from urllib import parse
import requests
import pandas as pd
from slugify import slugify


def query_address_district(conn, address_name):
    try:
        city_sql = "SELECT * FROM nz_district WHERE fq_slug LIKE %s"
        param = f"%{address_name}"
        cursor = conn.cursor()
        cursor.execute(city_sql, (param,))
        city_results = cursor.fetchall()
        cursor.close()
        if len(city_results) == 1:
            return city_results[0][4]
        else:
            return None        
    except:
        print("Parse Address3 Into Database Unexpected error:", sys.exc_info()[0])


def udpate_district_name(conn, agency_id, district_name):
    try:
        sql = "UPDATE homue_spider_agencies SET district_name=%s WHERE id=%s"
        cursor = conn.cursor()
        cursor.execute(sql, (district_name, agency_id))
        conn.commit()
        cursor.close()
    except:
        print("Update district_name Into Database Unexpected error:", sys.exc_info()[0])

def parse_address(conn):
    try:
        sql = "SELECT id, physical_address FROM homue_spider_agencies WHERE id > 149 AND LENGTH(detail_address) > 0"
        cursor = conn.cursor()
        cursor.execute(sql)
        agency_detail_address_results = cursor.fetchall()
        for item in agency_detail_address_results:
            physical_address = json.loads(item[1])
            city = physical_address.get('city')
            address3 = physical_address.get('address3')
            # print(f"city: {physical_address.get('city')}, address3: {physical_address.get('address3')}")
            match_district_slug_name = None
            if city is not None:
                print(f"query city")
                city_name = slugify(physical_address.get('city').strip())
                query_city_result = query_address_district(conn=conn, address_name=city_name)
                
                if query_city_result is not None:
                   match_district_slug_name = query_city_result
                #    print(f"Found the district with city name: {query_city_result}")
                #    continue 
            if address3 is not None and match_district_slug_name is None:
                print(f"query address3")
                address3 = slugify(physical_address.get('address3').strip())
                query_city_result = query_address_district(conn=conn, address_name=address3)
                
                if query_city_result is not None:
                   match_district_slug_name = query_city_result
                #    print(f"Found the district with address3 name: {query_city_result}")
                #    continue
            print(f"agency_id: {item[0]}, match_district_slug_name: {match_district_slug_name}")    
            udpate_district_name(conn=conn, agency_id=item[0], district_name=match_district_slug_name)
    except:
        print("Parse Address Into Database Unexpected error:", sys.exc_info()[0]) 



def location_matching(conn):
    try:
        sql = "SELECT id, detail_address FROM homue_spider_agencies WHERE LENGTH(detail_address)>0 AND district_name IS NULL"
        cursor = conn.cursor()
        cursor.execute(sql)
        agency_detail_address_results = cursor.fetchall()
        cursor.close()
        location_search_url_list = []
        location_base_search_url = "https://nominatim.openstreetmap.org/search?"
        for item in agency_detail_address_results:
            location_search_params = {
                'q': item[1],
                'accept-language': 'en',
                'countrycodes':'nz',
                'format': 'json',
                'addressdetails': 1
            }
            location_search_url = location_base_search_url + parse.urlencode(location_search_params)
            print(location_search_url)
            location_search_url_list.append((item[0], item[1], location_search_url))
        
        df = pd.DataFrame(data=location_search_url_list)
        df.to_excel('output_location_urls_v3.xlsx', index=False)
        # response = requests.get(location_search_url, headers=headers, allow_redirects=False, timeout=30, verify=False)
        # print(response.text)
    except:
        print("Insert Agents Into Database Unexpected error:", sys.exc_info()[0])    

def parse_google_match_locations(conn):
    file_path = 'google_match_location3_v3.xlsx'
    excel_data_df = pd.read_excel(file_path, index_col=False)
    location_dict = excel_data_df.to_dict(orient='records')
    # print(location_dict)
    for item in location_dict:
        print(f"id: {item.get('id')}, Suburb: {item.get('Suburb')}, City: {item.get('City')}, Postcode: {item.get('Postcode')}")
        suburb_name = slugify(item.get('City').strip())+'_'+slugify(item.get('Suburb').strip())
        query_district_result = query_address_district(conn=conn, address_name=suburb_name)
        
        if query_district_result is not None:
            udpate_district_name(conn=conn, agency_id=item.get('id'), district_name=query_district_result)      
            
    print('Parse Google Match Location Done!')

def request_openstreetmap_url():
    header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"}
    location_search_url = 'https://nominatim.openstreetmap.org/search?q=12+Girven+Road++BAYFAIR&accept-language=en&countrycodes=nz&format=json&addressdetails=1'
    response = requests.get(location_search_url, headers=header, allow_redirects=False, timeout=60, verify=False)
    print(response.text)

if __name__ == '__main__':
    project_settings = get_project_settings()
    conn = pymysql.connect(
                host=project_settings['DB_HOST'], 
                user=project_settings['DB_USER'], 
                password=project_settings['DB_PASSWORD'], 
                database=project_settings['DB_DATABASE'], 
                port=project_settings['DB_PORT']
            ) 
       
    # location_matching(conn=conn)
    # parse_address(conn=conn)
    # conn.close()
    # print(f"full address: {full_address}")
    # request_openstreetmap_url()
    parse_google_match_locations(conn=conn)