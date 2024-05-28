import pymysql
import os
from scrapy.utils.project import get_project_settings
# import logging
import json

def start_sync_local_images():
    project_settings = get_project_settings()
    conn = pymysql.connect(
                host=project_settings['DB_HOST'], 
                user=project_settings['DB_USER'], 
                password=project_settings['DB_PASSWORD'], 
                database=project_settings['DB_DATABASE'], 
                port=project_settings['DB_PORT']
            )
    
    sql = 'SELECT id, agency_websit_logo, agency_homue_logo, physical_address, postal_address FROM homue_spider_agencies WHERE is_live=1'
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    
    agencies_address_list = []
    image_path = project_settings['IMAGES_STORE']
    for row in results:
        data_id = row[0]
        web_logo_url = row[1]
        local_logo_url = row[2]
        if (web_logo_url is not None) and (local_logo_url is None):
            print(f'{data_id}: local_logo_url is None')
            image_name = web_logo_url.split('/')[-1]
            if os.path.exists(f'{image_path}/{image_name}') :
                sql = "UPDATE homue_spider_agencies SET agency_homue_logo=%s WHERE id=%s"
                cursor.execute(sql, (image_name, data_id))
                conn.commit()
        physical_address = json.loads(row[3])
        postal_address = json.loads(row[4])
        city_name = physical_address.get('city') if physical_address.get('city', None) is not None else postal_address.get('city', None)
        district_name = physical_address.get('address3', None)
        detail_address = physical_address.get('address1', None)
        agency_address_dict = {"id": data_id, "detail_address": detail_address}
        if district_name is not None:
            district_query_result = query_region_city_info(cursor=cursor, query_name=district_name, type='district')
            if district_query_result is not None:
                agency_address_dict.update(district_query_result)                  
                # agencies_address_list.append(agency_address_dict)
        elif city_name is not None:
            result = query_region_city_info(cursor=cursor, query_name=city_name, type='city')
            agency_address_dict.update(result)
        agencies_address_list.append(agency_address_dict)

            
        # if (city_name is None) and (district_name is None):
        #     postal_address = json.loads(row[4])
        #     city_name = postal_address.get('city', None)
        #     district_name = postal_address.get('address3', None)
        #     if district_name is not None:    
        #         district_query_result = query_region_city_info(cursor=cursor, query_name=district_name, type='district')
        #         if district_query_result is not None:
        #             agency_address_dict.update(district_query_result)
        #             agencies_address_list.append(agency_address_dict)        
        #         elif city_name is not None:    
        #             result = query_region_city_info(cursor=cursor, query_name=city_name, type='city')
        #             agency_address_dict.update(result)
        #             agencies_address_list.append(agency_address_dict)
                                       
    
    # print(agencies_address_list) 
    parse_address_list = parse_address(cursor=cursor, agencies_address_list=agencies_address_list)        
    print(parse_address_list)
    cursor.close()
    conn.close()
    # print(agencies_address_list)

def query_region_city_info(cursor, query_name, type):
    agency_address_dict = {}
    if (query_name == 'Hawkes bay') and (type== 'district'):
        return None
    district_sql = f"SELECT * FROM nz_district WHERE name LIKE '{query_name}%' LIMIT 1"
    cursor.execute(district_sql)
    nz_district_result = cursor.fetchone()
    if nz_district_result is not None:
        agency_address_dict.update({"district": {"id": nz_district_result[0], "city_id": nz_district_result[1], "name": nz_district_result[2]}})
    else:
        city_sql = f"SELECT * FROM nz_city WHERE name LIKE '{query_name}%' LIMIT 1"
        cursor.execute(city_sql)
        nz_city_result = cursor.fetchone()        
        if nz_city_result is not None:
            agency_address_dict.update({"city": {"id": nz_city_result[0], "region_id": nz_city_result[1], "name": nz_city_result[2]}})
        else:
            region_sql = f"SELECT * FROM nz_region WHERE name LIKE '{query_name}%' LIMIT 1"
            cursor.execute(region_sql)
            nz_region_result = cursor.fetchone()   
            if nz_region_result is not None:
                agency_address_dict.update({"region": {"id": nz_region_result[0], "name": nz_region_result[1]}})                        
    return agency_address_dict

def parse_address(cursor, agencies_address_list):
    agencies_parse_address = []
    for agencies_address_item in agencies_address_list:
        data_id = agencies_address_item.get('id')
        region_dict = agencies_address_item.get('region', None)
        city_dict = agencies_address_item.get('city', None)
        district_dict = agencies_address_item.get('district', None)
        detail_address = agencies_address_item.get('detail_address', None)
        agency_parse_address = {"id": data_id}
        city_id = 0
        if district_dict is not None:
            district_name = district_dict.get('name')
            district_id = district_dict.get('id')
            city_id = district_dict.get('city_id')
            agency_parse_address.update({"district_id": district_id, "district_name": district_name})
        elif city_dict is not None:
            city_id = city_dict.get('id')
        elif region_dict is not None:
            agency_parse_address.update({"region_id": region_dict.get('id'), "region_name": region_dict.get('name')})
        else:
            pass
        
        if city_id > 0:
            sql = f"SELECT nz_region.id AS region_id, nz_region.`name` AS region_name, b.id AS city_id, b.name AS city_name FROM (SELECT * FROM nz_city WHERE id={city_id}) b JOIN nz_region ON b.region_id=nz_region.id"
            cursor.execute(sql)
            nz_city_result = cursor.fetchone()
            if nz_city_result:
                agency_parse_address.update({"region_id": nz_city_result[0], "region_name": nz_city_result[1], "city_id": nz_city_result[2], "city_name": nz_city_result[3]})
                
        address = f"{detail_address}, {agency_parse_address.get('district_name')}, {agency_parse_address.get('city_name')}, {agency_parse_address.get('region_name')}" 
        agency_parse_address.update({"address": address})
        agencies_parse_address.append(agency_parse_address)
        
    return agencies_parse_address   

if __name__ == '__main__':
    start_sync_local_images()