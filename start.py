import pymysql
import os
from scrapy.utils.project import get_project_settings
# import logging
import json


def start_sync_local_images(conn):
    cursor = conn.cursor()    
    sql = 'SELECT id, agency_websit_logo, agency_homue_logo FROM homue_spider_agencies WHERE agency_websit_logo IS NOT NULL AND agency_homue_logo IS NULL AND is_live=1'
    cursor.execute(sql)
    results = cursor.fetchall()
    image_path = project_settings['IMAGES_STORE']
    for row in results:
        data_id = row[0]
        web_logo_url = row[1]
        local_logo_url = row[2]
        if (web_logo_url is not None) and (local_logo_url is None):
            print(f'{data_id}: local_logo_url is None')
            image_name = web_logo_url.split('/')[-1]
            if os.path.exists(f'{image_path}/{image_name}'):
                sql = "UPDATE homue_spider_agencies SET agency_homue_logo=%s WHERE id=%s"
                cursor.execute(sql, (image_name, data_id))
                conn.commit()
    cursor.close()
    print('Sync Local Logo Done!')

def parse_origin_address(conn):
    cursor = conn.cursor()
    sql = "SELECT id, physical_address, postal_address FROM homue_spider_agencies"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        data_id = row[0]
        physical_address = json.loads(row[1])
        postal_address = json.loads(row[2])
        city_name = physical_address.get('city', None)

        if city_name is None:
            city_name = postal_address.get('city', None)
        
        district_name = physical_address.get('address3', None)
        if district_name is None:
            district_name = postal_address.get('address3', None)
        
        detail_address = physical_address.get('address1', None)
        
        if city_name is None and district_name is None:
            if (detail_address is not None) and (len(detail_address.split(',')) > 1):
                district_name = detail_address.split(',')[-1].strip()
                
        if (detail_address is not None) and (len(detail_address.split(' ')) == 1):
            if district_name is None:
                district_name = detail_address
            
        if (detail_address is None) or (len(detail_address.split(' ')) == 1):
            detail_address = postal_address.get('address1', None)
        
        print(f"{data_id} city_name: {city_name}")
        print(f"{data_id} district_name: {district_name}")
        print(f"{data_id} detail_address: {detail_address}")
        
        # sync_agencies_to_homue(cursor=cursor, data_id = data_id, city_name=city_name, district_name=district_name)
        # agency_address_dict = {"detail_address": detail_address}
        # if district_name is not None:
        #     district_query_result = query_region_city_info(cursor=cursor, query_name=district_name, type='district')
        #     if district_query_result is not None:
        #         agency_address_dict.update(district_query_result)                  
        #     else:
        #         result = query_region_city_info(cursor=cursor, query_name=city_name, type='city')
        #         if result:
        #             agency_address_dict.update(result)
        # elif city_name is not None:
        #     result = query_region_city_info(cursor=cursor, query_name=city_name, type='city')
        #     agency_address_dict.update(result)

        # agency_parse_address = parse_address(cursor=cursor, agency_address_dict=agency_address_dict)
        # print(f'{data_id}: {agency_parse_address}')
        # add_parse_sql = "UPDATE homue_spider_agencies SET parse_address=%s WHERE id=%s"
        # cursor.execute(add_parse_sql, (json.dumps(agency_parse_address), data_id))
        # conn.commit()        
    cursor.close()

def query_region_city_info(cursor, query_name, type):
    agency_address_dict = {}
    # if (query_name == 'Hawkes bay') and (type== 'district'):
    #     return None

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

def parse_address(cursor, agency_address_dict):
    # agencies_parse_address = []
    agencies_parse_address = {}
    region_dict = agency_address_dict.get('region', None)
    city_dict = agency_address_dict.get('city', None)
    district_dict = agency_address_dict.get('district', None)
    detail_address = agency_address_dict.get('detail_address', None)
    city_id = 0
    if district_dict is not None:
        district_name = district_dict.get('name')
        district_id = district_dict.get('id')
        city_id = district_dict.get('city_id')
        agencies_parse_address.update({"district_id": district_id, "district_name": district_name})
    elif city_dict is not None:
        city_id = city_dict.get('id')
    elif region_dict is not None:
        agencies_parse_address.update({"region_id": region_dict.get('id'), "region_name": region_dict.get('name')})
    else:
        pass
    
    if city_id > 0:
        sql = f"SELECT nz_region.id AS region_id, nz_region.name AS region_name, b.id AS city_id, b.name AS city_name FROM (SELECT * FROM nz_city WHERE id={city_id}) b JOIN nz_region ON b.region_id=nz_region.id"
        cursor.execute(sql)
        nz_city_result = cursor.fetchone()
        if nz_city_result:
            agencies_parse_address.update({"region_id": nz_city_result[0], "region_name": nz_city_result[1], "city_id": nz_city_result[2], "city_name": nz_city_result[3]})
            
    address = f"{detail_address}, {agencies_parse_address.get('district_name')}, {agencies_parse_address.get('city_name')}, {agencies_parse_address.get('region_name')}" 
    agencies_parse_address.update({"address": address})
    return agencies_parse_address   

def sync_agencies_to_homue(cursor, data_id, city_name, district_name):
    if city_name == 'Mt Eden':
        city_name = 'Mount Eden'
    if district_name == 'Mt Eden':
        district_name = 'Mount Eden'    
    query_district_in_district = f"SELECT * FROM nz_district WHERE name LIKE '{district_name}%' LIMIT 1"
    query_district_in_city = f"SELECT * FROM nz_city WHERE name LIKE '{district_name}%' LIMIT 1"
    query_city_in_district = f"SELECT * FROM nz_district WHERE name LIKE '{city_name}%' LIMIT 1"
    query_city_in_city = f"SELECT * FROM nz_city WHERE name LIKE '{city_name}%' LIMIT 1"
    # print(f"{data_id}: {query_district_in_district}")
    # print(f"{data_id}: {query_district_in_city}")
    # print(f"{data_id}: {query_city_in_district}")
    # print(f"{data_id}: {query_city_in_city}")
    cursor.execute(query_district_in_district)
    district_in_district_result = cursor.fetchone()
    if district_in_district_result:
        print(f"{data_id} district_in_district_result: {district_in_district_result[0]} - {district_in_district_result[1]} - {district_in_district_result[2]}")
        
    cursor.execute(query_district_in_city)
    district_in_city_result = cursor.fetchone()
    if district_in_city_result:
        print(f"{data_id} district_in_city_result: {district_in_city_result[0]} - {district_in_city_result[1]} - {district_in_city_result[2]}")
        
    cursor.execute(query_city_in_district)
    city_in_district_result = cursor.fetchone()
    if city_in_district_result:
        print(f"{data_id} city_in_district_result: {city_in_district_result[0]} - {city_in_district_result[1]} - {city_in_district_result[2]}")  
        
    cursor.execute(query_city_in_city)
    city_in_city_result = cursor.fetchone()
    if city_in_city_result:
        print(f"{data_id} city_in_city_result: {city_in_city_result[0]} - {city_in_city_result[1]} - {city_in_city_result[2]}")
                      
#同步house_agency数据库
def update_homue_agencies(conn):
    cursor = conn.cursor()
    query_agencies_sql = "SELECT colloquial_name FROM homue_spider_agencies GROUP BY colloquial_name"
    cursor.execute(query_agencies_sql)
    results = cursor.fetchall()
    sync_agencies = []
    for item in results:
        sync_agencies.append((item[0].strip()))
    
    sync_agencies_sql = "INSERT IGNORE INTO house_agency(name) VALUES (%s)"
    cursor.executemany(sync_agencies_sql, sync_agencies)
    conn.commit()
    cursor.close()
    # conn.close() 

if __name__ == '__main__':
    project_settings = get_project_settings()
    conn = pymysql.connect(
                host=project_settings['DB_HOST'], 
                user=project_settings['DB_USER'], 
                password=project_settings['DB_PASSWORD'], 
                database=project_settings['DB_DATABASE'], 
                port=project_settings['DB_PORT']
            )
    # cursor = conn.cursor()
   
    update_homue_agencies(conn=conn)
    start_sync_local_images(conn=conn)
    parse_origin_address(conn=conn)
    conn.close()