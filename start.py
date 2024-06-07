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
                homue_image_name = f"https://homuestoragedev.blob.core.windows.net/agencies-logo/images/{image_name}"
                cursor.execute(sql, (homue_image_name, data_id))
                conn.commit()
    cursor.close()
    print('Sync Local Logo Done!')

def parse_origin_address(conn):
    cursor = conn.cursor()
    sql = "SELECT id, physical_address, postal_address FROM homue_spider_agencies WHERE parse_address IS NULL"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        data_id = row[0]
        physical_address = json.loads(row[1])
        postal_address = json.loads(row[2])

        district_name = physical_address.get('city', None)
        address3 = physical_address.get('address3', None)
        detail_address = physical_address.get('address1', None)

        district_query_result = query_district_info(cursor=cursor, district_name=district_name, address3=address3)
        
        agency_address_dict = {"detail_address": detail_address}
        if len(district_query_result) > 0:
            agency_address_dict.update(district_query_result)

        agency_parse_address = parse_address(cursor=cursor, agency_address_dict=agency_address_dict)
        print(f'{data_id}: {agency_parse_address}')
        add_parse_sql = "UPDATE homue_spider_agencies SET parse_address=%s WHERE id=%s"
        cursor.execute(add_parse_sql, (json.dumps(agency_parse_address), data_id))
        conn.commit()        
    cursor.close()

def query_district_info(cursor, district_name, address3):
    district_name = district_name.lower() if district_name else None
    address3 = address3.lower() if address3 else None
    
    if district_name and ('mt ' in district_name):
        district_name = district_name.replace('mt ', 'mount ')
    if address3 and ('Mt ' in address3):
        address3 = address3.replace('mt ', 'mount ')
    if district_name and ('st ' in district_name):
        district_name = district_name.replace('st ', 'saint ')
    if address3 and ('st ' in address3):
        address3 = address3.replace('st ', 'saint ')
    if district_name and ('cbd' in district_name):
        district_name = district_name.replace('cbd', 'central')
    if address3 and ('cbd' in address3):
        address3 = address3.replace('cbd', 'central') 
  
    district_info_dict = {}
    district_sql = f"SELECT * FROM nz_district WHERE name LIKE '{district_name}%' LIMIT 1"
    cursor.execute(district_sql)
    nz_district_result = cursor.fetchone()
    if nz_district_result:
        district_info_dict.update({"district": {"id": nz_district_result[0], "city_id": nz_district_result[1], "name": nz_district_result[2]}})     
    else:
        city_sql = f"SELECT * FROM nz_district WHERE name LIKE '{address3}%' LIMIT 1"
        cursor.execute(city_sql)
        nz_district_result = cursor.fetchone()
        if nz_district_result:
            district_info_dict.update({"district": {"id": nz_district_result[0], "city_id": nz_district_result[1], "name": nz_district_result[2]}})
    # cursor.close()
    return district_info_dict

def parse_address(cursor, agency_address_dict):
    agencies_parse_address = {}
    district_dict = agency_address_dict.get('district', None)
    detail_address = agency_address_dict.get('detail_address', None)
    address = detail_address if detail_address else ''
    city_id = 0
    if district_dict is not None:
        district_name = district_dict.get('name')
        district_id = district_dict.get('id')
        city_id = district_dict.get('city_id')
        agencies_parse_address.update({"district_id": district_id, "district_name": district_name})
        address = f"{address}, {district_name}"
    if city_id > 0:
        sql = f"SELECT nz_region.id AS region_id, nz_region.name AS region_name, b.id AS city_id, b.name AS city_name FROM (SELECT * FROM nz_city WHERE id={city_id}) b JOIN nz_region ON b.region_id=nz_region.id"
        cursor.execute(sql)
        nz_city_result = cursor.fetchone()
        if nz_city_result:
            agencies_parse_address.update({"region_id": nz_city_result[0], "region_name": nz_city_result[1], "city_id": nz_city_result[2], "city_name": nz_city_result[3]})
            address = f"{address}, {nz_city_result[3]}, {nz_city_result[1]}"
    agencies_parse_address.update({"address": address})
    return agencies_parse_address   


def sync_agency_branch_tohomue(conn):
    sql = 'SELECT house_agency.id AS agency_id, colloquial_name, homue_spider_agencies.name, phone, parse_address, agency_homue_logo, email, website_url FROM homue_spider_agencies JOIN house_agency ON homue_spider_agencies.colloquial_name = house_agency.name'
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    sync_agencies = []
    for item in results:
        parse_address = json.loads(item[4])
        address = parse_address.get('address') if parse_address.get('address') != '' else None
        region_id = parse_address.get('region_id')
        region_name = parse_address.get('region_name')
        city_id = parse_address.get('city_id')
        city_name = parse_address.get('city_name')
        district_id = parse_address.get('district_id')
        sync_agencies.append((item[0], item[1], item[2], address, region_id, region_name, city_id, city_name, district_id, item[3],item[5], item[6], item[7]))

    sync_agencies_sql = "INSERT IGNORE INTO house_agency_branch(agency_id, agency_name, branch_name, address, region_id, region, city_id, city, district_id, phone, agency_logo, email, website_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sync_agencies_sql, sync_agencies)
    conn.commit()
    cursor.close()         

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
    sync_agency_branch_tohomue(conn=conn)
    conn.close()