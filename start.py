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
    
    sql = 'SELECT id, agency_websit_logo, agency_homue_logo, physical_address FROM homue_spider_agencies WHERE is_live=1'
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
        # print(f'{data_id}: {physical_address}')
        city_name = physical_address.get('city', None)
        district_name = physical_address.get('address3', None)
        detail_address = physical_address.get('address1', None)
        maybe_district_name = detail_address.split(',')[-1] if detail_address is not None else None
        # address = f'{detail_address} - {district_name} - {city_name}'
        # print(f'{data_id}: {address}')
        print(f'{data_id}: {maybe_district_name}')
        agency_address_dict = {"id": data_id}
        if city_name is not None:
            region_sql = f"SELECT * FROM nz_region WHERE name LIKE '%{city_name}%' LIMIT 1"
            cursor.execute(region_sql)
            nz_region_result = cursor.fetchone()
            if nz_region_result is not None:
                agency_address_dict.update({"region": {"id": nz_region_result[0], "name": nz_region_result[1]}})
            else:
                city_sql = f"SELECT * FROM nz_city WHERE name LIKE '%{city_name}%' LIMIT 1"
                cursor.execute(city_sql)
                nz_city_result = cursor.fetchone()
                if nz_city_result is not None:
                    agency_address_dict.update({"city": {"id": nz_city_result[0], "name": nz_city_result[2]}})
                else:
                    district_sql = f"SELECT * FROM nz_district WHERE name LIKE '%{city_name}%' LIMIT 1"
                    cursor.execute(district_sql)
                    nz_district_result = cursor.fetchone()
                    if nz_district_result is not None:
                        agency_address_dict.update({"district": {"id": nz_district_result[0], "name": nz_district_result[2]}})                   
            agencies_address_list.append(agency_address_dict)
        if district_name is not None:
            region_sql = f"SELECT * FROM nz_region WHERE name LIKE '%{district_name}%' LIMIT 1"
            cursor.execute(region_sql)
            nz_region_result = cursor.fetchone()
            if nz_region_result is not None:
                agency_address_dict.update({"region": {"id": nz_region_result[0], "name": nz_region_result[1]}})
            else:
                city_sql = f"SELECT * FROM nz_city WHERE name LIKE '%{district_name}%' LIMIT 1"
                cursor.execute(city_sql)
                nz_city_result = cursor.fetchone()
                if nz_city_result is not None:
                    agency_address_dict.update({"city": {"id": nz_city_result[0], "name": nz_city_result[2]}})
                else:
                    district_sql = f"SELECT * FROM nz_district WHERE name LIKE '%{district_name}%' LIMIT 1"
                    cursor.execute(district_sql)
                    nz_district_result = cursor.fetchone()
                    if nz_district_result is not None:
                        agency_address_dict.update({"district": {"id": nz_district_result[0], "name": nz_district_result[2]}})                    
            agencies_address_list.append(agency_address_dict)            
        
    cursor.close()
    conn.close()
    # print(agencies_address_list)

if __name__ == '__main__':
    start_sync_local_images()