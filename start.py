import pymysql
import os
from scrapy.utils.project import get_project_settings
# import logging

def start_sync_local_images():
    project_settings = get_project_settings()
    conn = pymysql.connect(
                host=project_settings['DB_HOST'], 
                user=project_settings['DB_USER'], 
                password=project_settings['DB_PASSWORD'], 
                database=project_settings['DB_DATABASE'], 
                port=project_settings['DB_PORT']
            )
    
    sql = 'SELECT id, agency_websit_logo FROM homue_spider_agencies WHERE agency_websit_logo IS NOT NULL'
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    
    image_path = project_settings['IMAGES_STORE']
    for row in results:
        data_id = row[0]
        web_log_url = row[1]
        image_name = web_log_url.split('/')[-1]
        if os.path.exists(f'{image_path}/{image_name}') :
            sql = "UPDATE homue_spider_agencies SET agency_homue_logo=%s WHERE id=%s"
            cursor.execute(sql, (image_name, data_id))
            conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    start_sync_local_images()