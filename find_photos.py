# -*- coding: utf-8 -*-
"""   find_photos.py 

Find Jpeg, nef, tiff, etc  files, and hash the exif file.  Store to a DB for later
processing.

Created on Fri Jan 12 16:56:24 2018

Modfified Jan 24 to add file size

@author: Leo
"""

import os
import exifread as exif
import sqlite3
import hashlib  # Use crypto hash function consistent across runs

def exifhash(image_file):
    f= open(image_file, 'rb')
    hash_value= hashlib.md5()
    try:
        tags = exif.process_file(f)
        long_tag = str()
        if 'EXIF ImageUniqueID' in tags:
            unique_id =  str(tags['EXIF ImageUniqueID'])
        else:
            unique_id = ''
        for tag in tags:
            if tag != 'JPEGThumbnail':
                long_tag = long_tag + str(tags[tag])
        hash_value.update(str.encode(long_tag))  # Hash function needs bytes input
        hash_str = hash_value.hexdigest()    # hexdigest returns a string
    except:
        unique_id = ''
        hash_str = ''
    f.close()
    return hash_str, retrieved_tags

#cur_dir = 'C:\\Users\\Leo\\Google Drive\\Coding\\image_dedup\\test_files\\'
cur_dir = 'C:\\Users\\Leo\\Pictures\\'
#cur_dir = 'C:\\Users\\Leo\\Pictures\\2006 photos\\'

root_path = 'C:\\Users\\Leo\\Pictures\\'
#root_path = 'C:\\Users\\Leo\\Google Drive\\Coding\\image_dedup\\test_files\\'
#root_path = 'C:\\Users\\Leo\\Google Drive\\Coding\\image_dedup\\'

#conn = sqlite3.connect(root_path + 'imagesDB_test.sqlite')
conn = sqlite3.connect(root_path + 'imagesDB.sqlite')
cur = conn.cursor()
cur.executescript('''
    DROP TABLE IF EXISTS all_images;
    CREATE TABLE all_images (
            path TEXT,
            file_name TEXT,
            img_type TEXT,
            exif_id TEXT,
            exif_hash INTEGER,
            size INTEGER,
            created FLOAT
            );
    ''')

imagetypes = ['jpg', 'tif', 'nef']
tags_wanted = ['EXIF ImageUniqueID', 'Image Model']
image_count = 0
folder_count = 0
for dirpath, dirnames, filenames in os.walk(cur_dir):
    folder_count +=1
    print('Current path: ', dirpath)
    for file in filenames:
        img_type = file[-3:].lower()
        if img_type in imagetypes:
            image_count +=1
            filefullpath = os.path.join(dirpath, file)
            exhash, unique_id = exifhash(filefullpath)
            c_time = os.stat(filefullpath).st_ctime   # Get time created to compare if no EXIF
            size = os.stat(filefullpath).st_size
#            print(file, ', ', exhash)
            cur.execute('''INSERT INTO all_images (path, file_name,
                            img_type, exif_id, exif_hash, size, created) VALUES (?,?,?,?,?,?,?)''',
                            (dirpath, file, img_type, unique_id, exhash, size, c_time))
        print(image_count)
        if image_count % 1000 == 0:
            conn.commit()           # Commit every 1000 records
#        else:
#            print(file, ' no match')
conn.commit()
cur.close()

print('Folders scanned: ', folder_count)
print('images found: ', image_count)
