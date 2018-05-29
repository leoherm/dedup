# -*- coding: utf-8 -*-
"""
*****  find_all_photos.py   *********

RUN THIS FIRST -scan directories for images, retreive EXIF and other tags,
and store in a database

Then run find_dup_images.py, then move_images.py

Created on Mon Jan 26 21:12:14 2018
Modified May 29 2018

@author: Leo
"""
import os
import exifread as exif
import hashlib  # Use crypto hash function consistent across runs
import sqlite3

"""  Set up pointers to control the file directory scans  """ 
cur_dir = 'G:\\My Drive\\Coding\\image_dedup\\test_files\\'
#cur_dir = 'C:\\Users\\Leo\\Pictures\\'
#cur_dir = 'C:\\Users\\Leo\\Pictures\\2006 photos\\'
#root_path = 'C:\\Users\\Leo\\Pictures\\'
root_path = 'G:\\My Drive\\Coding\\image_dedup\\test_files\\'
#root_path = 'C:\\Users\\Leo\\Google Drive\\Coding\\image_dedup\\'
"""  Set up image databae  """
conn = sqlite3.connect(root_path + 'imagesDB.sqlite')
cur = conn.cursor()
cur.executescript('''
    DROP TABLE IF EXISTS all_images;
    CREATE TABLE all_images (
            image_id INTEGER PRIMARY KEY UNIQUE,
            path TEXT,
            file_name TEXT,
            file_name_lower TEXT,
            ref_folder BOOLEAN,
            model TEXT,
            img_type TEXT,
            exif_id TEXT,
            exif_hash TEXT,
            size INTEGER,
            created FLOAT, 
            status TEXT,
            hash_state TEXT,
            parent_id INTEGER,
            new_path TEXT,
            new_name TEXT
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
        exif_tags = {}
        img_type = file[-3:].lower()
        if img_type in imagetypes:
            image_count +=1
            filefullpath = os.path.join(dirpath, file)
            f= open(filefullpath, 'rb')
            hash_value= hashlib.md5()   # Reset hash value so no path dependency
            try:
                tags = exif.process_file(f)
                long_tag = str()
                # Get wanted tags
                for tag_name in tags_wanted:
                    if tag_name in tags:
                        exif_tags[tag_name] = tags[tag_name]
                    else:
                        exif_tags[tag_name] = 'tag missing'
                # Now build string of tag values to hash
                for tag in tags:
                    if tag != 'JPEGThumbnail':
                        long_tag = long_tag + str(tags[tag])
                hash_value.update(str.encode(long_tag))  # Hash function needs bytes input
                hash_str = hash_value.hexdigest()    # hexdigest returns a string
            except:
                print('EXIF failed')
                unique_id = ''
                hash_str = ''
                for tag_value in tags_wanted:  # put blanks for tag values
                    exif_tags[tag_value] = ''
            f.close()
            model = str(exif_tags['Image Model'])
            unique_id = str(exif_tags['EXIF ImageUniqueID'])
            c_time = os.stat(filefullpath).st_ctime  # Get time file created
            file_size = os.stat(filefullpath).st_size  # Gt file size
            if dirpath[-15:] == "non_ref_folders":
                ref = False
            else:
                ref = True
#  Write to DB            
            cur.execute('''INSERT INTO all_images (path, file_name, file_name_lower, 
                        ref_folder, model, img_type, exif_id, exif_hash, size, created) 
                        VALUES (?,?,?,?,?,?,?,?,?,?)''',
                        (dirpath, file, file.lower(), ref, model, img_type, unique_id, hash_str, 
                         file_size, c_time))
            
            print(file, image_count)
    conn.commit()
#            for tag in exif_tags:
 #               print(tag, ':', exif_tags[tag])
  #          print('Hash: ', hash_str)
   #         print('-------------')

print('---------------------------------')
print('Folders scanned: ', folder_count)
print('images found: ', image_count)

conn.commit()
cur.close()