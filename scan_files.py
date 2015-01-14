"""
backup validate --id disk_id --folder /path/to/disk # validates contents of the disk
backup reimage --id disk_id --folder /absolute/path/to/files # re-creates identical image
backup scan --folder /path/to/disk --backupname backup/path/name # scan for non-backed-up files (/modified files)
backup image --folder /absolute/path/to/files --backupname backup/path/name # fill image files with non-backed-up / modified files from --folder
backup tbb # to be burned - lists full images that are not marked as burned
backup create_images # creates UDF images
backup mount_images # mounts all UDF images
backup umount_full_images # umounts full images
backup umount_images # umounts all images
"""

import shutil
import datetime
import sys
import sqlite3
import os
import glob
import hashlib
EXCLUDED = [".DS_Store", ".lrprev", ".lrdata", ".AppleDouble", "/snapshots/"]
conn = sqlite3.connect('backup_todo.db')

c = conn.cursor()
sql = "create table if not exists files (filename text)"
c.execute(sql)

conn2 = sqlite3.connect("backup_disks.db")
c2 = conn2.cursor()

c2.execute("select filename from files")
processed_files = set()

while True:
 d = c2.fetchone()
 if d is None:
   break
 (f,) = d
 processed_files.add(f)


folder = sys.argv[2]

def process_folder(folder, path_prefix):
 for root, dirnames, filenames in os.walk(folder):
  invalid = False
  for exc in EXCLUDED:
   if exc in root:
    invalid = True
    break
  if invalid:
   continue
  for filename in filenames:
    filename_internal = os.path.join(path_prefix, os.path.join(root, filename).replace(folder, "", 1)[1:])
    dirname_internal = os.path.dirname(filename_internal)
    filename_external = os.path.join(root, filename)

    if filename_internal in processed_files:
     print "Already processed: %s" % filename_external
     continue

    print "processing %s (%s)" % (filename_external, filename_internal)
    if not os.path.isfile(filename_external):
      continue

    invalid = False
    for exc in EXCLUDED:
      if exc in filename_external:
        invalid = True
        break
    if invalid:
      print "Invalid filename. Skipping"
      continue


    print "Adding %s" % filename_external
    try:
     c.execute("insert into files VALUES (?)", (filename_external,))
    except:
     print "Failed: %s" % filename_external
  conn.commit()
 conn.commit()

process_folder(folder, sys.argv[1])
conn.close()
conn2.close()
