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


class Backup(object):
    EXCLUDED = [".apmaster", ".apversion", ".DS_Store", ".lrprev", ".lrdata", ".AppleDouble", "/snapshots/"]
    MIN_FREE_SPACE = 1024 * 1024 * 100

    def __init__(self, folder, path_prefix):
        self.folder = folder
        self.path_prefix = path_prefix

        self.conn = sqlite3.connect('backup_disks.db')
        self.c = self.conn.cursor()
        sql = "create table if not exists files (filename text, hash_sha512 char(128), disk_id int, added timestamp, size long)"
        self.c.execute(sql)

        self.conn_todo = sqlite3.connect("backup_todo.db")
        self.c_todo = self.conn_todo.cursor()

        self.selected_disk = None

    def get_next_free_disk(self, main_folder, required_size):
        for disk in glob.glob(main_folder+"/*"):
            disk_info = os.statvfs(disk)
            free_space = disk_info.f_frsize * disk_info.f_bfree
            if free_space - self.MIN_FREE_SPACE > required_size:
                if os.path.exists(disk+"/ID.txt"):
                    return disk
                else:
                    print "!!! Disk with no ID.txt: %s" % disk
        return None


    def process_file(self, filename_external, filename_internal):
        print "processing %s" % filename_external
        if not os.path.isfile(filename_external):
            return

        invalid = False
        for exc in self.EXCLUDED:
            if exc in filename_external:
                print "Invalid filename. Skipping"
                return

        try:
            self.c.execute("select * from files where filename=?", (filename_internal, ))
        except:
            return

        results = self.c.fetchone()
        if results is None:
            contents = open(filename_external)
            hash = hashlib.sha512()
            size = 0
            for data in contents:
                size += len(data)
                hash.update(data)
            digest = hash.hexdigest()
            if self.selected_disk is None:
                self.selected_disk = self.get_next_free_disk("/mnt/backup_disks", size)
            disk_info = os.statvfs(self.selected_disk)
            free_space = disk_info.f_frsize * disk_info.f_bfree
            if free_space - self.MIN_FREE_SPACE < size:
                print "Out of disk space on %s" % self.selected_disk
                self.selected_disk = self.get_next_free_disk("/mnt/backup_disks", size)
      
            if self.selected_disk is None:
                print "Out of disk space on all disks."
                sys.exit(2)

            disk_id = int(os.path.basename(self.selected_disk))

            dirname_internal = os.path.dirname(filename_internal)
            absolute_dirname_internal = os.path.join(self.selected_disk, dirname_internal)
            absolute_filename_internal = os.path.join(self.selected_disk, filename_internal)

            if not os.path.exists(absolute_dirname_internal) and len(dirname_internal) > 0:
                os.makedirs(absolute_dirname_internal)
            print "Copying %s to %s" % (filename_external, absolute_filename_internal)
            shutil.copyfile(filename_external, absolute_filename_internal)
            self.c.execute("insert into files VALUES (?, ?, ?, ?, ?)", (filename_internal, digest, disk_id, datetime.datetime.now(), size))
            self.conn.commit()
        else:
            print "Already backed up: %s" % (results, )

    def process_folder(self):
        for root, dirnames, filenames in os.walk(self.folder):
            print root, dirnames, filenames
            for filename in filenames:
                filename_internal = os.path.join(self.path_prefix, os.path.join(root, filename).replace(self.folder, "", 1)[1:])
                filename_external = os.path.join(root, filename)
                self.process_file(filename_external, filename_internal)

    def process_todo(self):
        self.c_todo.execute("select filename from files where filename like ?", (folder + "%",))
        tbd = []
        while True:
            r = self.c_todo.fetchone()
            if r is None:
                break
            (f, ) = r
            filename_internal = os.path.join(path_prefix, f.replace(folder, "", 1)[1:])
            filename_external = f
            print filename_external, filename_internal

            self.process_file(filename_external, filename_internal)
            tbd.append(f)

        for file in tbd:
            self.c_todo.execute("delete from files where filename=?", (file,))

        self.conn_todo.commit()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: %s internal/prefix /path/to/backups" % sys.argv[0]
        sys.exit(1)
    path_prefix = sys.argv[1]
    folder = sys.argv[2]

    b = Backup(folder, path_prefix)
    b.process_todo()

#conn2.close()
#sys.exit(0)
#process_folder(folder, path_prefix)
#conn.close()
