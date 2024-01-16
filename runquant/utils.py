import glob
import os
import gzip
import shutil
import tqdm
import sqlite3

def unpack_fitfile_archive(archive_dir: str, export_dir: str):
    ''' Unzip all the gzip files in a directory.

    Intended to unpack archive data directory from Strava export.
    '''
    fns = glob.glob(os.path.join(archive_dir,'*.gz'))
    
    for fn in tqdm.tqdm(fns):
        unpacked_fn = os.path.join(export_dir,os.path.basename(fn).replace('.gz',''))
        if not os.path.exists(unpacked_fn):
            with gzip.open(fn,'rb') as f:
                with open(unpacked_fn,'wb') as f_out:
                    shutil.copyfileobj(f,f_out)

class ActivityDB:
    def __init__(self, db_path):
        self.db = db_path
        create_flag = False
        if not os.path.exists(self.db):
            self.create_tables()
        self.connection = sqlite3.connect(self.db)
        if create_flag == True:
            self.create_tables()

    def create_activities_table(self, conn):
        cursor = conn.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS activity_metadata
                       ([activity_id] INTEGER PRIMARY KEY, [athlete_id] INTEGER, [provider_id] INTEGER, [provider] TEXT, [start_time] INTEGER)
'''
        )
        conn.commit()
        return conn

    def create_tables(self):
        conn = self.connect()
        conn = self.create_activities_table(conn)
        conn.close()

    def connect(self):
        return sqlite3.connect(self.db)
    
    def insert_from_dict(self,tbl, data):
        conn = self.connect()
        cursor = conn.cursor()
        exec_str = f'INSERT INTO {tbl} ({", ".join(data.keys())}) VALUES({":"+", :".join(data.keys())})'
        cursor.execute(exec_str,data)
        conn.commit()
        conn.close()

    def check_exists(self,id,tbl='activity_metadata'):
        conn = self.connect()
        cursor = conn.cursor()
        result = cursor.execute(f'SELECT rowid FROM {tbl} WHERE provider_id = {id}').fetchall()
        ret = False
        if len(result)>0:
            ret = True
        return ret