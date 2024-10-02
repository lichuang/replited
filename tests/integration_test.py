import sqlite3
import os, sys
import random
import string
from tempfile import TemporaryDirectory
import subprocess
import time

class Test:
    def __init__(self, root, max_records):
        self.conn = sqlite3.connect(root + '/test.db')
        self.max_records = max_records
        self.cursor = self.conn.cursor()
        
    def create_table(self):
        self.conn.execute('''
        CREATE TABLE IF NOT EXISTS random_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        )
        ''') 

    def insert_random_data(self, start):
        num_records = random.randint(1, 20)
        records = []
        for i in range(num_records):
            name = ''.join(random.choices(string.ascii_letters, k=5))
            value = i + start
            records.append((name, value))
        self.conn.executemany('INSERT INTO random_data (name, value) VALUES (?, ?)', records)
        self.conn.commit()

        return num_records

    def insert(self):
        num_records = 0
        while num_records < self.max_records:
            num_records += self.insert_random_data(num_records)

    def query_data(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM random_data order by value')
        return cursor.fetchall()

class ConfigGenerator:
    def __init__(self):
        #self.root = TemporaryDirectory().name
        self.cwd = os.getcwd()
        self.root = self.cwd + "/.test"
        try:
            os.rmdir(self.root)
        except:
            pass
        print("root: ", self.root)
        try:
            os.makedirs(self.root)
        except:
            pass

        self.config_file = ""

    def generate(self):
        print("generate config for backend type ", self.type)
        self.do_generate()

class FsConfigGenerator(ConfigGenerator):
    def __init__(self):
        ConfigGenerator.__init__(self)
        self.type = 'Fs'

    def do_generate(self):
        # create root dir of fs
        try:
            os.makedirs(self.root + "/replited")
        except:
            pass

        # generate config file
        file = open(self.cwd + '/tests/config/fs_template.toml')
        content = file.read()
        content = content.replace('{root}', self.root)
        config_file = self.root + "/fs.toml"
        file = open(config_file, 'w+')
        file.write(content)
        file.close()
        self.config_file = config_file

def start_replicate(p, config_file):
    cmd = p + " --config " + config_file + " replicate"
    subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def stop_replicate():
    cmd = "killall replicate"
    subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def test_restore(p, config_file, root, exp_data):
    db = root + "/test.db"
    output = os.getcwd() + "/test.db"
    try:
        os.remove(output)
    except:
        pass
    cmd = p + " --config " + config_file + " restore --db " + db + " --output " + output
    print("restore: ", cmd)
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("restore result: ", pipe.stdout.read())

    conn = sqlite3.connect(output)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM random_data order by value')
    data = cursor.fetchall()
    print("data: ", len(data))
    print("exp_data: ", len(exp_data))
    assert data == exp_data

if __name__ == '__main__':
    config = FsConfigGenerator()
    config.generate()

    test = Test(config.root, 2)
    test.create_table()

    bin = "/Users/codedump/source/replited/target/debug/replited"
    start_replicate(bin, config.config_file)

    test.insert()

    time.sleep(3)
    print('sleep')
    data = test.query_data()

    stop_replicate()

    test_restore(bin, config.config_file, config.root, data)

