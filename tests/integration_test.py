import sqlite3
import os, sys
import random
import string
import subprocess
import time
import shutil

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
        self.cursor.executemany('INSERT INTO random_data (name, value) VALUES (?, ?)', records)
        self.conn.commit()

        return num_records

    def insert(self):
        num_records = 0
        sleep_num = 0
        while num_records < self.max_records:
            num = self.insert_random_data(num_records)
            num_records += num
            sleep_num += num
            if sleep_num > 500:
                print("after insert ", sleep_num, " data, total: ", num_records, ", go to sleep(1)")
                time.sleep(1)
                sleep_num = 0

        print("finish insert test data, total: ", num_records)

    def query_data(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM random_data order by value')
        return cursor.fetchall()

class ConfigGenerator:
    def __init__(self):
        self.cwd = os.getcwd()
        self.root = self.cwd + "/.test"
        # clean test dir
        try:
            shutil.rmtree(self.root)
            pass
        except:
            pass
        print("root: ", self.root)
        try:
            os.makedirs(self.root)
        except:
            pass

        self.config_file = self.root + "/replited.toml"

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
        config_file = self.config_file
        file = open(config_file, 'w+')
        file.write(content)
        file.close()

class S3ConfigGenerator(ConfigGenerator):
    def __init__(self):
        ConfigGenerator.__init__(self)
        self.type = 'S3'

    def do_generate(self):
        # create root dir of fs
        try:
            os.makedirs(self.root + "/replited")
        except:
            pass

        # generate config file
        file = open(self.cwd + '/tests/config/s3_template.toml')
        content = file.read()
        content = content.replace('{root}', self.root)
        config_file = self.config_file
        file = open(config_file, 'w+')
        file.write(content)
        file.close()

def start_replicate(p, config_file):
    cmd = p + " --config " + config_file + " replicate &"
    print("replicate cmd: ", cmd)
    #cmds = [p, "--config", config_file, "replicate", "&"]
    #pipe = subprocess.Popen(cmds, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #ret = os.popen(cmds)
    os.system(cmd)
    #print("after replicate")
    #print("after replicate: ", pipe.stdout.read())

def stop_replicate():
    cmd = "killall replited"
    os.system(cmd)
    #subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def test_restore(p, config_file, root, exp_data):
    db = root + "/test.db"
    output = os.getcwd() + "/test.db"
    try:
        os.remove(output)
    except:
        pass
    cmd = p + " --config " + config_file + " restore --db " + db + " --output " + output
    #cmds = [p, "--config", config_file,"restore", "--db", db, "--output", output]
    print("restore: ", cmd)
    #pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #pipe = subprocess.Popen(cmds, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    os.system(cmd)
    #print("after restore")

    conn = sqlite3.connect(output)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM random_data order by value')
    data = cursor.fetchall()
    print("data len: ", len(data), ", exp_data len: ", len(exp_data))
    assert data == exp_data

def decide_config_generator(config_type):
    if config_type == "fs":
        return FsConfigGenerator()
    elif config_type == "s3":
        return S3ConfigGenerator()
    else:
        print("invalid config type: ", config_type)
        sys.exit(-1)

# python3 tests/integration_test.py [number of data] [config type] [replited bin path]
if __name__ == '__main__':
    print("args: ", sys.argv)
    number = int(sys.argv[1])
    if number <= 0:
        print("invalid number: ", number)
        sys.exit(-1)
    config_type = sys.argv[2]
    bin_path = sys.argv[3]

    stop_replicate()

    config = decide_config_generator(config_type)
    config.generate()

    test = Test(config.root, number)
    test.create_table()

    start_replicate(bin_path, config.config_file)

    test.insert()

    time.sleep(3)
    data = test.query_data()

    stop_replicate()

    test_restore(bin_path, config.config_file, config.root, data)

