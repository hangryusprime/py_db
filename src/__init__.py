import os
import sys
import io
import pandas as pd
import datetime
import pymysql
import cx_Oracle
from configparser import RawConfigParser as cp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


sys.argv.extend(['temp1', 'temp2'])  # 'temp' is a place holder item in sys.argv to bypass list index exception

# Initializing db constants
db_env = None

# Reading environment variables from config.ini
env = cp()
env.read('config.ini')
if not (all(x in env.sections() for x in ['dev', 'test', 'impl'])):
    raise Exception('Please set environment variables in config.ini')

# Validation of CLI arguments
if sys.argv[1] not in env.sections():
    raise Exception('Invalid env or env not listed in config.ini')
else:
    db_env = sys.argv[1]

in_data_path = os.path.join(os.getcwd(), 'data_in')
out_data_path = os.path.join(os.getcwd(), 'data_out')


msql_cred = {
    'user': env.get('DEFAULT', 'msql_user')
    , 'password': env.get(db_env, 'msql_pass')
    , 'host': env.get(db_env, 'msql_host')
    , 'database': env.get('DEFAULT', 'msql_db')
}

orcl_cred = {
    'user': env.get('DEFAULT', 'orcl_user')
    , 'password': env.get(db_env, 'orcl_pass')
    , 'host': env.get(db_env, 'orcl_host')
}

# Setting up MySQL engine using SQLAlchemy
msql_engine = create_engine(f"mysql+pymysql://{msql_cred['user']}:{msql_cred['password']}@{msql_cred['host']}:3306/{msql_cred['database']}", pool_recycle=3600,
                       echo=False)  # echo=True for sqlalchemy console logging
msql_session = sessionmaker(bind=msql_engine)

# Setting up Oracle engine using SQLAlchemy
orcl_engine = create_engine(f"oracle+cx_oracle://{orcl_cred['user']}:{orcl_cred['password']}@{orcl_cred['host']}")
orcl_session = sessionmaker(bind=orcl_engine)


def clear_directory(directory):
    try:
        file_list = os.listdir(os.path.join(os.getcwd(), directory))
        for file in file_list:
            os.remove(os.path.join(os.getcwd(), directory, file))
    except FileNotFoundError:
        os.mkdir(directory)


class LineFeeder:

    def __init__(self, file_in_path, file_out_path):
        self.file_in_path = file_in_path
        self.file_out_path = file_out_path
        if self.file_in_path is not None:
            self.file_in = io.open(self.file_in_path, 'r')
        else:
            self.file_in = None
        if self.file_out_path is not None:
            self.file_out = io.open(self.file_out_path, 'w')
        else:
            self.file_out = None

    def readline(self):
        if self.file_in is not None:
            line = self.file_in.readline()
            if line == '':
                return None
            else:
                return line
        else:
            print('Input file not specified')

    def writeline(self, line):
        if self.file_out is not None:
            self.file_out.write(line)
        else:
            print('output file not specified')

    def close(self):
        if self.file_in is not None:
            self.file_out.close()
        if self.file_out is not None:
            self.file_in.close()


def read_data(db_tables,engine=msql_engine):
    for table in db_tables:
        db_session = DBSession()
        count = engine.execute(f'select count(*) from {table}').scalar()
        offset = 0
        chunk_size = 100000
        time_exec = time.time()
        print(f'Reading: {table} (chunk_size: {chunk_size})')
        while offset <= count:
            loop_time = time.time()
            df = pd.read_sql(f'select * from {table} limit {offset}, {chunk_size}', con=engine)
            if offset == 0:
                df.to_csv(path_or_buf=f'{out_data_path}{table}.csv', na_rep='NaN', index=False, mode='w')
            else:
                df.to_csv(path_or_buf=f'{out_data_path}{table}.csv', na_rep='NaN', index=False, mode='a', header=False)
            loop_time = time.time() - loop_time
            print(f'Read: {offset + chunk_size} records in {round(loop_time, 3)} sec')
            offset += chunk_size
        time_exec = time.time() - time_exec
        print(f'Finished Reading: {table} in {round(time_exec, 3)} sec')
        db_session.close()
