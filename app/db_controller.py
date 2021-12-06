import os
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_, or_
from contextlib import contextmanager
import random
from datetime import timedelta
import datetime
from sqlalchemy import text
from db_models import Base, CollibraStatus, CollibraImportProcess, CollibraDBRecord

Base = declarative_base()


class DBController:
    __db_url = os.getenv('DB_URL', "localhost")
    __db_port = os.getenv('DB_PORT', "5432")
    __db_user = os.getenv('DB_USER', "postgres")
    __db_name = os.getenv('DB_NAME', "collibra_importer")
    __db_password = os.getenv('DB_PASSWORD', "mysecretpassword")
    __db_auth_mode = os.getenv('DB_AUTH_MODE', "password")  # password, cert
    __instance = None

    __conn = None
    __cur = None

    @staticmethod
    def getInstance():
        if DBController.__instance is None:
            DBController()
        return DBController.__instance

    def __init__(self):
        if DBController.__instance is not None:
            pass
        else:
            ssl_cert = os.getenv('POSTGRES_CERT')
            ssl_key = os.getenv('POSTGRES_KEY')
            ssl_root_cert = os.getenv('CA_BUNDLE_PEM')

            if self.__db_auth_mode == "cert":
                ssl_args = {
                "sslmode": "require",
                "sslcert": ssl_cert,
                "sslkey": ssl_key,
                "sslrootcert": ssl_root_cert}
                db_connect_string = 'postgres+psycopg2://{0}@{1}:{2}/{3}'.format(self.__db_user, self.__db_url,
                                                                             self.__db_port, self.__db_name)
            else:
                ssl_args = {}
                db_connect_string = 'postgres+psycopg2://{0}:{4}@{1}:{2}/{3}'.format(self.__db_user, self.__db_url,
                                                                                 self.__db_port, self.__db_name, self.__db_password)

            DBController.__instance = self

            try:
                self.engine = create_engine(db_connect_string, connect_args=ssl_args)
                self.Session = sessionmaker(bind=self.engine)
                self.query_session = self.Session()
                self.create_database()
            except Exception as e:
                print("*-*-*-* DB problem *-*-*-*: " + str(e))
                pass

    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    ####################################################  Manage Sessions  ####################################################
    def get_query_session(self):
        self.close_query_session()
        if self.query_session is not None:
            return self.query_session
        else:
            self.query_session = self.Session()
            return self.query_session

    def close_query_session(self):
        if self.query_session is not None:
            self.query_session.close()
            self.query_session = None
        return True

    ####################################################  Manage Databases  ####################################################
    def recreate_database(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def create_database(self):
        Base.metadata.create_all(self.engine)

    def drop_database(self):
        Base.metadata.drop_all(self.engine)

    ####################################################  Manage Queries  ####################################################
    def truncate_table(self, table):
        with self.session_scope() as s:
            s.execute('''TRUNCATE TABLE {}'''.format(table))
        return True

    def get_all(self, table, close_session=True):
        q1 = self.get_query_session().query(table).all()
        if close_session:
            self.close_query_session()
        return q1

    def get_count(self, table):
        class_table = eval(table)
        with self.session_scope() as s:
            d_count = s.query(class_table).count()
        return d_count  

    def get_element_by_pk(self, table, element_pk, close_session=True):
        class_table = eval(table)
        record = self.get_query_session().query(class_table).filter(class_table.id == element_pk).first()
        
        if close_session:
            self.close_query_session()
        return record

    def get_random_element(self, table):
        num_records = self.get_count(table)
        select_pk = random.randint(1,num_records)

        record = self.get_element_by_pk(table, select_pk)
        while record is None:
            select_pk = random.randint(1,num_records)
            record = self.get_element_by_pk(table, select_pk)
        return record

    def check_element_exists(self, table, field, value):
        class_table = eval(table)
        class_field = eval(table+'.'+field)
        
        result = False
        q1 = self.get_query_session().query(class_table).filter(class_field == value).first()
        if q1 is not None:
            result = True
        return result

    def manual_sql_select(self, query):
        output = []
        try:
            sql = text(query)
            result = self.engine.execute(sql)
            for row in result:
                output.append(row)
        except Exception as e:
                print("Error executing the sql: " + query)
                print(str(e))
        return output

    def manual_sql(self, query):
        # result = self.engine.execute(sql)

        with self.session_scope() as s:
            s.execute(query)
        return True

    ####################################################  Manage tables  ####################################################

    def add_record(self, obj):
        with self.session_scope() as s:
            s.add(obj)
        return True

    def add_records(self, records):
        with self.session_scope() as s:
            s.add_all(records)
        return True

    def delete_record(self, obj):
        self.query_session.close()
        with self.session_scope() as s:
            s.delete(obj)
        self.query_session = self.Session()
        return True

    def find_collibra_status_record(self, stratio_qr_dimension, metadatapath):        
        result = False
        q1 = self.query_session.query(CollibraStatus).filter_by(stratio_qr_dimension=stratio_qr_dimension).filter_by(metadatapath=metadatapath).first()
        return q1

    def exists_collibra_db_record(self, import_process_id, stratio_qr_dimension, metadatapath):        
        result = False
        q1 = self.query_session.query(CollibraDBRecord).filter_by(import_process_id=import_process_id).filter_by(stratio_qr_dimension=stratio_qr_dimension).filter_by(metadatapath=metadatapath).first()
        if q1 is not None:
            result = True
        return result
        

