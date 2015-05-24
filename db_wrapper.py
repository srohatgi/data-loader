__author__ = 'sumeetrohatgi'

import logging
import mysql.connector
from mysql.connector import errorcode
import re


class DBWrapper:
    config = {
        'database': 'ebdb',
        'raise_on_warnings': True,
        'autocommit': True
    }

    col_config = {
        'email_id': {
            'type': 'char',
            'length': 255,
            'index': True
        },
        'brand_id': {
            'type': 'int',
            'length': 4
        },
        'last_transaction_date': {
            'type': 'date',
            'length': None
        },
        'first_name': {
            'type': 'char',
            'length': 56
        },
        'last_name': {
            'type': 'char',
            'length': 56
        },
        'contact_first_name': {
            'type': 'char',
            'length': 56
        },
        'contact_last_name': {
            'type': 'char',
            'length': 56
        },
        'occasion_date': {
            'type': 'date',
            'length': None
        },
        'gift_message': {
            'type': 'varchar',
            'length': 512
        }
    }

    def __init__(self, table_name, dbconn, force=False):
        self.conn = None
        self.cursor = None
        self.columns = []
        self.table_name = str(table_name).lower()
        self.force = force
        p = re.compile('(\w+)@(\w+):(.*)')
        try:
            logging.debug("dbconn: %s", dbconn)
            self.config["user"], self.config["password"], self.config["host"] = p.match(dbconn).groups()
        except:
            raise Exception('unable to parse dbconn string: {}'.format(dbconn))

    def __enter__(self):
        self.open_db_conn()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_db_conn()

    def open_db_conn(self):
        if self.conn is None:
            logging.debug("driver connection params: %s", self.config)
            self.conn = mysql.connector.connect(**self.config)

    def close_db_conn(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()

    def build_ddl(self, items):

        self.columns.append({'row_num': {'type': 'int', 'length': 11, 'auto': True}})
        self.columns.append({'status': {'type': 'char', 'length': 3, 'default': 'NEW', 'index': True}})

        for i in items:
            # print i, self.col_config.has_key(i)
            if i in self.col_config:
                self.columns.append({i: self.col_config[i]})
            else:
                self.columns.append({i: {'type': 'char', 'length': 1}})

        # print("columns: {}".format(self.columns))
        string = "CREATE TABLE `{}` (".format(self.table_name)

        # column definitions
        first = True
        for col in self.columns:
            # print col, self.cols[col]
            name = col.keys()[0]
            if not first:
                string += ","
            else:
                first = False
            string += "`{}` {}".format(name, col[name]['type'])
            if col[name]['length']:
                string += "({})".format(col[name]['length'])
            if 'auto' in col[name]:
                string += " NOT NULL AUTO_INCREMENT"
            else:
                string += " NULL"
            if 'default' in col[name]:
                string += " DEFAULT '{}'".format(col[name]['default'])

        string += ", PRIMARY KEY (`row_num`)"

        # indexes
        for col in self.columns:
            # print col, self.cols[col]
            name = col.keys()[0]
            if 'index' in col[name]:
                string += ", INDEX(`{}`)".format(name)

        string += ") Engine=InnoDB"

        try:
            self.open_db_conn()
            self.cursor = self.conn.cursor()

            logging.info("DDL=%s", string)

            if self.force:
                logging.info("Dropping table %s", self.table_name)
                self.cursor.execute("drop table if exists {}".format(self.table_name))

            self.cursor.execute(string)
        except mysql.connector.Error as db_err:
            if db_err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.warn("TABLE %s already exists.", self.table_name)
                return False
            else:
                logging.exception("creating %s: %s", self.table_name, db_err.msg)
                raise db_err
        else:
            logging.info("TABLE %s created.", self.table_name)
            return True
        finally:
            if self.cursor:
                self.cursor.close()
                self.cursor = None

    def insert(self, row):
        string = "insert into {} ".format(self.table_name)

        string += " ("

        first = True
        for col in self.columns:
            name = col.keys()[0]
            if name == 'row_num' or name == 'status':
                continue
            if not first:
                string += ","
            string += name
            first = False

        string += ") values ("

        first = True
        for i in range(0, len(self.columns) - 2):
            if not first:
                string += ","
            name = self.columns[i + 2].keys()[0]
            column_type = self.columns[i + 2][name]['type']
            value = row[i]
            if column_type == 'int':
                string += "{}".format(value)
            elif column_type == 'date':
                if value == "NULL":
                    string += "NULL"
                else:
                    value = value[:10]
                    if value.find(' ') != -1:
                        value = value[:value.index(' ')]
                    string += "str_to_date('{}','%m/%d/%Y')".format(value)
            else:
                string += "'{}'".format(value)
            first = False

        string += ")"

        try:
            if self.cursor is None:
                self.cursor = self.conn.cursor()
            self.cursor.execute(string)
            # print string
            return True
        except mysql.connector.Error:
            logging.exception("unable to insert: %s", string)
            return False

    def process_rows(self, make_call):
        string = "select * from {} where status = 'NEW'".format(self.table_name)
        update_string = "update {} set status = 'PRC' where row_num = ".format(self.table_name)

        update_conn = None
        update_cursor = None
        try:
            if not self.cursor:
                self.cursor = self.conn.cursor()

            update_conn = mysql.connector.connect(**self.config)
            update_cursor = update_conn.cursor()

            logging.debug("select query: %s", string)
            self.cursor.execute(string)

            for row in self.cursor:
                row_dict = dict(zip(self.cursor.column_names, row))
                make_call(row_dict)
                update_cursor.execute(update_string + str(row_dict['row_num']))
            return True
        except:
            logging.exception("unable to select/ make_call")
            return False
        finally:
            if update_cursor:
                update_cursor.close()
            if update_conn:
                update_conn.close()