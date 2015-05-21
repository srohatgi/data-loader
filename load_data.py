__author__ = 'sumeetrohatgi'

import csv
import logging
import mysql.connector
from mysql.connector import errorcode


class Loader:
    config = {
        'database': 'ebdb',
        'raise_on_warnings': True,
        'autocommit': True
    }

    col_config = {
        'email_id': {
            'type': 'char',
            'length': 255
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

    def __init__(self, table_name, user, password, host, force=False):
        self.conn = None
        self.cursor = None
        self.columns = []
        self.table_name = table_name
        self.force = force
        self.config["user"] = user
        self.config["password"] = password
        self.config["host"] = host

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
        string = "CREATE TABLE `{}` (`row_num` int(11) NOT NULL AUTO_INCREMENT".format(self.table_name)

        for i in items:
            # print i, self.col_config.has_key(i)
            if i in self.col_config:
                self.columns.append({i: self.col_config[i]})
            else:
                self.columns.append({i: {'type': 'char', 'length': 1}})

        # print("columns: {}".format(self.columns))

        for col in self.columns:
            # print col, self.cols[col]
            name = col.keys()[0]
            string += ",`{}` {}".format(name, col[name]['type'])
            if col[name]['length'] is None:
                string += " NULL"
            else:
                string += "({}) NULL".format(col[name]['length'])

        string += ", PRIMARY KEY (`row_num`)) Engine=InnoDB"

        try:
            self.open_db_conn()
            self.cursor = self.conn.cursor()

            logging.info("DDL=%s", string)

            if self.force:
                logging.info("Dropping table %s", self.table_name)
                self.cursor.execute("drop table {}".format(self.table_name))

            self.cursor.execute(string)
        except mysql.connector.Error as db_err:
            if db_err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.warn("TABLE %s already exists.", self.table_name)
            else:
                logging.exception("creating %s: %s", self.table_name, db_err.msg)
                raise db_err
        else:
            logging.info("TABLE %s created.", self.table_name)
        finally:
            if self.cursor is not None:
                self.cursor.close()
                self.cursor = None

    def insert(self, row):
        string = "insert into {} ".format(self.table_name)

        string += " ("
        first = True
        for col in self.columns:
            name = col.keys()[0]
            if not first:
                string += ","
            string += name
            first = False
        string += ") values ("

        first = True
        for i in range(0, len(self.columns)):
            if not first:
                string += ","
            column_type = self.columns[i][self.columns[i].keys()[0]]['type']
            if column_type == 'int':
                string += "{}".format(row[i])
            elif column_type == 'date':
                if row[i] == "NULL":
                    string += "NULL"
                else:
                    value = row[i][:10]
                    if value.find(' ') != -1:
                        value = value[:value.index(' ')]
                    string += "str_to_date('{}','%m/%d/%Y')".format(value)
            else:
                string += "'{}'".format(row[i])
            first = False

        string += ")"

        try:
            if self.cursor is None:
                self.cursor = self.conn.cursor()
            self.cursor.execute(string)
            # print string
        except mysql.connector.Error as insert_err:
            logging.exception("unable to insert: %s", string)


def header_row_cleanup(row):
    row[row.index('GiftMessage')] = 'gift_message'
    cleaned_row = []
    for item in row:
        if len(item) > 0:
            cleaned_row.append(item.lower())
    return cleaned_row


def data_row_cleanup(row, control_chars):
    for index, field in enumerate(row):
        row[index] = field.translate(None, control_chars)
    return row


def parse_file(txt_file, db_loader):
    control_chars = ''.join(map(chr, range(0, 32) + range(127, 256)))
    control_chars += "\\'"

    with open(txt_file, 'rb') as tsv_file:
        tsv_file = csv.reader(tsv_file, delimiter='\t')

        row_number = 0
        for row in tsv_file:
            # print row
            if row_number == 0:
                db_loader.build_ddl(header_row_cleanup(row))
            else:
                db_loader.insert(data_row_cleanup(row, control_chars))
            row_number += 1
            if row_number % 500 == 0:
                logging.debug("processed %s rows", row_number)

        logging.info("processed %s rows", row_number)


def usage(message=None):
    if message is not None:
        print message
    print "usage: load_data.py [-h] [-f] -d|--datafile <datafile-path> -t|--table <table_name>"


def check_arg(var, message):
    if var is None:
        usage(message)
        sys.exit(1)

if __name__ == "__main__":
    import getopt
    import sys

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                   "hft:d:u:p:s:",
                                   ["help", "force", "table=", "datafile=", "user=", "password=", "server="])
    except getopt.GetoptError as err:
        usage(err)
        sys.exit(1)

    filename = None
    table_name = None
    force = False
    user = None
    password = None
    server = None

    for option, argument in opts:
        if option in ('-f', '--force'):
            force = True
        elif option in ('-d', '--datafile'):
            filename = argument
        elif option in ('-t', '--table'):
            table_name = argument
        elif option in ('-u', '--user'):
            user = argument
        elif option in ('-p', '--password'):
            password = argument
        elif option in ('-s', '--server'):
            server = argument
        elif option == 'h':
            usage()
            sys.exit(0)
        else:
            assert False, "unhandled option"

    check_arg(filename, "please specify a file")
    check_arg(table_name, "please specify table name")
    check_arg(password, "please specify password")
    check_arg(server, "please specify server")

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='run.log',
                        filemode='a',
                        level=logging.DEBUG)

    logging.info("*"*20)
    logging.info("starting to load filename: %s into %s", filename, table_name)
    logging.info("force flag = %s", force)

    try:
        with Loader(table_name, user=user, password=password, host=server, force=force) as loader:
            # print "loader = ", loader
            parse_file(filename, loader)
    except:
        logging.exception("unable to process file %s correctly", filename)
        sys.exit(2)
    finally:
        logging.info("finished loading filename: %s into %s", filename, table_name)
        logging.info("*"*20)
