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
        self.columns.append({'reminder_id': {'type': 'int', 'length': 11, 'index': True}})
        self.columns.append({'contact_id': {'type': 'int', 'length': 11, 'index': True}})
        self.columns.append({'owner': {'type': 'int', 'length': 11, 'index': True}})

        for i in items:
            # print i, self.col_config.has_key(i)
            if i in self.col_config:
                self.columns.append({i: self.col_config[i]})
            else:
                self.columns.append({i: {'type': 'char', 'length': 1}})

        # print("columns: {}".format(self.columns))
        sql = ["CREATE TABLE `{name}` (".format(name=self.table_name)]

        # column definitions
        first = True
        for col in self.columns:
            # print col, self.cols[col]
            name = col.keys()[0]

            if not first:
                sql.append(",")
            else:
                first = False

            sql.append("`{col_name}` {col_type}".format(col_name=name, col_type=col[name]['type']))

            if col[name]['length']:
                sql.append("({col_len})".format(col_len=col[name]['length']))

            if 'auto' in col[name]:
                sql.append(" NOT NULL AUTO_INCREMENT")
            else:
                sql.append(" NULL")

            if 'default' in col[name]:
                sql.append(" DEFAULT '{col_default}'".format(col_default=col[name]['default']))

        sql.append(", PRIMARY KEY (`row_num`)")

        # indexes
        for col in self.columns:
            # print col, self.cols[col]
            name = col.keys()[0]
            if 'index' in col[name]:
                sql.append(", INDEX(`{col_index}`)".format(col_index=name))

        sql.append(") Engine=InnoDB")

        try:
            self.open_db_conn()
            self.cursor = self.conn.cursor()

            sql_string = ''.join(sql)

            logging.info("DDL=%s", sql_string)

            if self.force:
                logging.info("Dropping table %s", self.table_name)
                self.cursor.execute("drop table if exists {name}".format(name=self.table_name))

            self.cursor.execute(sql_string)
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
        sql = ["insert into {table_name} (".format(table_name=self.table_name)]
        skip_cols = ['row_num', 'status', 'reminder_id', 'contact_id', 'owner']

        first = True
        for col in self.columns:
            name = col.keys()[0]
            if name in skip_cols:
                continue
            if not first:
                sql.append(",")
            sql.append(name)
            first = False

        sql.append(") values (")

        first = True
        for i in range(0, len(self.columns) - len(skip_cols)):
            if not first:
                sql.append(",")
            else:
                first = False

            name = self.columns[i + len(skip_cols)].keys()[0]
            column_type = self.columns[i + len(skip_cols)][name]['type']
            value = row[i]

            if column_type == 'int':
                sql.append("{value}".format(value=value))
            elif column_type == 'date':
                if value == "NULL":
                    sql.append("NULL")
                else:
                    value = value[:10]
                    if value.find(' ') != -1:
                        value = value[:value.index(' ')]
                    sql.append("str_to_date('{date_value}','%m/%d/%Y')".format(date_value=value))
            else:
                if name == 'email_id':
                    value = value.lower()
                sql.append("'{string_value}'".format(string_value=value))

        sql.append(")")

        sql_string = ''.join(sql)

        try:
            if self.cursor is None:
                self.cursor = self.conn.cursor()
            self.cursor.execute(sql_string)
            # print string
            return True
        except mysql.connector.Error:
            logging.exception("unable to insert: %s", sql_string)
            return False

    def process_rows(self):
        string = "select * from {name} where status = 'NEW' and email_id != ''".format(name=self.table_name)

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

                owner = self.build_owner(email=row_dict['email_id'],
                                          fname=row_dict['first_name'],
                                          lname=row_dict['last_name'],
                                          acct_creation=row_dict['last_transaction_date'],
                                          brand=row_dict['brand_id'],
                                          cursor=update_cursor)

                occasion = map_occasion(row_dict)

                reminder_id = self.build_reminder(owner=owner,
                                                  created=row_dict['last_transaction_date'],
                                                  occ_date=row_dict['occasion_date'],
                                                  occasion=occasion,
                                                  note=row_dict['gift_message'],
                                                  brand_id=row_dict['brand_id'],
                                                  cursor=update_cursor)

                self.build_reminder_frequency(brand_id=row_dict['brand_id'],
                                              reminder_id=reminder_id,
                                              cursor=update_cursor)

                contact_id = self.build_contact(fname=row_dict['contact_first_name'],
                                                lname=row_dict['contact_last_name'],
                                                reminder_id=reminder_id,
                                                cursor=update_cursor)

                update_string = "update {name} set " \
                                "status = 'PRC', " \
                                "reminder_id = {reminder_id}, " \
                                "contact_id = {contact_id}, " \
                                "owner = {owner} " \
                                "where row_num = {row_num}"\
                    .format(name=self.table_name,
                            reminder_id=reminder_id,
                            contact_id=contact_id,
                            owner=owner,
                            row_num=row_dict['row_num'])

                update_cursor.execute(update_string)

                logging.info('%s generated into: owner(%s) reminder(%s), contact(%s)',
                             row_dict['email_id'],
                             owner,
                             reminder_id,
                             contact_id)

            return True
        except:
            logging.exception("unable to select/ make_call")
            return False
        finally:
            if update_cursor:
                update_cursor.close()
            if update_conn:
                update_conn.close()

    def get_cursor(self, cursor=None):
        if not cursor:
            self.open_db_conn()
            self.cursor = self.conn.cursor()
            return self.cursor
        else:
            return cursor

    def build_owner(self, email, fname, lname, acct_creation, brand, cursor=None):
        cursor = self.get_cursor(cursor=cursor)

        sql_string = "SELECT id FROM sruser where email = '{email}'".format(email=email)

        cursor.execute(sql_string)
        row = cursor.fetchone()
        if row:
            return row[0]

        # lets create a new user
        sql_string = "INSERT INTO sruser (account_active, account_creation_date, email, brand) " \
                     "VALUES  (1, '{created}', '{email}', '{brand}')"\
            .format(created=acct_creation.strftime('%Y/%m/%d'),
                    email=email,
                    brand=brand)

        logging.debug("inserting into sruser: %s", sql_string)

        cursor.execute(sql_string)
        owner = cursor.lastrowid

        sql_string = "INSERT INTO sruser_profile (first_name, last_name, version, owner) " \
                     "VALUES  ('{fname}', '{lname}', 0, {owner})"\
            .format(fname=fname,
                    lname=lname,
                    owner=owner)

        logging.debug("inserting into sruser_profile: %s", sql_string)

        cursor.execute(sql_string)
        return owner

    def build_reminder(self, owner, created, occ_date, note, brand_id, occasion, cursor=None):
        cursor = self.get_cursor(cursor)

        sql_string = "SELECT id from reminder where " \
                     "active = 1 and " \
                     "brand_id = {brand} and " \
                     "day_no = {day} and " \
                     "month_no = {month} and " \
                     "year_no = {year} and " \
                     "owner = {owner} and " \
                     "occasion = {occasion} "\
            .format(day=occ_date.strftime('%d'),
                    month=occ_date.strftime('%m'),
                    year=occ_date.strftime('%Y'),
                    brand=brand_id,
                    occasion=occasion,
                    owner=owner)

        logging.debug("selecting from reminder: %s", sql_string)
        cursor.execute(sql_string)

        row = cursor.fetchone()
        if row:
            return row[0]

        sql_string = "INSERT INTO reminder (active, short_description, version, " \
                     "created, updated, day_no, month_no, year_no, special_note, " \
                     "brand_id, occasion, owner) " \
                     "VALUES (1, '', 0, '{on}', '{on}', {day}, {month}, {year}, '{note}', {brand}, {occasion}, {owner})"\
            .format(on=created.strftime('%Y/%m/%d'),
                    day=occ_date.strftime('%d'),
                    month=occ_date.strftime('%m'),
                    year=occ_date.strftime('%Y'),
                    note=note,
                    brand=brand_id,
                    occasion=occasion,
                    owner=owner)

        logging.debug("inserting into reminder: %s", sql_string)

        cursor.execute(sql_string)

        return cursor.lastrowid

    def build_contact(self, fname, lname, reminder_id, cursor=None):
        cursor = self.get_cursor(cursor)

        sql_string = "SELECT id from contact where " \
                     "reminder = {reminder} and " \
                     "first_name = '{fname}' and " \
                     "last_name = '{lname}'"\
            .format(reminder=reminder_id,
                    fname=fname,
                    lname=lname)

        logging.debug("selecting from contact: %s", sql_string)
        cursor.execute(sql_string)

        row = cursor.fetchone()
        if row:
            return row[0]

        sql_string = "INSERT INTO contact (first_name, last_name, version, reminder) " \
                     "VALUES  ('{fname}', '{lname}', 0, {reminder_id})"\
            .format(fname=fname,
                    lname=lname,
                    reminder_id=reminder_id)

        logging.debug("inserting into contact: %s", sql_string)

        cursor.execute(sql_string)

        return cursor.lastrowid

    def build_reminder_frequency(self, brand_id, reminder_id, cursor=None):
        sql_string = "SELECT count(*) from reminder_frequency where reminder = {reminder}".format(reminder=reminder_id)

        logging.debug("selecting from reminder_frequency: %s", sql_string)

        cursor.execute(sql_string)

        row = cursor.fetchone()
        if row:
            if row[0] == 4:
                return
            else:
                sql_string = "DELETE from reminder_frequency where reminder = {reminder}".format(reminder=reminder_id)

                assert reminder_id is not None

                logging.debug("deleting from reminder_frequency: %s", sql_string)
                cursor.execute(sql_string)

        sql_string = "INSERT INTO reminder_frequency (days_before, version, channel, reminder) VALUES " \
                     "(0, 0, {brand}, {reminder}), " \
                     "(1, 0, {brand}, {reminder}), " \
                     "(7, 0, {brand}, {reminder}), " \
                     "(14, 0, {brand}, {reminder})"\
            .format(brand=brand_id, reminder=reminder_id)

        logging.debug("inserting into reminder_frequency: %s", sql_string)
        cursor.execute(sql_string)

        return



def map_occasion(row):
    if row['mothersday'] == '1':
        return 26
    elif row['xmas'] == '1':
        return 29
    elif row['halloween'] == '1':
        return 35
    elif row['anniversary'] == '1':
        return 6
    else:
        return 3  # default is bday