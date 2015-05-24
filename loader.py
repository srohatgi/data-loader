__author__ = 'sumeetrohatgi'

import db_wrapper
import file_wrapper
import api_wrapper
import logging


def usage(message=None):
    code = 0
    if message:
        print message
        code = 1
    print "usage: loader.py [-h] [-f] -d|--datafile <datafile-path> " \
          "-t|--table <table_name> -c|--dbconn <db connection>"
    sys.exit(code)


def check_arg(var, message):
    if var is None:
        usage(message)


if __name__ == "__main__":
    import getopt
    import sys

    opts = None
    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                   "hft:d:c:",
                                   ["help", "force", "table=", "datafile=", "dbconn="])
    except getopt.GetoptError as err:
        usage(err)

    filename = None
    table_name = None
    force = False
    dbconn = None
    apiconn = None

    for option, argument in opts:
        if option in ('-f', '--force'):
            force = True
        elif option in ('-d', '--datafile'):
            filename = argument
        elif option in ('-t', '--table'):
            table_name = argument
        elif option in ('-c', '--dbconn'):
            dbconn = argument
        elif option in ('-a', '--apiconn'):
            apiconn = argument
        elif option == 'h':
            usage()
        else:
            assert False, "unhandled option"

    check_arg(filename, "please specify a file")
    check_arg(table_name, "please specify table name")
    check_arg(dbconn, "please specify database connection string")

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='run.log',
                        filemode='a',
                        level=logging.DEBUG)

    logging.info("*"*20)
    logging.info("starting to load filename: %s into %s", filename, table_name)
    logging.info("force flag = %s", force)

    try:
        with db_wrapper.DBWrapper(table_name, dbconn=dbconn, force=force) as loader:
            # print "loader = ", loader
            file_wrapper.parse_file(filename, loader.build_ddl, loader.insert)
    except:
        logging.exception("unable to process file %s correctly", filename)
        sys.exit(2)
    finally:
        logging.info("finished loading filename: %s into %s", filename, table_name)
        logging.info("*"*20)
