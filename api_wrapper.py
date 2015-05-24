__author__ = 'sumeetrohatgi'

import requests
import json
import logging
import sys


payload = {
    'email_id': '',
    'brand_id': 4,  # for CHERYL: 4, FM: 5, TPF: 6
    'last_transaction_date': '',
    'first_name': '',
    'last_name': '',
    'contacts': [
        {
            'first_name': '',
            'last_name': ''
        }
    ],
    'occasion_date': '',
    'gift_message': '',
    'occasion_type': 'MOTHERS_DAY|ANNIVERSARY|HALLOWEEN|CHRISTMAS|BIRTHDAY'
}

response = {
    'reminder_id': 1000,
    'user_id': 2000,
    'contact_id': 3000
}


def usage(message=None):
    code = 0
    if message:
        print message
        code = 1
    print("usage: load_reminders --server <uri> --auth <authcode> --table <table>")
    sys.exit(code)


def check_arg(var, message):
    if not var:
        usage(message)


if __name__ == "__main__":
    import getopt

    opts = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:a:t:", ["help", "server=", "auth=", "table="])
    except getopt.GetoptError:
        print sys.exc_info()
        usage("error getting options")

    server_name = None
    auth_code = None
    table_name = None
    for option, argument in opts:
        if option in ["-h", "--help"]:
            usage()
        elif option in ["-s", "--server"]:
            server_name = argument
        elif option in ["-a", "--auth"]:
            auth_code = argument
        elif option in ["-t", "--table"]:
            table_name = argument
        else:
            assert False, "unhandled option"

    check_arg(server_name, "please provide server uri")
    check_arg(auth_code, "please provide auth code")
    check_arg(table_name, "please provide table name")

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='run.log',
                        filemode='a',
                        level=logging.DEBUG)

    logging.info("*"*20)
    logging.info("starting to load table: %s into %s", table_name, server_name)
    logging.info("*"*20)
