__author__ = 'sumeetrohatgi'

import requests
import json
import logging
import re


class ReminderAPI:

    def __init__(self, apiconn):
        logging.debug("apiconn: %s", apiconn)
        p = re.compile('(\w+)@(.*)')
        try:
            auth, self.uri = p.match(apiconn).groups()
            self.auth = {'X-ST-Auth': auth}
        except:
            raise Exception('unable to parse apiconn: {}'.format(apiconn))

    def make_call(self, row):
        data = json.dumps(row)
        r = requests.post(self.uri, data=data, headers=self.auth)
        logging.debug(r)

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