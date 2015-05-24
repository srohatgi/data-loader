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
        payload = {
            'email_id': row['email_id'],
            'brand_id': row['brand_id'],
            'last_transaction_date': row['last_transaction_date'].strftime('%Y/%m/%d'),
            'first_name': row['first_name'],
            'last_name': row['last_name'],
            'contacts': [
                {
                    'first_name': row['contact_first_name'],
                    'last_name': row['contact_last_name']
                }
            ],
            'occasion_date': row['occasion_date'].strftime('%Y/%m/%d'),
            'gift_message': row['gift_message'],
            'occasion_type': 'MOTHERS_DAY|ANNIVERSARY|HALLOWEEN|CHRISTMAS|BIRTHDAY'
        }

        for ot in ['mothersday', 'bday', 'xmas', 'anniversary', 'halloween']:
            if row[ot] == '1':
                payload['occasion_type'] = ot
                break

        data = json.dumps(payload)
        # r = requests.post(self.uri, data=data, headers=self.auth)
        # logging.debug(r)

payload1 = {
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

response1 = {
    'reminder_id': 1000,
    'user_id': 2000,
    'contact_id': 3000
}