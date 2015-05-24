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