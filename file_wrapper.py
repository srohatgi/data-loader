__author__ = 'sumeetrohatgi'

import logging
import csv


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


def parse_file(txt_file, header_row_fn, data_row_fn):
    control_chars = ''.join(map(chr, range(0, 32) + range(127, 256)))
    control_chars += "\\'"

    with open(txt_file, 'rb') as tsv_file:
        tsv_file = csv.reader(tsv_file, delimiter='\t')

        row_number = 0
        for row in tsv_file:
            # print row
            if row_number == 0:
                if not header_row_fn(header_row_cleanup(row)):
                    return
            else:
                data_row_fn(data_row_cleanup(row, control_chars))
            row_number += 1
            if row_number % 500 == 0:
                logging.debug("processed %s rows", row_number)

        logging.info("processed %s rows", row_number)
