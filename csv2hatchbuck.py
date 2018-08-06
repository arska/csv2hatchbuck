"""
Import customers from CSV to Hatchbuck CRM
"""

import logging
import argparse
import os
import csv
import codecs
from dotenv import load_dotenv
from HATCHBUCK import Hatchbuck


load_dotenv()
STATS = {}

PARSER = argparse.ArgumentParser(
    description='sync CSV contacts into Hatchbuck.com CRM'
)
PARSER.add_argument(
    '-v',
    '--verbose',
    help='output verbose debug logging',
    action='store_true',
    default=False
)
PARSER.add_argument(
    '-n',
    '--noop',
    help='dont actually post, just log what would have been posted',
    action='store_true',
    default=False
)
PARSER.add_argument('filename', help='csv file', metavar="file")
PARSER.add_argument(
    'tag',
    help='Hatchbuck contact Tag',
    metavar="tag",
    default=None
)
ARGS = PARSER.parse_args()

LOGFORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

if ARGS.verbose:
    logging.basicConfig(level=logging.DEBUG, format=LOGFORMAT)
else:
    logging.basicConfig(level=logging.INFO, format=LOGFORMAT)
    logging.getLogger('requests.packages.urllib3.connectionpool')\
        .setLevel(logging.WARNING)

logging.debug("starting with arguments: %s", ARGS)

HATCHBUCK = Hatchbuck(os.environ.get('HATCHBUCK_APIKEY'), noop=ARGS.noop)

CSVFILE = csv.reader(codecs.open(ARGS.filename, encoding='utf-8'))
CSVFILE.__next__()  # skip header line


def split_name(fullname):
    """
    Heuristics to split "Firstname Lastname"
    """
    parts = fullname.strip().split(' ')
    if len(parts) < 2:
        # oops, no first/lastname
        raise Exception("only 1 word passed as first/lastname")
    elif len(parts) == 2:
        # the trivial case
        result = (parts[0], parts[1])
    else:
        # "jean marc de fleurier" -> "jean marc", "de fleurier"
        if parts[-2].lower() in ['van', 'von', 'de', 'zu', 'da']:
            result = (' '.join(parts[:-2]), ' '.join(parts[-2:]))
        else:
            result = (' '.join(parts[:-1]), ' '.join(parts[-1]))
    return result


for line in CSVFILE:
    logging.debug(line)
    STATS['contacts'] = STATS.get('contacts', 0) + 1
    line[1] = line[1].strip()
    emails = [x.strip() for x in line[1].split(',')]

    if not emails:
        # empty email -> skip
        STATS['noemail'] = STATS.get('noemail', 0) + 1
        logging.warning("no email address found for %s, skipping", line)
        continue

    profile = HATCHBUCK.search_email_multi(emails)

    firstname, lastname = split_name(line[0])

    if profile is None:
        # no contact found with any email addresses, create new contact
        STATS['notfound'] = STATS.get('notfound', 0) + 1

        profile = {}
        profile['firstName'] = firstname
        profile['lastName'] = lastname

        profile['subscribed'] = True
        profile['status'] = {'name': 'Customer'}

        profile['emails'] = []
        for addr in emails:
            profile['emails'].append({'address': addr, 'type': 'Work'})

        # create the HATCHBUCK contact with the profile information
        # then return the created profile including the assigned 'contactId'
        profile = HATCHBUCK.create(profile)
        logging.info("added contact: %s", profile)
    else:
        STATS['found'] = STATS.get('found', 0) + 1

    if profile.get('firstName', '') == '':
        profile = HATCHBUCK.profile_add(profile, 'firstName', None, firstname)

    if profile.get('lastName', '') == '':
        profile = HATCHBUCK.profile_add(profile, 'lastName', None, lastname)

    for addr in emails:
        profile = HATCHBUCK.profile_add(
            profile,
            'emails',
            'address',
            addr,
            {'type': 'Work'}
        )

    if ARGS.tag:
        if not HATCHBUCK.profile_contains(profile, 'tags', 'name', ARGS.tag):
            HATCHBUCK.add_tag(profile['contactId'], ARGS.tag)

logging.info("STATS: %s", STATS)
