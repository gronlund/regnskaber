import argparse
import datetime

from . import (interactive_ensure_config_exists, setup_database_connection,
               parse_date, interactive_configure_connection, read_config)

from . import fetch
from . import make_feature_table as transform
from . import make_feature_table_json


class Commands:
    @staticmethod
    def fetch(from_date, processes, **general_options):
        interactive_ensure_config_exists()
        # setup engine and Session.
        setup_database_connection()
        fetch.fetch_to_db(processes, from_date)

    @staticmethod
    def transform(table_definition_file, **general_options):
        interactive_ensure_config_exists()
        # setup engine and Session.
        setup_database_connection()
        transform.main(table_definition_file)

    @staticmethod
    def transform_json(table_definition_file, **general_options):
        interactive_ensure_config_exists()
        # setup engine and Session.
        setup_database_connection()
        make_feature_table_json.main(table_definition_file)
        
    @staticmethod
    def reconfigure(**general_options):
        interactive_configure_connection()

    @staticmethod
    def showconfig(**general_options):
        config = read_config()['Global']
        for key, value in config.items():
            print(key, value)

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest='command')
subparsers.required = True
parser_fetch = subparsers.add_parser('fetch', help='fetch from erst')
parser_fetch.add_argument('-f', '--from-date',
                          dest='from_date',
                          help='From date on the form: YYYY-mm-dd[THH:MM:SS].',
                          type=parse_date,
                          default=datetime.datetime(2011, 1, 1),
                          required=False)

parser_fetch.add_argument('-p', '--processes',
                          dest='processes',
                          help=('The number of parallel jobs to start.'),
                          type=int,
                          default=1)

parser_transform = subparsers.add_parser('transform',
                                         help=('build useful tables from data '
                                               'fetched from erst.'))
parser_transform.add_argument('table_definition_file', type=str,
                              help=('A file that specifies the table to be '
                                    'created. If the table name already '
                                    'exists, it is first deleted.'))

parser_transform_json = subparsers.add_parser('transform_json',
                                         help=('build useful json table from data '
                                               'fetched from erst.'))
parser_transform_json.add_argument('table_definition_file', type=str,
                                   help=('A file that specifies the table to be created. If the table name already exists, it is first deleted.'))

parser_reconfigure = subparsers.add_parser('reconfigure',
                                           help='Reconfigure database info.')

parser_show = subparsers.add_parser('showconfig',
                                           help='Reconfigure database info.')


if __name__ == "__main__":
    args = vars(parser.parse_args())
    getattr(Commands, args.pop('command'))(**args)
