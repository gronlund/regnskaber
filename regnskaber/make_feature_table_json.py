import datetime
import json

from itertools import groupby

from .shared import financial_statement_iterator, partition_consolidated, tag_previous_reporting_period
from .make_feature_table import generic_text, make_header, Header, register_method, method_translation

from sqlalchemy import Table, Column, ForeignKey, MetaData
from sqlalchemy import JSON, Integer
import pdb


from .models import Base
from . import Session, engine

current_regnskabs_id = 0


def populate_row(table_description, fs_entries, fs_id,
                 consolidated=False):
    """
    returns a dict with keys based on table_description and values
    read from regnskab_tuples based on the method in table_description
    """
    global current_regnskabs_id
    current_regnskabs_id = fs_id
    # print('current regnskabs id')
    fs_dict = dict([(k, list(v))
                    for k, v in groupby(fs_entries,
                                        lambda k: k.fieldName)])
    session = Session()
    header = make_header(fs_dict, fs_id, consolidated, session)
    result = {'headerId': header.id}
    session.close()
    data = {}
    for column_description in table_description['columns']:
        methodname = column_description['method']['name']
        assert methodname in method_translation.keys()
        dimensions = column_description['dimensions']
        regnskabs_fieldname = column_description['regnskabs_fieldname']
        column_name = regnskabs_fieldname

        if 'when_multiple' in column_description['method'].keys():
            when_multiple = column_description['method']['when_multiple']
            #data[column_name]
            out = method_translation[methodname](
                fs_dict,
                regnskabs_fieldname,
                dimensions=dimensions,
                when_multiple=when_multiple
            )
        else:
            #data[column_name]
            out = method_translation[methodname](
                fs_dict,
                regnskabs_fieldname,
                dimensions=dimensions,
            )
        if out is not None:
            data[column_name] = out
    result['data'] = data
    return result


def populate_table(table_description, table):
    assert(isinstance(table_description, dict))
    assert(isinstance(table, Table))
    print("Populating table %s" % table_description['tablename'])
    cache = []
    cache_sz = 1000
    fs_iterator = financial_statement_iterator(data_transform=tag_previous_reporting_period)
    ERASE = '\r\x1B[K'
    progress_template = "Processing financial statements %s/%s"
    for i, end, fs_id, fs_entries in fs_iterator:
        print(ERASE, end='', flush=True)
        print(progress_template % (i, end), end='', flush=True)
        partition = partition_consolidated(fs_entries)
        fs_entries_cons, fs_entries_solo = partition
        if len(fs_entries_cons):
            row_values = populate_row(table_description, fs_entries_cons,
                                      fs_id, consolidated=True)
            if row_values:
                cache.append(row_values)
        if len(fs_entries_solo):
            row_values = populate_row(table_description, fs_entries_solo,
                                      fs_id, consolidated=False)
            if row_values:
                cache.append(row_values)
        if len(cache) >= cache_sz:
            engine.execute(table.insert(), cache)
            cache = []
    if len(cache):
        engine.execute(table.insert(), cache)
        cache = []
    print(flush=True)
    return


def find_regnskabs_id():
    return current_regnskabs_id


def main(table_descriptions_file):
    Base.metadata.create_all(engine)
    tables = dict()

    with open(table_descriptions_file) as fp:
        table_descriptions = json.load(fp)

    for t in table_descriptions:
        table = create_table(t, drop_table=True)
        populate_table(t, table)
        tables[t['tablename']] = table

    return


def create_table(table_description, drop_table=False):
    assert(isinstance(table_description, dict))

    metadata = MetaData(bind=engine)
    tablename = table_description['tablename']
    columns = [Column('headerId', Integer,
                      ForeignKey(Header.id),
                      primary_key=True),
               Column('data', JSON)]

    print('creating table', tablename)
    t = Table(tablename, metadata, *columns)
    if drop_table:
        t.drop(engine, checkfirst=True)
        t.create(engine, checkfirst=False)
    return t
