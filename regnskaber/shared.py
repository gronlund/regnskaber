import datetime

from contextlib import closing

from .models import FinancialStatement
from . import Session

from sqlalchemy.sql.expression import func
from collections import namedtuple


def get_reporting_period(fs_entries):
    date_format = '%Y-%m-%d'
    start_date = None
    end_date = None
    last_end_date = None
    for entry in fs_entries:
        if entry.fieldName == 'gsd:ReportingPeriodStartDate':
            start_date = entry.fieldValue
        if entry.fieldName == 'gsd:ReportingPeriodEndDate':
            end_date = entry.fieldValue
        if entry.fieldName == 'gsd:PredingReportingPeriodEndDate':
            last_end_date = entry.fieldValue
                
    start_date = datetime.datetime.strptime(start_date[:10], date_format)
    end_date = datetime.datetime.strptime(end_date[:10], date_format)
    if last_end_date is not None:
        last_end_date = datetime.datetime.strptime(last_end_date[:10],
                                                   date_format)
    
    return start_date, end_date, last_end_date


def date_is_in_range(start_date, end_date, query_date):
    if start_date is None and end_date is None:
        return True

    if start_date is None:
        return query_date <= end_date

    if end_date is None:
        return query_date >= start_date

    return query_date >= start_date and query_date <= end_date


def date_is_instant(start_date, end_date):
    return start_date is None and end_date is not None


def filter_reporting_period(fs_entries):
    """
    returns a subset fs_entries where each entry is in the reporting period.

    """
    start_date, end_date, _ = get_reporting_period(fs_entries)
    result = []
    for entry in fs_entries:
        if entry.startDate is None and entry.endDate is None:
            continue
        if date_is_instant(entry.startDate, entry.endDate):
            if date_is_in_range(start_date, end_date, entry.endDate):
                # append to result
                result.append(entry)
            continue

        if (not date_is_in_range(start_date, end_date, entry.startDate) or
                not date_is_in_range(start_date, end_date, entry.endDate)):
            continue
        result.append(entry)

    return result


def arelle_parse_value(d):
    """Decodes an arelle string as a python type (float, int or str)"""
    if not isinstance(d, str):  # already decoded.
        return d
    try:
        return int(d.replace(',', ''))
    except ValueError:
        pass
    try:
        return float(d.replace(",", ""))
    except ValueError:
        pass
    return d


def partition_consolidated(fs_entries):
    fs_tuples_cons = [r for r in fs_entries
                      if r.koncern]
    fs_tuples_solo = [r for r in fs_entries
                      if not r.koncern]

    return fs_tuples_cons, fs_tuples_solo


def get_number_of_rows():
    with closing(Session()) as session:
        total_rows = session.query(FinancialStatement).count()
        return total_rows


def financial_statement_iterator(end_idx=None, length=None, buffer_size=500, data_transform=None):
    """Provide an iterator over financial_statements in order of id

    Keyword arguments:
    end_idx -- One past the last financial_statement_id to iterate over.
    length -- The number of financial statements to iterate.
              Note only one of end_idx and length can be provided.
    buffer_size -- the internal buffer size to use for iterating.  The buffer
                   size is measured in number of financial statements.

    """
    
    if end_idx is not None and length is not None:
        raise ValueError("Cannot accept both end_idx and length.")

    if end_idx is None and length is None:
        try:
            session = Session()
            max_id = session.query(func.max(FinancialStatement.id)).scalar()
            end_idx = max_id + 1
        except (IndexError, ValueError):
            raise LookupError('Could not lookup maximum financial_statement_id'
                              ' in financial_statement table.')
        finally:
            session.close()

    if end_idx is not None:
        assert(isinstance(end_idx, int))

    if length is not None:
        assert(isinstance(length, int))
        end_idx = length

    total_rows = get_number_of_rows()
    if data_transform is None:
        data_transform = filter_reporting_period
    with closing(Session()) as session:
        curr = 1
        while curr < end_idx:
            q = session.query(FinancialStatement).filter(
                FinancialStatement.id >= curr,
                FinancialStatement.id < min(curr + buffer_size, end_idx)
            ).enable_eagerloads(True).all()
            for i, fs in enumerate(q):
                entries = data_transform(fs.financial_statement_entries)
                yield i+curr, total_rows, fs.id, entries
            curr += buffer_size
    return


def tag_previous_reporting_period(fs_entries):
    """
    returns a subset fs_entries where each entry is in the reporting period.

    """
    prev_tag = '_prev'
    tuple_entry = namedtuple('tuple_entry', ['id', 'financial_statement_id', 'fieldName', 'fieldValue', 'decimals', 'cvrnummer', 'startDate', 'endDate', 'dimensions', 'unitIdXbrl', 'koncern'])

    def make_prev_tuple(my_entry):
        dd = my_entry.__dict__
        dt = {x: dd[x] for x in tuple_entry._fields}
        dt['fieldName'] = dt['fieldName'] + prev_tag
        return tuple_entry(**dt)
            
    data_dict = {}
    for elm in fs_entries:
        if elm.fieldName in data_dict:
            data_dict[elm.fieldName].append(elm)
        else:
            data_dict[elm.fieldName] = [elm]

    date_format = '%Y-%m-%d'
    start_field = 'gsd:ReportingPeriodStartDate'
    end_field = 'gsd:ReportingPeriodEndDate'
    last_end_field = 'gsd:PredingReportingPeriodEndDate'
    start_date = None
    end_date = None
    last_end_date = None
    if start_field in data_dict:
        start_date = datetime.datetime.strptime(data_dict[start_field][0].fieldValue[:10], date_format)
    if end_field in data_dict:
        end_date = datetime.datetime.strptime(data_dict[end_field][0].fieldValue[:10], date_format)
    if last_end_field in data_dict:
        last_end_date = datetime.datetime.strptime(data_dict[last_end_field][0].fieldValue[:10], date_format)

    result = []
    
    def parse_entry(entry):
        if entry.startDate is None and entry.endDate is None:
            return None
        elif date_is_instant(entry.startDate, entry.endDate):
            if not date_is_in_range(start_date, end_date, entry.endDate):
                return make_prev_tuple(entry)
            elif (last_end_date is not None) and (entry.endDate <= last_end_date):
                return make_prev_tuple(entry)
        elif not (date_is_in_range(start_date, end_date, entry.startDate) or date_is_in_range(start_date, end_date, entry.endDate)):
            return make_prev_tuple(entry)
        return entry

    for key, items in data_dict.items():
        if len(items) == 1:
            parsed_entry = parse_entry(items[0])
            if parsed_entry is not None:
                result.append(parsed_entry)
                # maybe just add it.
        else:
            endDates = [x.endDate for x in items if x.endDate is not None]
            if len(endDates) == 0:
                continue
            max_date = max(endDates)
            for _entry in items:
                parsed_entry = parse_entry(_entry)
                if not parsed_entry.fieldName.endswith(prev_tag) and (
                        (max_date - parsed_entry.endDate) >= datetime.timedelta(days=360)):
                    parsed_entry = make_prev_tuple(parsed_entry)
                result.append(parsed_entry)
    return result
                

