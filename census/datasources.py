from decimal import InvalidOperation

from census.parse import FormulaParser
from census.models import Row
from census.meta import CensusMeta, ACSMeta, Census2010Meta, ACS2010Meta
from census.data import Value


class CensusBase(object):
    def __init__(self):
        self.parser = FormulaParser(self)

    def data(self, formula, geo_dict):
        t = self.parser.parse(formula)
        result = t(geo_dict)
        return result

    def _type_value(self, str_value):
        from decimal import Decimal
        try:
            return int(str_value)
        except ValueError:
            try:
                return Decimal(str_value)
            except InvalidOperation:
                return None


class Census2000(CensusBase):
    def __init__(self, summary_file):
        if summary_file not in ['SF1', 'SF3']:
            raise ValueError('%s is not a supported summary file.' % summary_file)

        self.summary_file = summary_file
        super(Census2000, self).__init__()

    def get_value(self, table, geo_dicts):
        if not isinstance(geo_dicts, list):
            geo_dicts = [geo_dicts]
        census_info = CensusMeta(self.summary_file)
        fileid = 'u%s' % self.summary_file

        col = census_info.csv_column_for_matrix(table)
        raw_vals = Row.objects.filter(
            fileid=fileid,
            cifsn=census_info._file_name_for_matrix(table),
            stusab__in=map(lambda g: g['STUSAB'].upper(), geo_dicts),
            logrecno__in=map(lambda g: g['LOGRECNO'], geo_dicts)
        ).values_list("col%s" % str(col - 4), flat=True)

        return map(lambda v: Value(self._type_value(v)), raw_vals)


class Census2010(CensusBase):
    def __init__(self, file_type):
        if file_type not in ['pl', 'sf1']:
            raise ValueError('%s is not a supported 2010 file type' % file_type)
        self.file_type = file_type

        if self.file_type == 'sf1':
            self.file_id = 'SF1ST'
        super(Census2010, self).__init__()

    def get_value(self, table, geo_dicts):
        census_info = Census2010Meta(self.file_type)

        if not isinstance(geo_dicts, list):
            geo_dicts = [geo_dicts]

        col = census_info.csv_column_for_matrix(table)
        raw_vals = Row.objects.filter(
            fileid=self.file_id,
            cifsn=census_info._file_name_for_matrix(table),
            stusab__in=map(lambda g: g['STUSAB'].upper(), geo_dicts),
            logrecno__in=map(lambda g: g['LOGRECNO'], geo_dicts)
        ).values_list("col%s" % str(col - 4), flat=True)

        return map(lambda v: Value(self._type_value(v)), raw_vals)


class ACS2009e5(CensusBase):
    def get_value(self, table, geo_dicts):
        acs_info = ACSMeta()

        if not isinstance(geo_dicts, list):
            geo_dicts = [geo_dicts]
        acs_info = ACSMeta()

        col = acs_info.csv_column_for_matrix(table)
        raw_values = Row.objects.filter(
            fileid='ACSSF',
            filetype__in=['2009e5', '2009m5'],
            cifsn=acs_info._file_name_for_matrix(table),
            stusab__in=map(lambda g: g['STUSAB'].lower(), geo_dicts),
            logrecno__in=map(lambda g: g['LOGRECNO'], geo_dicts)
        ).values_list('logrecno', 'filetype', "col%s" % str(col - 5))
        values = {}
        for logrecno, filetype, val in raw_values:
            if not logrecno in values:
                values[logrecno] = Value(0)

            if filetype == '2009e5':
                values[logrecno].value = self._type_value(val)
            else:
                values[logrecno].moe = self._type_value(val)

        return values.values()


class ACS2010e5(CensusBase):
    def get_value(self, table, geo_dicts):
        if not isinstance(geo_dicts, list):
            geo_dicts = [geo_dicts]

        acs_info = ACS2010Meta()
        col = acs_info.csv_column_for_matrix(table)

        raw_values = Row.objects.filter(
            fileid='ACSSF',
            filetype__in=['2010e5', '2010m5'],
            cifsn=acs_info._file_name_for_matrix(table),
            stusab__in=map(lambda g: g['STUSAB'].lower(), geo_dicts),
            logrecno__in=map(lambda g: g['LOGRECNO'], geo_dicts)
        ).values_list('logrecno', 'filetype', "col%s" % str(col - 5))

        values = {}
        for logrecno, filetype, val in raw_values:

            value = 0
            moe = 0

            if filetype == '2010e5':

                value = self._type_value(val)
            else:
                moe = self._type_value(val)

            if not logrecno in values:
                values[logrecno] = Value(value, moe)

        return values.values()
