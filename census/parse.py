from pyparsing import Word, oneOf, operatorPrecedence, opAssoc, printables, nums, alphanums, Optional, OneOrMore, alphas, Combine
from decimal import Decimal
from census.data import Table, Value


class IdentityDatasource(object):
    """ A data source that always returns the table argument """
    def get_value(self, table, geo_dicts):
        return [table, ]


class FormulaParser(object):
    op_map = {
        '+': lambda a, b: a + b,
        '-': lambda a, b: a - b,
        '*': lambda a, b: a * b,
        '/': lambda a, b: a / b,
    }

    def __init__(self, datasource):
        self.datasource = datasource

    def _number_parse_action(self, result):
        number = Value(result[0])
        return Table(IdentityDatasource(), number)

    def _table_parse_action(self, result):
        return Table(self.datasource, result[0])

    def grammar(self):
        number = Combine(Word(nums) + Optional("." + OneOrMore(Word(nums))))
        table = Combine(Word(alphas) + OneOrMore(Word("_"+alphanums)))

        number.setParseAction(self._number_parse_action)
        table.setParseAction(self._table_parse_action)

        signop = oneOf('+ -')
        multop = oneOf('* /')
        plusop = oneOf('+ -')

        operand = number | table

        return operatorPrecedence(operand, [
            (signop, 1, opAssoc.RIGHT),
            (multop, 2, opAssoc.LEFT),
            (plusop, 2, opAssoc.LEFT)
        ])

    def tokens(self, formula):
        return self.grammar().parseString(formula)

    def _df_parse(self, parse_result):
        if type(parse_result) in (Table, Decimal, str):
            return parse_result

        output = None
        pending_op = None
        for expr in parse_result:
            expr = self._df_parse(expr)

            if type(expr) is str:
                pending_op = expr

            if type(expr) is Table:
                if output is None:
                    output = expr
                else:
                    output = self.op_map[pending_op](output, expr)
        return output

    def parse(self, formula):
        formula = str(formula)  # unicode isn't (currently) supported for formula
        return self._df_parse(self.tokens(str(formula))[0])
