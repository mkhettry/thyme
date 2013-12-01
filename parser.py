from datetime import datetime


class Parser:
    def __init__(self, date_field, date_format, description_field, amount_field, amount_negated):
        self.date_field = date_field
        self.date_format = date_format
        self.description_field = description_field
        self.amount_field = amount_field
        self.amount_negated = amount_negated

    def parse(self, row):
        try:
            amount = float(row[self.amount_field])
            if self.amount_negated:
                amount = -amount

            return {
                "date": datetime.strptime(row[self.date_field], self.date_format),
                "description": row[self.description_field],
                "amount": amount
            }

        except ValueError:
            return {}
        except IndexError:
            return {}

parsers = { 'amex': Parser(0, "%m/%d/%Y %a", 2, 7, True),
            'bofa': Parser(0, "%m/%d/%Y", 1, 2, False),
            'chase': Parser(1, "%m/%d/%Y", 3, 4, False)}
