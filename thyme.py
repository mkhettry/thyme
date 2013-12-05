import cmd
import db
from datetime import date

import logging

class Thyme(cmd.Cmd):
    """Simple command line interpreter to explore expenses"""

    def do_EOF(self, args):
        return True

    def do_list(self, args=""):
        """
        list transactions. You can say 'list 10' for transactions in october.
        without a numeric list defaults to the current month.
        """
        start, end = Thyme.get_start_end(args)
        transactions = db.read_txn_for_time(start, end)
        for tx in transactions:
            desc = " ".join(tx['description'].split()).title()
            print("%-8s %-30s %-20s %10.2f" %
                  (tx['date'].isoformat(), desc, tx['name'].title(), tx['amount']))

    def do_bycat(self, args=""):
        """ show transactions by category. bycat 10 will aggregate transactions by category for the month of october"""
        start, end = Thyme.get_start_end(args)
        transactions = db.read_txn_for_time_by_category(start, end)
        for tx in transactions:
            print("%-30s %10.2f" %
                  (tx['name'].title(), tx[1]))



    def do_cat(self, args):
        """
        list all categories
        """
        for cat in db.list_categories():
            print("%-4s %-10s" % (str(cat[0]), cat[2]))

    @staticmethod
    def get_start_end(args):
        today = date.today()

        if not args:
            start = date(today.year, today.month, 1)
            end = Thyme.start_of_next_month(today.year, today.month)
            return start, end
        else:
            month = int(args)
            start = date(today.year, month, 1)
            end = Thyme.start_of_next_month(today.year, month)
            return start, end

    @staticmethod
    def start_of_next_month(year, month):
        if month == 12:
            return date(year + 1, 1, 1)
        else:
            return date(year, month + 1, 1)


if __name__ == '__main__':
    logging.basicConfig(filename='example.log', level=logging.INFO)
    thyme = Thyme()
    thyme.prompt = "thyme> "
    thyme.cmdloop()