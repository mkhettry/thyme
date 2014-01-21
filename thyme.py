import cmd
import db
from datetime import date

import loader
import logging


class Thyme(cmd.Cmd):
    """Simple command line interpreter to explore expenses"""

    GREEN = '\033[92m'
    RED = '\033[91m'
    END = '\033[0m'

    MONTHS = {"jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3, "apr": 4, "april": 4,
              "may": 6, "june": 6, "jun": 6, "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9,
              "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12}

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.tx_id_map = {}

    def do_EOF(self, args):
        return True

    def do_list(self, args=""):
        """
        list transactions. You can say 'list 10' for transactions in october.
        without a numeric list defaults to the current month
        """
        args_array = args.split()
        if len(args_array) == 1:
            time = args_array[0]
            category_filter = None
        else:
            time = args_array[1]
            category_filter = args_array[0].strip().lower()

        start, end = self.guess_time_range(time)

        transactions = db.read_txn_for_time(start, end, category_filter)
        i = 0
        sum = 0

        for tx in transactions:
            desc = " ".join(tx['description'].split()).title()[0:29]
            i += 1
            self.tx_id_map[i] = tx["id"]
            sum += float(tx['amount'])
            print("%-3s %8s %-8s %-30s %-20s %10.2f" %
                  (i, tx['nickname'], tx['date'].isoformat(), desc, tx['name'].title(), tx['amount']))
        print("%67s %10.2f" % ("Total", sum))

    def do_updcat(self, args=""):
        """
        update the category of one transaction. You can say `updcat <txid> <categoryname>'
        """
        txid, category = args.split()
        trimmed_category = category.strip().lower()
        category_id = db.find_category_id(trimmed_category)
        if not category_id:
            print("I couldn't find a category %s" % trimmed_category)
        else:
            rowcount = db.update_tx_category(self.tx_id_map[int(txid)], category_id)
            if rowcount == 0:
                print("no rows found")
            else:
                print("row updated")


    def do_bycat(self, args=""):
        """ show transactions by category. bycat 10 will aggregate transactions by category for the month of october"""
        start, end = self.guess_time_range(args)
        transactions = db.read_txn_for_time_by_category(start, end)
        sum = 0.0

        budget_map = {}
        total_budget = 0
        for cat in db.list_categories():
            budget_map[cat['name']] = cat['budget']
            total_budget += cat['budget']

        for tx in transactions:
            category_name = tx['name'].title()
            if category_name == 'Transfer' or category_name == 'Paycheck':
                continue
            sum += float(tx[1])
            diff = budget_map[tx['name']] + float(tx[1])
            if diff > 0:
                color = self.GREEN
            else:
                color = self.RED

            print("%-30s %10.2f %5d %s %8.2f %s" % (tx['name'].title(), tx[1], budget_map[tx['name']], color, diff, self.END))
        print("%-30s %10.2f %6d" % ("", sum, total_budget))


    def do_cat(self, args):
        """
        list all categories
        """
        for cat in db.list_categories():
            print("%-4s %-24s %-4d" % (str(cat['id']), cat['name'], cat['budget']))

    def do_acct(self, args):
        """
        CRUD on accounts
        """
        args_array = args.split()
        command = args_array and args_array[0] or "list"
        if command == "list":
            print("%-4s %-6s %-20s %6s" % ("Id", "Nickname", "Name", "Fid"))
            print("-------------------------------------")
            for account in db.list_institutions():
                print("%-4d %-6s %-20s %6d" % (account[0], account[1], account[2], account[3]))
        elif command == "update":
            db.update_institution(int(args_array[1]), args_array[2])

    #@staticmethod
    #def print_table(fields, rows):
    #    for field, size in fields.iteritems():
    #

    def do_load(self, args):
        loader.load_qfx_new()

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

    # valid values are jan, jan:mar, jan, 1, 1:3
    def guess_time_range(self, args):
        logging.info("Guessing time range for " + args)

        args_array = args.split(":")
        if len(args_array) == 1:
            start = self.get_first_of_month(args_array[0])
            end = self.next_month(start)
            return start, end
        else:
            start = self.get_first_of_month(args_array[0])
            end = self.get_last_of_month(args_array[1])

            return start, end


    def next_month(self, dt):
        if dt.month == 12:
            return date(dt.year + 1, 1, 1)
        else:
            return date(dt.year, dt.month + 1, 1)


    def get_first_of_month(self, arg):
        today = date.today()

        month = self.get_month(arg)

        if month <= today.month:
            return date(today.year, month, 1)
        else:
            return date(today.year - 1, month, 1)

    def get_month(self, arg):
        normalized = arg.strip().lower()
        if normalized in self.MONTHS:
            month = self.MONTHS[normalized]
        else:
            month = int(normalized)
        return month

    def get_last_of_month(self, arg):
        today = date.today()

        month = self.get_month(arg)
        if month <= today.month:
            year = today.year
        else:
            year = today.year - 1

        if month == 12:
            return date(year + 1, 1, 1)
        else:
            return date(year, month + 1, 1)


    @staticmethod
    def start_of_next_month(year, month):
        if month == 12:
            return date(year + 1, 1, 1)
        else:
            return date(year, month + 1, 1)


if __name__ == '__main__':
    thyme = Thyme()
    thyme.prompt = "thyme> "
    thyme.cmdloop()