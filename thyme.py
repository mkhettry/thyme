import cmd
import db
from datetime import date

import loader
import logging
import argparse
from argparse import ArgumentError

class Thyme(cmd.Cmd):
    """Simple command line interpreter to explore expenses"""

    GREEN = '\033[92m'
    RED = '\033[91m'
    END = '\033[0m'

    MONTHS = {"jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3, "apr": 4, "april": 4,
              "may": 6, "june": 6, "jun": 6, "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9,
              "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12}

    LIST_PARSER = argparse.ArgumentParser(description='List Parser')
    LIST_PARSER.add_argument("-f", "--filter", help='Search for transactions by this filter', default="")
    LIST_PARSER.add_argument('timerange', nargs='?', help='time range for transactions')

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.tx_id_map = {}

    def do_EOF(self, args):
        return True

    def do_list(self, args=""):
        """
        list transactions. The syntax is list [-f filter] [timerange]

        The filter is matched against the transaction description as well as the category name.
        There are a few different ways to specify the timerange.

        Examples:

            list dec            # Show all transactions for december.
            list nov:jan        # Show all transactions from november to january inclusive.
            list -f coffee jan  # show me tx's with the word coffee or the category coffee for january
            list -f pizza       # All transactions with the word pizza in them for the current month

        """

        parsed_args = self.LIST_PARSER.parse_args(args.split())
        logging.info(parsed_args)

        start, end = self.guess_time_range(parsed_args.timerange)

        idx = 0
        sum = 0
        transactions = db.read_txn_for_time(start, end, parsed_args.filter)
        for tx in transactions:
            sum += self.print_transaction(idx, tx)
            idx += 1

        print("%67s %10.2f" % ("Total", sum))


    def print_transaction(self, tx_id, tx):
        desc = " ".join(tx['description'].split()).title()[0:29]
        self.tx_id_map[tx_id] = tx["id"]
        amount = float(tx['amount'])
        print("%-3s %8s %-8s %-30s %-20s %s" %
              (tx_id, tx['nickname'], tx['date'].isoformat(), desc, tx['name'].title(), self.print_amount(tx['amount'])))
        return amount

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
            print("%-30s %10.2f %6d %s" % (tx['name'].title(), tx[1], budget_map[tx['name']], self.print_amount(diff, neg=True)))

        print("%-30s %10.2f %6d %s" % ("", sum, total_budget, self.print_amount(total_budget + sum)))


    def do_cat(self, args):
        """
        List, Add or Update categories

        Examples:
            cat                           # list out all categories
            cat list                      #  -- ditto --
            cat add booze                 # what can i say? i drink a lot!
            cat update shopping 200    # set the budget for shopping to 200.
        """
        args_array = args.split()
        command = args_array and args_array[0] or "list"

        if command == "list":
            budget = 0.0
            for cat in db.list_categories():
                print("%-4s %-24s %-4d" % (str(cat['id']), cat['name'], cat['budget']))
                budget += cat['budget']
            print("Total Budget: %10.2f" % budget)

        elif command == "add":
            if len(args_array) != 2:
                print("I expect the name of the category you want to add")
            else:
                db.insert_category(args_array[1])
        elif command == "update":
            if len(args_array) != 3:
                print("I expect a category name and amount")
            else:
                if db.update_category(args_array[1], args_array[2]) == 1:
                    print("Category updated")
                else:
                    print("I could not find category '" + args_array[1] + "'")
        else:
            print("I don't understand " + command)



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

        if not args:
            today = date.today()
            start = date(today.year, today.month, 1)
            end = self.next_month(start)
        else:
            args_array = args.split(":")
            if len(args_array) == 1:
                start = self.get_first_of_month(args_array[0])
                end = self.next_month(start)
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

    def print_amount(self, amount, neg=False):
        if amount < 0.0:
            if neg:
                return self.RED + '%10.2f' % -amount + self.END
            else:
                return '%10.2f' % -amount
        else:
            return self.GREEN + ('%10.2f' % amount) + self.END



if __name__ == '__main__':
    thyme = Thyme()
    thyme.prompt = "thyme> "
    thyme.cmdloop()