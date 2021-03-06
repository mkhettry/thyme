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
    LIST_PARSER.add_argument('--new', const='new', dest='new', nargs='?', help='time range for transactions')

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
            list --new -f uncat # Show all uncategorized transactions from the last load command.
        """

        parsed_args = self.LIST_PARSER.parse_args(args.split())
        logging.info(parsed_args)

        start, end = self.guess_time_range(parsed_args.timerange)
        if parsed_args.new and not parsed_args.timerange:
            start = date(2000, 1, 1)
            end = date.today()

        idx = 0
        sum = 0.0

        transactions = db.read_txn_for_time(start, end, parsed_args.filter, only_new=parsed_args.new)

        td = TabularDisplay(('Id', -3), ('Acct', -8), ('Date', -10), ('Description', -30), ('Category', -20), ('Amount', 10, '*'))
        td.print_header()

        for tx in transactions:
            desc = " ".join(tx['description'].split()).title()[0:29]
            self.tx_id_map[idx] = tx["id"]
            td.print_row(idx, tx['nickname'], tx['date'].isoformat(), desc, tx['name'].title(), self.print_amount(tx['amount']))

            idx += 1
            sum += tx['amount']

        td.print_summary(self.print_amount(sum))


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
        td = TabularDisplay(('Category', -30), ('Total', 10, '*'), ('Budget', 10, '*'), ('Diff', 10, '*'))
        td.print_header()

        for tx in transactions:
            category_name = tx['name'].title()
            if category_name == 'Transfer' or category_name == 'Paycheck':
                continue
            sum += float(tx[1])
            diff = budget_map[tx['name']] + float(tx[1])
            td.print_row(tx['name'].title(), self.print_amount(tx[1], color_negative=False), budget_map[tx['name']],
                         self.print_amount(diff, color_negative=True))
        td.print_summary(self.print_amount(sum), total_budget, self.print_amount(total_budget + sum, color_negative=True))


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



    def do_cat(self, args):
        """
        List, Add or Update categories

        Examples:
            cat                           # list out all categories
            cat list                      #  -- ditto --
            cat add booze                 # what can i say? i drink a lot!
            cat update shopping 200       # set the budget for shopping to 200.
        """
        args_array = args.split()
        command = args_array and args_array[0] or "list"

        if command == "list":
            budget = 0
            td = TabularDisplay(('Name', -24), ('Budget', 6, '*'))
            td.print_header()
            for cat in db.list_categories():
                td.print_row(cat['name'].title(), cat['budget'])
                #print("%-4s %-24s %-4d" % (str(cat['id']), cat['name'], cat['budget']))
                budget += cat['budget']
            td.print_summary(budget)

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


    def do_load(self, args):
        db.clear_last_load()
        loader.load_qfx_new()
        print("Use 'list --new' to see new transactions loaded by this command.")

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

    def print_amount(self, amount, color_negative=False):
        if amount < 0.0:
            if color_negative:
                return self.RED + '%10.2f' % -amount + self.END
            else:
                return '%10.2f' % -amount
        else:
            return self.GREEN + ('%10.2f' % amount) + self.END



class TabularDisplay(object):

    def __init__(self, *columns):
        self.columns = columns
        self.header = ""
        self.divider = ""

        # print("%-3s %-8s %-8s %-30s %-16s %-12s" %

        for column in self.columns:
            if self.header:
                self.header += " "
            fmt = "%" + str(column[1]) + 's'
            self.header += (fmt % column[0])

        # I'm sure there is a better way to do this! No stackoverlow over the pacific :(
        for i in xrange(len(self.header)):
            self.divider += "-"

    def print_header(self):
        print(self.header)
        print(self.divider)

    def print_row(self, *values):
        if len(values) != len(self.columns):
            raise ArgumentError
        # man, this is ugly. no other words

        row = ""
        idx = 0
        for column in self.columns:
            if row:
                row += " "
            format = "%" + str(column[1]) + 's'
            row += (format % values[idx])
            idx += 1

        print(row)

    def print_summary(self, *values):
        print(self.divider)
        summary = ""
        values_idx = 0
        for column in self.columns:
            if summary:
                summary += ' '

            fmt = "%" + str(column[1]) + 's'
            if len(column) > 2:
                summary += (fmt % values[values_idx])
                values_idx += 1
            else:
                summary += (fmt % ' ')

        print(summary)

if __name__ == '__main__':
    thyme = Thyme()
    thyme.prompt = "thyme> "
    thyme.cmdloop()