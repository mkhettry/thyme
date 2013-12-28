import cmd
import db
from datetime import date

import loader


class Thyme(cmd.Cmd):
    """Simple command line interpreter to explore expenses"""

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.tx_id_map = {}

    def do_EOF(self, args):
        return True

    def do_list(self, args=""):
        """
        list transactions. You can say 'list 10' for transactions in october.
        without a numeric list defaults to the current month.
        """
        args_array = args.split()
        if len(args_array) == 1:
            time = args_array[0]
            category_filter = None
        else:
            time = args_array[1]
            category_filter = args_array[0].strip().lower()

        start, end = Thyme.get_start_end(time)
        transactions = db.read_txn_for_time(start, end, category_filter)
        i = 0
        sum = 0

        for tx in transactions:
            desc = " ".join(tx['description'].split()).title()[0:29]
            i += 1
            self.tx_id_map[i] = tx["id"]
            sum += float(tx['amount'])
            print("%-3s %-8s %-8s %-30s %-20s %10.2f" %
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
        start, end = Thyme.get_start_end(args)
        transactions = db.read_txn_for_time_by_category(start, end)
        sum = 0.0
        for tx in transactions:
            category_name = tx['name'].title()
            if category_name == 'Transfer' or category_name == 'Paycheck':
                continue
            sum += float(tx[1])
            print("%-30s %10.2f" % (tx['name'].title(), tx[1]))
        print("%-30s %10.2f" % ("", sum))


    def do_cat(self, args):
        """
        list all categories
        """
        for cat in db.list_categories():
            print("%-4s %-10s" % (str(cat[0]), cat[2]))

    def do_inst(self, args):
        """
        CRUD on financial institutions
        """
        args_array = args.split()
        command = args_array and args_array[0] or "list"
        if command == "list":
            for institution in db.list_institutions():
                print("%-4s %-10s %-10s" % (str(institution[0]), institution[1], institution[2]))
        elif command == "create":
            db.create_institution(args_array[1], args_array[2])

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