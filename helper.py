import re
from io import StringIO
from html.parser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()

def get_price(price):
    value = int(float(extract_number_only(
        price, thousand_separator=',', scale_separator='.')))
    value_2 = int(float(extract_number_only(
        price, thousand_separator='.', scale_separator=',')))
    price = min(value, value_2)
    if price < 10:
        price = max(value, value_2)
    return price
def extract_number_only(input_string, thousand_separator='.', scale_separator=','):
    input_string = str(input_string).replace(thousand_separator, "")
    input_string = input_string.replace(scale_separator, ".")

    numbers = re.findall(r'\d+(?:\.\d+)?', input_string)
    if numbers:
        return numbers[0]
    else:
        return 0

def extract_utilities(currency, description):
    if currency in description:
        find = description.rindex(currency)
        utilities = description[(find + 1):(find + 7)]
        return utilities
    else:
        return None

class ItemClear:
    def __init__(self, response=None, item_loader=None, item_name=None, input_value=None, input_type=None,
                 split_list={}, replace_list={}, get_num=False, lower_or_upper=None,
                 tf_item=False, tf_words={}, tf_value=True, sq_ft=False, per_week=False):
        self.response = response
        self.item_loader = item_loader
        self.item_name = item_name
        self.input_value = input_value
        self.input_type = input_type
        self.split_list = split_list
        self.replace_list = replace_list
        self.get_num = get_num
        self.lower_or_upper = lower_or_upper
        self.tf_item = tf_item
        self.tf_words = tf_words
        self.tf_value = tf_value
        self.sq_ft = sq_ft
        self.per_week = per_week
        self.raw_data = ""
