import re, datetime


def sum_n(vals):
    non_null_vals = [val for val in vals if val]
    return sum(non_null_vals) if non_null_vals else None


def isendstring(substr, val):
    if type(substr) == str and type(val) == str and val[-len(substr) :] == substr:
        return len(val) - len(substr) + 1
    else:
        return 0


def ismidstring(substr, val):
    if (
        type(substr) == str
        and type(val) == str
        and substr in val
        and val[: len(substr)] != substr
    ):
        return val.index(substr) + 1
    else:
        return 0


def regex_replace(substr1, substr2, val):
    if type(substr1) == str and type(substr2) == str and type(val) == str:
        return re.sub(substr1, substr2, val)
    else:
        return val


def replace(substr1, substr2, val):
    if type(substr1) == str and type(substr2) == str and type(val) == str:
        return val.replace(substr1, substr2)
    else:
        return val


def substr_index(substr, val, i=1):
    if type(substr) == str and type(val) == str and type(i) == int:
        if substr in val[i - 1 :]:
            return val[i - 1 :].index(substr) + i
        else:
            return 0
    else:
        return None


def to_datetime_infer(val):
    if type(val) == str and re.search("^\d?\d/\d?\d/\d\d\d\d$", val):
        return datetime.datetime.strptime(val, "%d/%m/%Y")
    elif type(val) == str and re.search("^\d\d\d\d/\d?\d/\d?\d$", val):
        return datetime.datetime.strptime(val, "%Y/%m/%d")
    elif type(val) == str and re.search("^\d\d\d\d-\d?\d-\d?\d$", val):
        return datetime.datetime.strptime(val, "%Y-%m-%d")
    elif type(val) == str and re.search("^\d\d\d\d\d\d\d\d$", val):
        return datetime.datetime.strptime(val, "%Y%m%d")
    elif type(val) == str and re.search("^\d\d\d\d-\d?\d-\d?\d \d\d:\d\d:\d\d$", val):
        return datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    elif type(val) == str and re.search(
        "^\d\d\d\d-\d?\d-\d?\d \d\d:\d\d:\d\d\.\d+$", val
    ):
        return datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
    elif type(val) == datetime.datetime:
        return val
    elif type(val) == datetime.date:
        return datetime.datetime(val.year, val.month, val.day)
    else:
        return None


def to_date_infer(val):
    val = to_datetime_infer(val)
    if val == None:
        return None
    else:
        return val.date()


def strip(val):
    return val.strip() if type(val) == str else val


def rstrip(val):
    return val.rstrip() if type(val) == str else val


def lstrip(val):
    return val.lstrip() if type(val) == str else val


def datetime_timestamp(year, month, day, hour, minute, second):
    if (
        type(year) == int
        and type(month) == int
        and type(day) == int
        and type(hour) == int
        and type(minute) == int
        and type(second) == int
    ):
        year = min(year, 9999)
        try:
            return datetime.datetime(year, month, day, hour, minute, second)
        except ValueError:
            return None
    else:
        return None


def datetime_date(year, month, day):
    datetime_obj = datetime_timestamp(year, month, day, 0, 0, 0)
    if type(datetime_obj) == datetime.datetime:
        return datetime_obj.date()
    else:
        return None


def seconds_to_datetime(seconds):
    if seconds:
        return datetime.datetime.fromtimestamp(seconds)
    else:
        return seconds


def num_to_month(num):
    if type(num) == int and 1 <= num <= 12:
        return [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ][num - 1]
    else:
        return None


def allbutfirst(i, val):
    if type(val) != str:
        return val
    if i > len(val):
        return None
    else:
        return val[i:]


def allbutlast(i, val):
    if type(val) != str:
        return val
    if i > len(val):
        return None
    else:
        return val[:-i]


def to_string(val):
    if val == None:
        return val
    elif type(val) == float:
        return "{:.6f}".format(val)
    else:
        return str(val)