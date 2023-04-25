import re

_RE_COMBINE_WHITESPACE = re.compile(r"\s\s+")

def get_sql_commands(sql_file_path):
    # Read file and return the sql commands as a list
    with open(sql_file_path, 'r') as f:
        sql_file = f.read()
    # all SQL commands (split on ';')
    # sql_file = sql_file.strip('\n')
    sql_file = sql_file.replace('\n', ' ')
    sql_file = sql_file.replace('\t', ' ')
    sql_file = _RE_COMBINE_WHITESPACE.sub(" ", sql_file).strip()
    sql_file = sql_file.replace(' )', ')')
    sql_file = sql_file.replace('( ', '(')
    sql_file = sql_file.replace(' ,', ',')
    sql_commands = sql_file.split(';')
    sql_commands = [s + ';' for s in sql_commands if len(s)>=1]
    return sql_commands

