import os
import json
from datetime import date

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH


class Row(object):
    def __init__(self, row):
        for key, value in row.items():
            setattr(self, key, value)
            

class DateTimeEncoder(json.JSONEncoder):
    def default(self, entry):
        if isinstance(entry, date):
            return entry.isoformat()
        return json.JSONEncoder.default(self, entry)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name
        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                           '{}.json'.format(self.name))
        if not os.path.exists(self.table_filepath):
            with open(self.table_filepath, 'w') as file_object:
                table_data = {'columns': columns, 'rows': []}
                json_data = json.dumps(table_data)
                file_object.write(json_data)
        self.columns = columns or self._read_columns()


    def _read_columns(self):
        with open(self.table_filepath, 'r') as file_object:
            return json.load(file_object)['columns'] 

    def insert(self, *args):
        if len(args) != len(self.columns):
            raise ValidationError('Invalid amount of field')
        
        new_row = {}
        for index, column in enumerate(self.columns):
            ctype = column['type']
            atype = type(args[index]).__name__
            if ctype != atype:
                raise ValidationError(
                    'Invalid type of field "{}": Given "{}", expected "{}"'
                    .format(column['name'], atype, ctype))
            new_row[column['name']] = args[index]
                
        with open(self.table_filepath, 'r+') as f:
            data = json.load(f)
            data['rows'].append(new_row)
            f.seek(0)
            f.write(json.dumps(data, cls=DateTimeEncoder))

    def query(self, **kwargs):
        with open(self.table_filepath, 'r') as f:
            data = json.load(f)
            for row in data['rows']:
                for key, value in kwargs.items():
                    if value == row[key]:
                        yield Row(row)
                

    def all(self):
        with open(self.table_filepath, 'r') as f:
            data = json.load(f)
            for row in data['rows']:
                yield Row(row)

    def count(self):
        with open(self.table_filepath, 'r') as f:
            return (len(json.load(f)['rows']))

    def describe(self):
        return self.columns


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        if os.path.exists(db_filepath):
            raise ValidationError('Database with name "{}" already exists.'
                                  .format(name))
        os.makedirs(db_filepath)

    def _read_tables(self):
        PATH = self.db_filepath
        table_list = []
        for file in os.listdir(PATH):
            if file.endswith(".json"):
                base_file = file[:-5]
                table_data = Table(self, base_file)
                setattr(self, base_file, table_data)
                table_list.append(base_file)
        return table_list
                    
    def create_table(self, table_name, columns):
        PATH = self.db_filepath
        for file in os.listdir(PATH):
            if (table_name + '.json') == file:
                raise ValidationError
        new_table = Table(self, table_name, columns)
        setattr(self, table_name, new_table)
        self.tables.append(table_name)

    def show_tables(self):
        return self.tables 


def create_database(db_name):
    DataBase.create(db_name)
    return connect_database(db_name)

def connect_database(db_name):
    return DataBase(name=db_name)
