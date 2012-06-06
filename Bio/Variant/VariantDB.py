
from abc import ABCMeta, abstractmethod
from datetime import datetime
import json
import sqlite3


class VariantDB(object):
    """
    Interface/ABC for Variant database IO
    All methods must be implemented by child classes.

    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, dbname=None):
        """
        Connect to the database and create the tables.
        NOTE: call this method in the child implementation
        to initialize schema values.
        """
        # Expected schema for predefined tables
        self.schema = {
            'metadata': [
                ('id', 'INTEGER PRIMARY KEY'),
                ('filename', 'TEXT'),
                ('misc', 'TEXT'),
                ('create_date', 'TEXT'),
                ('update_date', 'TEXT'),
            ],
            'site': [
                ('id', 'INTEGER PRIMARY KEY'),
                ('metadata', 'INTEGER'),
                ('accession', 'TEXT'),
                ('position', 'INTEGER'),
                ('site_id', 'TEXT'),
                ('chrom', 'TEXT'),
                ('filter', 'TEXT'),
                ('qual', 'TEXT'),
                ('misc', 'TEXT'),
                ('create_date', 'TEXT'),
                ('update_date', 'TEXT'),
                ('FOREIGN KEY', '(metadata) REFERENCES metadata(id)'),
            ],
            'variant': [
                ('id', 'INTEGER PRIMARY KEY'),
                ('site', 'INTEGER'),
                ('name', 'TEXT'),
                ('ref', 'TEXT'),
                ('alt', 'TEXT'),
                ('misc', 'TEXT'),
                ('create_date', 'TEXT'),
                ('update_date', 'TEXT'),
                ('FOREIGN KEY', '(site) REFERENCES site(id)'),
            ],
        }

        self.create_stmt = dict()
        self.ins_stmt = dict()
        for table, col_list in self.schema.iteritems():
            self.create_stmt[table] = "CREATE TABLE IF NOT EXISTS \
            %s (%s)" % (
                table,
                ", ".join((" ".join(item) for item in col_list))
            )

            self.ins_stmt[table] = "INSERT INTO %s VALUES (%s)" % (
                table,
                ", ".join(("".join((":", x[0]))
                          for x in col_list if x[0] != "FOREIGN KEY")
                )
            )

    @abstractmethod
    def __del__(self):
        """Close the database connection"""
        raise NotImplementedError

    @abstractmethod
    def insert_row(self, table, **kwargs):
        """
        Insert a row into the table.
        Set create_date and update_date to current datetime.
        Return id.

        """
        raise NotImplementedError

    @abstractmethod
    def query(self, query):
        """
        Run query on DB.
        As DB will most likely be local, security is the user's concern. 

        """
        pass


class VariantSqlite(VariantDB):
    """
    Sqlite3 interface for variant storage.

    """

    def __init__(self, dbname=None):
        """Connect to the database and create the tables."""
        VariantDB.__init__(self, dbname=dbname)
        if dbname is None:
            dbname = "variant.db"
        self.conn = sqlite3.connect(dbname)
        self.cursor = self.conn.cursor()
        for stmt in self.create_stmt.values():
            self.cursor.execute(stmt)
        self.conn.commit()

    def __del__(self):
        """Try to close the database connection"""
        try:
            self.conn.close()
        except AttributeError:
            pass

    def insert_commit(self, **kwargs):
        """Insert a row into a table, commit, and return id"""
        self.insert_row(**kwargs)
        self.conn.commit()
        return self.cursor.lastrowid

    def insert_row(self, table, **insert_dict):
        """Insert a row into a table."""
        insert_string = self.ins_stmt[table]
        time = self._time()
        insert_dict['id'] = None
        insert_dict['create_date'] = time
        insert_dict['update_date'] = time
        self.cursor.execute(insert_string, insert_dict)

    def insert_many(self, table, row_iter):
        """Insert multiple rows; provide an iterable of dicts"""
        insert_string = self.ins_stmt[table]
        time = self._time()
        for insert_dict in row_iter:
            insert_dict['id'] = None
            insert_dict['create_date'] = time
            insert_dict['update_date'] = time
        self.cursor.executemany(insert_string, row_iter)

    def _time(self):
        "Return current time as YYYY-mm-DD HH:MM:SS"
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def query(self, query):
        self.cursor.execute(query)


if __name__ == "__main__":
    db = VariantSqlite("test.db")
    meta_row = db.insert_commit(
        table="metadata",
        filename="myfile",
        misc=json.dumps(dict(FORMATS="blahblah", INFOS="bloobloo")))
    site_row = db.insert_commit(
        table="site",
        metadata=meta_row,
        accession="rf8320d",
        position=12324,
        site_id="ggsgdg",
        chrom=2,
        filter='q10',
        qual='22',
        misc=json.dumps(dict(INFO='something interesting')))
    variant_row = db.insert_row(
        table="variant",
        site=site_row,
        name="NA001",
        ref="A",
        alt="G",
        misc=json.dumps(dict(called=True, phased=False)))
    db.conn.commit()
    print "meta", meta_row
    print "site", site_row
    print "variant", variant_row
