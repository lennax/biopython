
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
        Connect to the database and create the tables
        Expected tables:
        metadata (
            id INTEGER PRIMARY KEY,
            filename TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT
        )
        site (
            id INTEGER PRIMARY KEY,
            metadata INTEGER,
            accession TEXT,
            position INTEGER,
            site_id TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT,
            FOREIGN KEY(metadata) REFERENCES metadata(id),
        )
        variant (
            id INTEGER PRIMARY KEY,
            site INTEGER,
            name TEXT,
            ref TEXT,
            alt TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT
            FOREIGN KEY(site) REFERENCES site(id),
        )

        """
        raise NotImplementedError

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
        pass


class VariantSqlite(VariantDB):
    """
    Sqlite3 interface for variant storage.

    """

    def __init__(self, dbname=None):
        """Connect to the database and create the tables."""
        if dbname is None:
            dbname = "variant.db"
        self.conn = sqlite3.connect(dbname)
        self.cursor = self.conn.cursor()
        self.schema = dict(
            metadata=[
                ('id', 'INTEGER PRIMARY KEY'),
                ('filename', 'TEXT'),
                ('misc', 'TEXT'),
                ('create_date', 'TEXT'),
                ('update_date', 'TEXT'),
            ],
            site=[
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
            variant=[
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
        )

        for name, col_list in self.schema.iteritems():
            cols = ", ".join((" ".join(item) for item in col_list))
            create_stmt = "CREATE TABLE IF NOT EXISTS %s (%s)" % (
                            name, cols)
            self.cursor.execute(create_stmt)
        self.conn.commit()

    def __del__(self):
        """Try to close the database connection"""
        try:
            self.conn.close()
        except AttributeError:
            pass

    def insert_row(self, table, **insert_dict):
        """Insert a row into a table. Return id of inserted row."""
        values = ", ".join(  # join items by comma
            ("".join((":", x[0]))  # prepend keys with colon
            for x in self.schema[table] if x[0] != "FOREIGN KEY"))
        insert_string = "INSERT INTO %s VALUES (%s)" % (table, values)
        time = self._time()
        insert_dict['id'] = None
        insert_dict['create_date'] = time
        insert_dict['update_date'] = time
        self.cursor.execute(insert_string, insert_dict)
        self.conn.commit()
        return self.cursor.lastrowid

    def _time(self):
        "Return current time as YYYY-mm-DD HH:MM:SS"
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def query(self, query):
        pass

if __name__ == "__main__":
    db = VariantSqlite("test.db")
    meta_row = db.insert_row(
        table="metadata",
        filename="myfile",
        misc=json.dumps(dict(FORMATS="blahblah", INFOS="bloobloo")))
    site_row = db.insert_row(
        table="site",
        metadata=meta_row,
        accession="rf8320d",
        position=12324,
        site_id="ggsgdg",
        misc=json.dumps(dict(CHROM=2)))
    variant_row = db.insert_row(
        table="variant",
        site=site_row,
        name="NA001",
        ref="A",
        alt="G",
        misc=json.dumps(dict(called=True, phased=False)))
    print "meta", meta_row
    print "site", site_row
    print "variant", variant_row
