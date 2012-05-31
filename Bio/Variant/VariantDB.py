
from abc import ABCMeta, abstractmethod
from datetime import datetime
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
            FOREIGN KEY(metadata) REFERENCES metadata(id),
            accession TEXT,
            position INTEGER,
            site_id TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT,
        )
        variant (
            id INTEGER PRIMARY KEY,
            site INTEGER,
            FOREIGN KEY(site) REFERENCES site(id),
            name TEXT,
            ref TEXT,
            alt TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT
        )

        """
        raise NotImplementedError

    @abstractmethod
    def __del__(self):
        """Close the database connection"""
        raise NotImplementedError

    @abstractmethod
    def metadata_insert(self, filename, misc):
        """
        Insert a row into metadata. Expected schema:
        metadata (
            id INTEGER PRIMARY KEY,
            filename TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT
        )
        Set create_date to current datetime.
        Return id.
   
        """
        raise NotImplementedError

    @abstractmethod
    def site_insert(self, metadata, accession, position, site_id, misc):
        """
        Insert a row into site. Expected schema:
        site (
            id INTEGER PRIMARY KEY,
            metadata INTEGER,
            FOREIGN KEY(metadata) REFERENCES metadata(id),
            accession TEXT,
            position INTEGER,
            site_id TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT
        )
        Set create_date and update_date to current datetime.
        Return id.

        """
        raise NotImplementedError

    @abstractmethod
    def variant_insert(self, site, name, ref, alt, misc):
        """
        Insert a row into variant. Expected schema:
        variant (
            id INTEGER PRIMARY KEY,
            site INTEGER,
            FOREIGN KEY(site) REFERENCES site(id),
            name TEXT,
            ref TEXT,
            alt TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT
        )
        Set create_date and update_date to current datetime.
 
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
        schema = dict()
        schema['metadata'] = """(
            id INTEGER PRIMARY KEY,
            filename TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT
        )"""
        schema['site'] = """(
            id INTEGER PRIMARY KEY,
            metadata INTEGER,
            accession TEXT,
            position INTEGER,
            site_id TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT,
            FOREIGN KEY(metadata) REFERENCES metadata(id)
        )"""
        schema['variant'] = """(
            id INTEGER PRIMARY KEY,
            site INTEGER,
            var_name TEXT,
            ref TEXT,
            alt TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT,
            FOREIGN KEY(site) REFERENCES site(id)
        )"""

        for k, v in schema.iteritems():
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS %s %s''' \
                                % (k, v))
        self.conn.commit()

    def __del__(self):
        """Try to close the database connection"""
        try:
            self.conn.close()
        except AttributeError:
            pass

    def metadata_insert(self, filename, misc):
        """Insert a row into metadata. Return id of inserted row."""
        insert_string = """INSERT INTO metadata VALUES (
            NULL, ?, ?, ?, ?)"""
        self.cursor.execute(insert_string, (filename, misc, 
                            datetime.now(), datetime.now()))
        self.conn.commit()
        return self.cursor.lastrowid

    def site_insert(self, metadata, accession, position, site_id, misc):
        pass

    def variant_insert(self, site, name, ref, alt, misc):
        pass

    def query(self, query):
        pass
    
if __name__ == "__main__":
    db = VariantSqlite("test.db")

