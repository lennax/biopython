
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
            name TEXT,
            ref TEXT,
            alt TEXT,
            misc TEXT,
            create_date TEXT,
            update_date TEXT,
            FOREIGN KEY(site) REFERENCES site(id)
        )"""

        for k, v in schema.iteritems():
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS %s %s''' 
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
        insert_string = "INSERT INTO metadata VALUES (:id, :filename, \
                        :misc, :create_date, :update_date)"
        time = self._time()
        insert_dict = dict(id=None, filename=filename, 
            misc=json.dumps(misc), create_date=time, update_date=time)
        self.cursor.execute(insert_string, insert_dict)
        self.conn.commit()
        return self.cursor.lastrowid

    def site_insert(self, metadata, accession, position, site_id, misc):
        """Insert a row into site. Return id of inserted row."""
        insert_string = "INSERT INTO site VALUES (\
            :id, :metadata, :accession, :position, :site_id, :misc, \
            :create_date, :update_date)"
        time = self._time()
        insert_dict = dict(id=None, metadata=metadata, 
            accession=accession, position=position, site_id=site_id,
            misc=json.dumps(misc), create_date=time, update_date=time)
        self.cursor.execute(insert_string, insert_dict)
        self.conn.commit()
        return self.cursor.lastrowid

    def variant_insert(self, site, name, ref, alt, misc):
        """Insert a row into variant. Return id of inserted row."""
        insert_string = "INSERT INTO variant VALUES (:id, :site, \
            :name, :ref, :alt, :misc, :create_date, :update_date)"
        time = self._time()
        insert_dict = dict(id=None, site=site, name=name, 
            ref=ref, alt=alt, misc=json.dumps(misc), 
            create_date=time, update_date=time)
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
    meta_row = db.metadata_insert(filename="myfile", misc=dict(FORMATS="blahblah", INFOS="bloobloo"))
    site_row = db.site_insert(metadata=meta_row, accession="rf8320d", position=12324, site_id="ggsgdg", misc=dict(CHROM=2))
    variant_row = db.variant_insert(site=site_row, name="NA001", ref="A", alt="G", misc=dict(called=True, phased=False))
    print "meta", meta_row
    print "site", site_row
    print "variant", variant_row
