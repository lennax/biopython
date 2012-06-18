
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
        self.create_stmt = dict()
        self.ins_stmt = dict()
        for table, col_list in self.schema.iteritems():
            self.create_stmt[table] = "CREATE TABLE IF NOT EXISTS \
            %s (%s)" % (
                table,
                ", ".join((" ".join(item) for item in col_list))
            )

            ins_iter = [x[0] for x in col_list if x[0] not in (
                "id", "update_date", "FOREIGN KEY")]
            self.ins_stmt[table] = "INSERT INTO %s (%s) VALUES (%s)" % (
                table,
                ", ".join(ins_iter),
                ", ".join(("".join((":", x)) for x in ins_iter))
            )

    # schema definitions
    schema = {
        'metadata': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('filename', 'TEXT'),
            ('misc', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
        ],
        'sample': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('metadata', 'INTEGER'),
            ('sample', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(metadata) REFERENCES metadata(id)'),
        ],
        'site': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('metadata', 'INTEGER'),
            ('chrom', 'TEXT'),
            ('pos', 'INTEGER'),
            ('site_id', 'TEXT'),
            ('ref', 'TEXT'),
            ('filter', 'TEXT'),
            ('qual', 'TEXT'),
            # Reserved INFO in vcf 4.0
            ('AA', 'TEXT'),
            ('AN', 'INTEGER'),
            ('BQ', 'FLOAT'),
            ('CIGAR', 'TEXT'),
            ('DB', 'INTEGER'),  # bool
            ('DP', 'INTEGER'),
            ('END', 'INTEGER'),
            ('H2', 'INTEGER'),  # bool
            ('MQ', 'FLOAT'),
            ('MQ0', 'INTEGER'),
            ('NS', 'INTEGER'),
            ('SB', 'TEXT'),
            ('SOMATIC', 'INTEGER'),  # bool
            ('VALIDATED', 'INTEGER'),  # bool
            # Some vcf 4.1 additions
            ('H3', 'INTEGER'),  # bool
            ('THOUSANDG', 'INTEGER'),  # bool; == 1000G
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(metadata) REFERENCES metadata(id)'),
        ],
        'alt': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('alt_id', 'INTEGER'),
            ('site', 'INTEGER'),
            ('alt', 'TEXT'),
            # reserved info keys that map better to allele (number=A)
            ('AC', 'INTEGER'),
            ('AF', 'FLOAT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
        ],
        'call': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('site', 'INTEGER'),
            ('sample', 'INTEGER'),
            # reserved format keys from vcf 4.0 (GT must be first)
            ('GT', 'TEXT'),
            ('DP', 'INTEGER'),
            ('FT', 'TEXT'),
            ('GQ', 'INTEGER'),
            ('HQ1', 'INTEGER'),
            ('HQ2', 'INTEGER'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
            ('FOREIGN KEY', '(sample) REFERENCES sample(id)'),
        ],
        'key': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('metadata', 'INTEGER'),
            ('scope', 'TEXT'),
            ('key', 'TEXT'),
            ('number', 'TEXT'),
            ('type', 'TEXT'),
            ('description', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(metadata) REFERENCES metadata(id)'),
        ],
        'site_info': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('site', 'INTEGER'),
            ('key', 'INTEGER'),
            ('value', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
            ('FOREIGN KEY', '(key) REFERENCES key(id)'),
        ],
        'alt_info': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('alt', 'INTEGER'),
            ('call', 'INTEGER DEFAULT NULL'),
            ('key', 'INTEGER'),
            ('value', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(alt) REFERENCES alt(id)'),
            ('FOREIGN KEY', '(call) REFERENCES call(id)'),
            ('FOREIGN KEY', '(key) REFERENCES key(id)'),
        ],
        'call_format': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('call', 'INTEGER'),
            ('key', 'INTEGER'),
            ('value', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(call) REFERENCES call(id)'),
            ('FOREIGN KEY', '(key) REFERENCES key(id)'),
        ],
    }

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
        raise NotImplementedError


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
        self.cursor.execute("PRAGMA synchronous=OFF")
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
        """Insert a row into a table. NOTE: does not commit cursor!"""
        insert_string = self.ins_stmt[table]
        self.cursor.execute(insert_string, insert_dict)

    def insert_many(self, table, row_iter):
        """Insert multiple rows; provide an iterable of dicts"""
        insert_string = self.ins_stmt[table]
        self.cursor.executemany(insert_string, row_iter)

    def query(self, query):
        self.conn.row_factory = sqlite3.Row
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        for row in results:
            print row



if __name__ == "__main__":
    db = VariantSqlite("test.db")
    meta_row = db.insert_commit(
        table="metadata",
        filename="myfile",
        misc=json.dumps(dict(FORMATS="blahblah", INFOS="bloobloo"))
    )
    site_row = db.insert_commit(
        table="site",
        metadata=meta_row,
        chrom=2,
        pos=12324,
        site_id="ggsgdg",
        ref="G",
        filter='q10',
        qual=22,
        AA="G",
        AN=None,
        BQ=None,
        CIGAR=None,
        DB=False,
        DP=2,
        END=None,
        H2=False,
        MQ=None,
        MQ0=None,
        NS=3,
        SB=None,
        SOMATIC=None,
        VALIDATED=None,
        H3=None,
        THOUSANDG=None,
    )
    call_row = db.insert_row(
        table="call",
        site=site_row,
        name="NA001",
        GT="0|0",
        DP=2,
        FT=None,
        GQ=34,
        HQ1=25,
        HQ2=35,
    )
    db.conn.commit()
    print "meta", meta_row
    print "site", site_row
    print "call", call_row

    db.query("SELECT site.chrom, site.pos, call.GT FROM site, call WHERE site.id = call.site")
