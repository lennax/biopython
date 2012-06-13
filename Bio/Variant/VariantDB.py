
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
            ('create_date', 'TIMESTAMP NOT NULL'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
        ],
        'site': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('metadata', 'INTEGER'),
            ('chrom', 'TEXT'),
            ('position', 'INTEGER'),
            ('accession', 'TEXT'),
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
            ('1000G', 'INTEGER'),  # bool
            ('create_date', 'TIMESTAMP NOT NULL'),
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
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
        ],
        'variant': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('site', 'INTEGER'),
            ('name', 'TEXT'),
            # reserved format keys from vcf 4.0 (GT must be first)
            ('GT', 'TEXT'),
            ('DP', 'INTEGER'),
            ('FT', 'TEXT'),
            ('GQ', 'INTEGER'),
            ('HQ1', 'INTEGER'),
            ('HQ2', 'INTEGER'),
            ('create_date', 'TIMESTAMP'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
        ],
        'key': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('key', 'TEXT'),
            ('number', 'TEXT'),
            ('type', 'TEXT'),
            ('description', 'TEXT'),
        ],
        'site_info': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('site', 'INTEGER'),
            ('key', 'INTEGER'),
            ('value', 'TEXT'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
            ('FOREIGN KEY', '(key) REFERENCES key(id)'),
        ],
        'variant_info': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('variant', 'INTEGER'),
            ('key', 'INTEGER'),
            ('value', 'TEXT'),
            ('FOREIGN KEY', '(variant) REFERENCES variant(id)'),
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
        insert_dict['create_date'] = datetime.now()
        self.cursor.execute(insert_string, insert_dict)

    def insert_many(self, table, row_iter):
        """Insert multiple rows; provide an iterable of dicts"""
        insert_string = self.ins_stmt[table]
        for insert_dict in row_iter:
            insert_dict['create_date'] = datetime.now()
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
        position=12324,
        accession="rf8320d",
        site_id="ggsgdg",
        ref="G",
        filter='q10',
        qual=22,
        NS=3,
        DP=2,
        AA="G",
        DB=False,
        H2=False,
    )
    variant_row = db.insert_row(
        table="variant",
        site=site_row,
        name="NA001",
        GT="0|0",
        GQ=34,
        DP=2,
        HQ1=25,
        HQ2=35,
    )
    db.conn.commit()
    print "meta", meta_row
    print "site", site_row
    print "variant", variant_row

    db.query("SELECT site.chrom, site.position, variant.GT FROM site, variant WHERE site.id = variant.site")
