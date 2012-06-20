
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
        'file': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('file', 'TEXT'),
            ('parser', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
        ],
        'default_keys': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('file', 'INTEGER'),
            ('key', 'TEXT'),
            ('key_id', 'TEXT'),
            ('number', 'TEXT'),
            ('type', 'TEXT'),
            ('desc', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(file) REFERENCES file(id)'),
        ],
        'metadata': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('file', 'INTEGER'),
            ('key', 'TEXT'),
            ('value', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(file) REFERENCES file(id)'),
        ],
        'sample': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('file', 'INTEGER'),
            ('sample', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(file) REFERENCES file(id)'),
        ],
        'site': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('file', 'INTEGER'),
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
            ('FOREIGN KEY', '(file) REFERENCES file(id)'),
        ],
        'alt': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('alt_index', 'INTEGER'),
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
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
            ('FOREIGN KEY', '(sample) REFERENCES sample(id)'),
        ],
        'key': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('file', 'INTEGER'),
            ('scope', 'TEXT'),
            ('key', 'TEXT'),
            ('number', 'TEXT'),
            ('type', 'TEXT'),
            ('description', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(file) REFERENCES file(id)'),
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
        # FIXME how to query this
        'alt_info': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('site', 'INTEGER'),
            ('sample', 'INTEGER DEFAULT NULL'),
            ('alt_index', 'INTEGER'),
            ('key', 'INTEGER'),
            ('value', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
            ('FOREIGN KEY', '(sample) REFERENCES sample(id)'),
            ('FOREIGN KEY', '(key) REFERENCES key(id)'),
        ],
        'call_format': [
            ('id', 'INTEGER PRIMARY KEY'),
            ('site', 'INTEGER'),
            ('sample', 'INTEGER'),
            ('key', 'INTEGER'),
            ('value', 'TEXT'),
            ('update_date', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
            ('FOREIGN KEY', '(site) REFERENCES site(id)'),
            ('FOREIGN KEY', '(sample) REFERENCES sample(id)'),
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

    @abstractmethod
    def write_vcf(self, filepath,
                  file_filter=None, site_filter=None, call_filter=None):
        """
        Write from DB to VCF.

        """
        raise NotImplementedError


class VariantSqlite(VariantDB):
    """
    Sqlite3 interface for variant storage.

    """

    def __init__(self, dbname=None):
        """Connect to the database and create the tables."""
        # Call parent constructor to create schema lists
        VariantDB.__init__(self, dbname=dbname)
        if dbname is None:
            dbname = "variant.db"
        self.conn = sqlite3.connect(dbname)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA synchronous=OFF")
        for stmt in self.create_stmt.values():
            self.cursor.execute(stmt)
        self.conn.commit()

    def __del__(self):
        """Try to close the database connection"""
        try:
            # Final commit to be sure
            self.conn.commit()
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
        """Insert multiple rows; provide an iterable of insert dicts"""
        insert_string = self.ins_stmt[table]
        self.cursor.executemany(insert_string, row_iter)

    def query(self, query):
        """Run query on DB; return row factory."""
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        #for row in results:
            #print row
        return results

    def write_vcf(self, filepath,
                  file_id=None, site_filter=None, call_filter=None):
        if file_id is None:
            file_id = 1
        metadata_qs = 'SELECT key, value FROM metadata WHERE file={0}'
        metadata = self.query(metadata_qs.format(file_id))
        for result in metadata:
            print "##%s=%s" % (result['key'], result['value'])

        h_qs = 'SELECT key, key_id, number, type, desc FROM default_keys \
                WHERE file={0}'
        header = self.query(h_qs.format(file_id))

        def swap_num(result):
            old = result['number']
            numbers = {'-2': 'G', '-1': 'A', None: '.'}
            if old in numbers:
                return numbers[old]
            else:
                return int(old)

        sub_list = ['##{0[key]!s}=<ID={0[key_id]!s},',
                       'Description="{0[desc]!s}">']
        short_str = "".join(sub_list)
        sub_list.insert(1, 'Number={1!s},Type={0[type]!s},')
        long_str = "".join(sub_list)
        sub_strs = {"INFO": long_str, "FORMAT": long_str,
                    "ALT": short_str, "FILTER": short_str}
        for result in header:
            sub_str = sub_strs[result['key']]
            print sub_str.format(result, swap_num(result))

        vcf_header = "#CHROM POS ID REF ALT QUAL FILTER INFO FORMAT".split()
        sample_qs = 'SELECT sample FROM sample WHERE file={0}'
        sample_q = self.query(sample_qs.format(file_id))
        samples = [r['sample'] for r in sample_q]
        print "\t".join(vcf_header + samples)

        site_cols = [c[0] for c in self.schema['site'][2:-2]]
        call_cols = [c[0] for c in self.schema['call'][3:-3]]
        site_q = self.query('SELECT id, {0} FROM site WHERE \
                            file={1}'.format(', '.join(site_cols), file_id))
        for site_row in site_q:
            # FIXME need to differentiate between missing and "." in the file
            print site_row
            si_qs = 'SELECT k.key, si.value FROM key AS k, site_info AS si \
                    WHERE si.site={0} AND si.key=k.id'
            site_info_q = self.query(si_qs.format(site_row['id']))
            for info in site_info_q:
                print info

            call_qs = 'SELECT sample, {0} FROM call WHERE site={1}'
            call_q = self.query(call_qs.format(', '.join(call_cols),
                                               site_row['id']))
            for call_row in call_q:
                print call_row
                c_qs = 'SELECT k.key, c.value FROM key AS k, call_format AS c \
                      WHERE c.site={0} AND c.sample={1} AND c.key = k.id'
                call_fmt_q = self.query(c_qs.format(site_row['id'],
                                                    call_row['sample']))
                for fmt in call_fmt_q:
                    print fmt


if __name__ == "__main__":
    db = VariantSqlite("test.db")
    file_row = db.insert_commit(
        table="file",
        file="myfile",
        parser="FakeParser",
    )
    meta_row = db.insert_commit(
        table="metadata",
        file=file_row,
        key="fileformat",
        value="VCFv4.0",
    )
    db.insert_commit(
        table="default_keys",
        file=file_row,
        key="ALT",
        key_id="DEL",
        number=None,
        type=None,
        desc="Deletion"
    )
    db.insert_commit(
        table="default_keys",
        file=file_row,
        key="FORMAT",
        key_id="GT",
        number=1,
        type="String",
        desc="Sample genotype"
    )
        #alts='{"DEL": ["DEL", "Deletion"]}',
        #filters=None,
        #formats='{"GT": ["GT", 1, "String", "Genotype"]}',
        #infos='{"AC": ["AC", null, "Integer", "Allele count in genotypes, for each ALT allele, in the same order as listed"]}',
        #metadata='{"fileformat": "VCFv4.0", "contig": "<ID=chrY,length=39584842,assembly=hg19>"}',
    #)

    db.insert_commit(table='sample', file=file_row, sample="NA001")
    site_row = db.insert_commit(
        table="site",
        file=file_row,
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
        sample=1,
        GT="0|0",
        DP=2,
        FT=None,
        GQ=34,
    )
    db.conn.commit()
    print "meta", meta_row
    print "site", site_row
    print "call", call_row

    t_qs = "SELECT site.chrom, site.pos, call.GT FROM site, call \
            WHERE site.id = call.site"
    test_query = db.query(t_qs)
    for row in test_query:
        print row

    test_write = VariantSqlite('walk.db')  # small output from walk_left.vcf
    test_write.write_vcf(filepath = 'walk.vcf')
