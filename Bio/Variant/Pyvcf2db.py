import json

try:
    from cyvcf import Reader, Writer
except ImportError:
    from vcf import Reader, Writer

from VariantDB import VariantSqlite


class Pyvcf2db(object):
    def __init__(self, database, filename, compressed=False,
                 prepend_chr=False):
        """
        Given a variant database object and a file:
            - opens file
            - inits parser
            - stores file info in database
            - reads file headers for unknown ##INFO or ##FORMAT

        """
        self.db = database

        # XXX When should this handle be closed?
        handle = open(filename, "rb")
        self._parser = Reader(fsock=handle, compressed=compressed,
                             prepend_chr=prepend_chr)

        # Store file info in db
        # TODO remove json
        file_data = json.dumps(dict(
            filters = self._parser.filters,
            formats = self._parser.formats,
            infos = self._parser.infos,
            metadata = self._parser.metadata,
        ))
        self.metadata = db.insert_commit(table='metadata',
                                  filename=filename, misc=file_data)

        self.scopes = ("INFO", "FORMAT")
        # get INFO tags stored in site table
        # cols 0-8 are fixed; last 3 cols are dates and FK
        self.INFO_cols = [col[0] for col in self.db.schema['site'][9:-3]]
        # get FORMAT tags stored in variant table
        # cols 0-2 are fixed; last 3 cols are dates and FK
        self.FORMAT_cols = [col[0] for col in self.db.schema['variant'][3:-3]]
        # Init empty dicts for storing arbitrary keys
        for scope in self.scopes:
            for dict_name in ("extra_%s", "%s_A", "%s_G"):
                setattr(self, dict_name % scope, {})
        # Store reserved A keys
        self.INFO_A['AC'] = None
        self.INFO_A['AF'] = None
        # Associate key lists with tables
        # FIXME this is still kind of nasty; tied to _find_key
        self.INFO_tables = dict(default_keys='site', new_keys='site_info',
                                A_keys='alt', G_keys='')  # FIXME
        self.FORMAT_tables = dict(default_keys='variant',
                                  new_keys='variant_info',
                                  A_keys='', G_keys='')  # FIXME

        # Scan header ##INFO and ##FORMAT lines for new keys
        self._scan_headers()

    def parse_next(self):
        """Read one row into database"""
        # call next() on parser
        row = self._parser.next()
        # insert row and commit
        self._insert_row(row)
        db.conn.commit()

    def parse_all(self):
        """Read entire file into database"""
        # loop through parser and insert
        for row in self._parser:
            self._insert_row(row)
        db.conn.commit()

    def _scan_headers(self):
        for scope in self.scopes:
            key_iter = getattr(self._parser, "%ss" % scope.lower()).itervalues()
            for field in key_iter:
                if self._find_key(scope, field.id) is None:
                    self._add_key(scope, *field)
                    # XXX ** causes error; could use _as_dict() but new obj

    def _find_key(self, scope, key):
        """Look for key; if found return table, else None"""
        if scope not in self.scopes:
            raise TypeError("Unknown key scope '%s'" % scope)
        key_lists = {
            'default_keys': getattr(self, "%s_cols" % scope),
            'new_keys': getattr(self, "extra_%s" % scope),
            'A_keys': getattr(self, "%s_A" % scope),
            'G_keys': getattr(self, "%s_G" % scope),
        }
        tables = getattr(self, "%s_tables" % scope)
        for name, key_list in key_lists.iteritems():
            if key in key_list:
                if name == 'default_keys':
                    return (tables[name], None)
                return (tables[name], key_list[key])
        else:  # key was not found on any list
            return None

    def _add_key(self, scope, key_id, num=None, key_type=None, desc=None):
        """Add unknown keys to key table, return id"""
        if scope not in self.scopes:
            raise TypeError("Unknown key scope '%s'" % scope)
        insert_dict = dict(
            scope = scope,
            key = key_id,
            number = num,
            type = key_type,
            description = desc,
        )
        new_id = db.insert_commit(table='key', **insert_dict)
        new_keys = getattr(self, "extra_%s" % scope)
        A_keys = getattr(self, "%s_A" % scope)
        G_keys = getattr(self, "%s_G" % scope)
        if num == "A":
            A_keys[key_id] = new_id
        elif num == "G":
            G_keys[key_id] = new_id
        else:
            new_keys[key_id] = new_id

    def _insert_row(self, row):
        """
        PRIVATE
        Insert a row into database.
        Note: does not commit.

        """
        # Organize and insert site/row/record info
        site_dict = dict(
            metadata = self.metadata,
            chrom = row.CHROM,
            position = row.POS,
            accession = None,  # FIXME I think this is ##reference
            site_id = row.ID,
            ref = row.REF,
            filter = row.FILTER,
            qual = row.QUAL,
        )

        # Set default site info keys
        for key in self.INFO_cols:
            # get() will set missing to None
            site_dict[key] = row.INFO.get(key)

        # Organize extra site info
        extra_sites = []
        # Loop through keys in file
        for key, value in row.INFO.iteritems():
            if self._find_key('INFO', key) is None:
                self._add_key('INFO', key)
            table, key_id = self._find_key('INFO', key)
            if table == 'site':
                continue  # default keys already added
            elif table == 'site_info':
                if isinstance(value, list):
                    for item in value:
                        extra_sites.append(dict(key=key_id, value=item))
                else:
                    extra_sites.append(dict(key=key_id, value=value))
            else:
                # FIXME 1. can an INFO ever be G?
                # 2. How to get A to allele?
                pass

        site_id = db.insert_commit(table='site', **site_dict)

        # Insert extra site info
        for item in extra_sites:
            item['site'] = site_id
        db.insert_many(table='site_info', row_iter=extra_sites)

        # Organize and insert allele/alt info
        # TODO for 4.1 put number = A here
        alleles = []
        for num, allele in enumerate(row.ALT):
            # Try to get AF from row INFO
            AF_list = row.INFO.get('AF')
            try:
                AF = AF_list[num]
            except TypeError:
                AF = None
            AC_list = row.INFO.get('AC')
            try:
                AC = AC_list[num]
            except TypeError:
                AC = None
            alt_dict = dict(
                alt_id = num + 1,
                site = site_id,
                alt = allele,
                AC = AC,
                AF = AF,
            )
            alleles.append(alt_dict)

        db.insert_many(table='alt', row_iter=alleles)

        # Organize and insert sample/genotype info
        # TODO handle arbitrary ##FORMAT
        samples = []
        for samp in row.samples:
            # Don't insert genotype that wasn't called XXX ?
            if samp.called == False:
                continue
            # Divide HQ pair or set to None
            HQ = samp.data.get('HQ')
            try:
                HQ1, HQ2 = HQ
            except TypeError:
                HQ1 = HQ2 = None
            variant_dict = dict(
                site = site_id,
                name = samp.sample,
                GT = samp.gt_nums,
                DP = samp.data.get('DP'),
                FT = samp.data.get('FT'),
                GQ = samp.data.get('GQ'),
                HQ1 = HQ1,
                HQ2 = HQ2,
            )
            # FIXME probably also want phased, gt_bases
            samples.append(variant_dict)

        # XXX insert_many precludes arbitrary format (need id)
        # XXX unless I use and trust an internal row counter
        db.insert_many(table='variant', row_iter=samples)


if __name__ == "__main__":
    import sys
    import os
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python script.py test_file")

    filename = sys.argv[1]
    compressed = False
    if os.path.splitext(filename)[1] == ".gz":
        compressed = True
    db = VariantSqlite("vcftest.db")
    parser = Pyvcf2db(database=db, filename=filename, compressed=compressed)
    #parser.parse_next()
    parser.parse_all()
