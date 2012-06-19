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

        # Store sample info in db
        samples = []
        self.sample_indexes = {}
        for id, sample in enumerate(self._parser.samples):
            samples.append(dict(id=id, metadata=self.metadata, sample=sample))
            self.sample_indexes[sample] = id
        db.insert_many(table='sample', row_iter=samples)

        self.scopes = ("INFO", "FORMAT")
        # get INFO tags stored in site table
        # cols 0-7 are fixed; last 2 cols are date and FK
        self.INFO_cols = [col[0] for col in self.db.schema['site'][8:-2]]
        # get FORMAT tags stored in call table
        # cols 0-2 are fixed; last 2 cols are date and FK
        self.FORMAT_cols = [col[0] for col in self.db.schema['call'][3:-2]]
        # Init empty dicts for storing arbitrary keys
        for scope in self.scopes:
            for dict_name in ("extra_%s", "%s_A", "%s_G"):
                setattr(self, dict_name % scope, {})
        # Store reserved A keys
        self.default_INFO_A = ['AC', 'AF']
        # Associate key lists with tables
        # FIXME this is still kind of nasty; tied to _find_key
        self.INFO_tables = dict(default_keys='site', new_keys='site_info',
                                A_keys='alt', G_keys='')  # FIXME
        self.FORMAT_tables = dict(default_keys='call',
                                  new_keys='call_format',
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
            metadata = self.metadata,
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
            pos = row.POS,
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
                base = {'key': key_id}
                if isinstance(value, list):
                    for item in value:
                        extra_sites.append(dict(base, value=item))
                else:
                    extra_sites.append(dict(base, value=value))
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
        # Dict for default INFOs that are per-A
        default_allele_infos = {}
        for key in self.default_INFO_A:
            default_allele_infos[key] = row.INFO.get(key)
        for num, allele in enumerate(row.ALT):
            alt_dict = dict(
                alt_index = num + 1,
                site = site_id,
                alt = allele,
            )
            # Set default per-A values
            for key in self.default_INFO_A:
                value = default_allele_infos[key]
                try:
                    alt_dict[key] = value[num]
                except TypeError:
                    alt_dict[key] = None
            alleles.append(alt_dict)

        db.insert_many(table='alt', row_iter=alleles)

        # Organize extra alt info
        # FIXME have to do all this in the ALT loop
        #       unless I change the alt FK in alt_info to be alt_id
        #       makes joins a little uglier but insert easier
        allele_infos = {}
        for key in self.INFO_A.iterkeys():
            allele_infos[key] = row.INFO.get(key)


        # Organize and insert call/genotype info
        # TODO handle arbitrary ##FORMAT
        calls = []
        extra_calls = []
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
            # Retrieve sample index
            sample_id = self.sample_indexes[samp.sample]
            call_dict = dict(
                site = site_id,
                sample = sample_id,
                GT = samp.gt_nums,
                DP = samp.data.get('DP'),
                FT = samp.data.get('FT'),
                GQ = samp.data.get('GQ'),
                HQ1 = HQ1,
                HQ2 = HQ2,
            )
            # FIXME probably also want phased, gt_bases
            calls.append(call_dict)

            for samp_k, samp_v in samp.data.iteritems():
                if self._find_key('FORMAT', samp_k) is None:
                    self._add_key('FORMAT', samp_k)
                table, key_id = self._find_key('FORMAT', samp_k)
                if table == 'call':
                    continue  # default keys already added
                elif table == 'call_format':
                    base = {'site': site_id, 'sample': sample_id, 'key': key_id}
                    if isinstance(samp_v, list):
                        for item in samp_v:
                            extra_calls.append(dict(base, value=item))
                    else:
                        extra_calls.append(dict(base, value=samp_v))

             # TODO have to add site.id and sample.id to the dicts

        # XXX insert_many precludes arbitrary format (need id)
        # XXX unless I use and trust an internal row counter
        #     or make samples table (e.g. id=1 sampname=NA0001)
        #     and have call_format use site and sampname instead of call id
        db.insert_many(table='call', row_iter=calls)
        db.insert_many(table='call_format', row_iter = extra_calls)


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
