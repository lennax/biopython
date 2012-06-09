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
        file_data = json.dumps(dict(
            filters = self._parser.filters,
            formats = self._parser.formats,
            infos = self._parser.infos,
            metadata = self._parser.metadata,
        ))
        self.metadata = db.insert_commit(table='metadata',
                                  filename=filename, misc=file_data)

        # get info tags stored in site table
        self.site_cols = [col[0] for col in self.db.schema['site'][9:-3]]
        self.extra_site = {}
        # check whether file contains unknown ##INFO fields
        for info in self._parser.infos.itervalues():
            if info.id not in self.site_cols and info.id != "AF":
                # store unknown ##INFO fields in key table
                new_id = self._add_key(info)
                self.extra_site[info.id] = new_id

        # get format tags stored in variant table
        self.variant_cols = [col[0] for col in self.db.schema['variant'][3:-3]]
        self.extra_variant = {}
        # check whether file contains unknown ##FORMAT fields
        for fmt in self._parser.formats.itervalues():
            if fmt.id not in self.variant_cols:
                # store unknown ##FORMAT fields in key table
                new_id = self._add_key(fmt)
                self.extra_variant[fmt.id] = new_id

    def next(self):
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

    def _add_key(self, field):
        """Add unknown keys to key table, return id"""
        insert_dict = dict(
            key = field.id,
            number = field.num,
            type = field.type,
            description = field.desc,
        )
        return db.insert_commit(table='key', **insert_dict)

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
            accession = None,  # I think this is ##reference
            site_id = row.ID,
            ref = row.REF,
            filter = row.FILTER,
            qual = row.QUAL,
        )

        # XXX would it make more sense to set default ones all at once
        # then do `if key not in self.site_cols:` ?
        for key in row.INFO.iterkeys():
            if key in self.site_cols:
                site_dict[key] = row.INFO.get(key)  # don't tempt fate
            else:
                try:
                    key_id = self.extra_site[key]
                except KeyError:
                    key_id = db.insert_commit(table='key',
                        key=key, number=None,
                        type=None, description=None)
                    # TODO add key to key table
                # TODO add INFO pair to narrow table
                # need site_id for this insert
                # so have to store these somewhere and insert later
                #db.insert_row(table='site_info', )

        # Set any site info keys that were missing to None
        for key in self.site_cols:
            if key not in site_dict:
                site_dict[key] = None

        site_id = db.insert_commit(table='site', **site_dict)
        # Organize and insert allele/alt info
        alleles = []
        for num, allele in enumerate(row.ALT):
            # Try to get AF from row INFO
            AF_list = row.INFO.get('AF')
            try:
                AF = AF_list[num]
            except TypeError:
                AF = None
            alt_dict = dict(
                alt_id = num + 1,
                site = site_id,
                alt = allele,
                AF = AF,
            )
            alleles.append(alt_dict)

        db.insert_many(table='alt', row_iter=alleles)

        # Organize and insert sample/genotype info
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
                GQ = samp.data.get('GQ'),
                DP = samp.data.get('DP'),
                HQ1 = HQ1,
                HQ2 = HQ2,
            )
            # FIXME probably also want phased, gt_bases
            samples.append(variant_dict)

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
    #parser.next()
    parser.parse_all()
