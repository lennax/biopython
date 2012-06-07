import json

try:
    from cyvcf import Reader, Writer
except ImportError:
    from vcf import Reader, Writer

from VariantDB import VariantSqlite


class Pyvcf2db(object):
    def __init__(self, database, filename, compressed=False,
                 prepend_chr=False):
        self.db = database

        # XXX When should this handle be closed?
        handle = open(filename, "rb")
        self._parser = Reader(fsock=handle, compressed=compressed,
                             prepend_chr=prepend_chr)

        site_cols = [col[0] for col in self.db.schema['site']]
        print "site info to add"
        for info in self._parser.infos.itervalues():
            if info.id not in site_cols:
                print info.id, info.num, info.type, info.desc
        variant_cols = [col[0] for col in self.db.schema['variant']]
        print "variant info to add"
        for fmt in self._parser.formats.itervalues():
            if fmt.id not in variant_cols:
                print fmt.id, fmt.num, fmt.type, fmt.desc
        # Store info in db
        file_data = json.dumps(dict(
            filters = self._parser.filters,
            formats = self._parser.formats,
            infos = self._parser.infos,
            metadata = self._parser.metadata,
        ))
        self.metadata = db.insert_commit(table='metadata',
                                  filename=filename, misc=file_data)

    def next(self):
        self._insert_row(self._parser.next())
        db.conn.commit()

    def parse_all(self):
        for row in self._parser:
            self._insert_row(row)
        db.conn.commit()

    def _insert_row(self, row):
        site_dict = dict(
            metadata = self.metadata,
            chrom = row.CHROM,
            position = row.POS,
            accession = None,  # I think this is ##reference
            site_id = row.ID,
            ref = row.REF,
            filter = row.FILTER,
            qual = row.QUAL,
            NS = row.INFO.get('NS'),
            DP = row.INFO.get('DP'),
            AA = row.INFO.get('AA'),
            DB = row.INFO.get('DB'),
            H2 = row.INFO.get('H2'),
        )
        site_id = db.insert_commit(table='site', **site_dict)
        alleles = []
        for num, allele in enumerate(row.ALT):
            AF_list = row.INFO.get('AF')
            try:
                AF = AF_list[num]
            except TypeError:
                AF = None
            alt_dict = dict(
                alt_id = num+1,
                site = site_id,
                alt = allele,
                AF = AF,
            )
            alleles.append(alt_dict)

        db.insert_many(table='alt', row_iter=alleles)

        samples = []
        for samp in row.samples:
            if samp.called == False:
                continue
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
    #parser.parse_all()
    
