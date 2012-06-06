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
            accession = None,  # I think this is ##reference
            position = row.POS,
            site_id = row.ID,
            chrom = row.CHROM,
            filter = row.FILTER,
            qual = row.QUAL,
        )
        site_dict['misc'] = json.dumps(dict(
            info = row.INFO,
            #sample_indexes = row._sample_indexes,  # FIXME cyvcf error
            alleles = json.dumps(row.alleles),  # etc
        ))
        site_id = db.insert_commit(table='site', **site_dict)

        for samp in row.samples:
            if samp.called == False:
                continue
            variant_dict = dict(
                site = site_id,
                name = samp.sample,
                ref = row.REF,  # XXX is this correct?
                alt = samp.gt_bases,  # ditto
            )
            variant_dict['misc'] = json.dumps(dict(
                type = samp.gt_type,
                called = samp.called,
                data = samp.data,
                is_get = samp.is_het,
                is_variant = samp.is_variant,
                phased = samp.phased
            ))

            db.insert_row(table='variant', **variant_dict)


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
    
