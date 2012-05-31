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
        file_data = dict(
            filters = self._parser.filters,
            formats = self._parser.formats,
            infos = self._parser.infos,
            metadata = self._parser.metadata,
        )
        db.metadata_insert(filename=filename, misc=file_data)

    def next(self):
        row = self._parser.next()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python script.py test_file")

    filename = sys.argv[1]
    db = VariantSqlite("vcftest.db")
    parser = Pyvcf2db(database=db, filename=filename)

