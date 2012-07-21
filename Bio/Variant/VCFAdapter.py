import warnings

from vcf import Reader

from Bio.SeqFeature import FeatureLocation
from variant import Variant


class VCFAdapter(object):
    def __init__(self, filename):
        self.parser = Reader(filename=filename)
        # FIXME VCF does not explicity require an accession
        # best way to find the right one in a file?

        # XXX note: requires Reader._parse_alt to be a static method
        def as_AltRecord(self):
            from vcf import Reader
            return Reader._parse_alt(str(self.post))

        Variant.as_AltRecord = as_AltRecord

    def __str__(self):
        # TODO make str
        pass

    def next(self):
        row = self.parser.next()
        alts = []
        accession = "?"  # FIXME
        # VCF position is 1 based
        start = row.POS - 1
        for alt in row.ALT:
            end = start + len(alt)
            loc = FeatureLocation(start, end)
            alts.append(Variant(accession, loc, row.REF, alt, row.var_type))
        return row, alts


if __name__ == "__main__":
    p = VCFAdapter("/Users/lenna/Python/PyVCF/vcf/test/walk_left.vcf")
    while True:
        try:
            row, alts = p.next()
        except TypeError:
            break
        print row
        print row.var_type
        #print row.var_subtype
        print alts
        #alt_rec = VCFAdapter.as_AltRecord(alts[0])
        alt_rec = alts[0].as_AltRecord()
        print type(alt_rec), alt_rec
        #samp = row.samples[0]
