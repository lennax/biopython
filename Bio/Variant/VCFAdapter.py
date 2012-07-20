import warnings

from vcf import Reader

from Bio.SeqFeature import FeatureLocation
from variant import Variant


class VCFAdapter(object):
    def __init__(self, filename):
        self.parser = Reader(filename=filename)
        # FIXME VCF does not explicity require an accession
        # best way to find the right one in a file?

    def __str__(self):
        # TODO make str
        pass

    def next(self):
        row = self.parser.next()
        #alts = self._fmt_alts(row.POS, row.REF, row.ALT)
        alts = []
        accession = "?"  # FIXME
        # VCF position is 1 based
        start = row.POS - 1
        for alt in row.ALT:
            end = start + len(alt)
            location = FeatureLocation(start, end)
            alts.append(Variant(accession, location, row.REF, alt, row.var_type))
        return row, alts

    # FIXME if I make a Variant to PyVCF AltRecord method,
    # this class won't work because the constructor requires a filename
    # so should I have separate classes for each direction? 
    # or is it completely illogical to go back to PyVCF?


if __name__ == "__main__":
    p = VCFAdapter("/Users/lenna/Python/PyVCF/vcf/test/walk_left.vcf")
    while True:
        try:
            row, alts = p.next()
        except TypeError:
            break
        print row
        print row.var_type
        print row.var_subtype
        print alts
        #samp = row.samples[0]
