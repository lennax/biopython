

from vcf import Reader

from Bio.SeqFeature import FeatureLocation
from variant import Variant, Genotype


class VCFAdapter(object):
    def __init__(self, filename):
        self.parser = Reader(filename=filename)
        # FIXME VCF does not explicity require an accession
        # best way to find the right one in a file?

    def next(self):
        row = self.parser.next()
        alts = self._fmt_alts(row.POS, row.REF, row.ALT)
        #print alts
        samples = self._fmt_samples(row.samples)

    def _fmt_samples(self, sample_list):
        for samp in sample_list:
            print samp

    def _fmt_alts(self, position, ref, alt_list):
        alts = []
        accession = "?"  # FIXME
        # VCF position is 1 based
        start = position - 1
        for alt in alt_list:
            end = start + len(alt) 
            location = FeatureLocation(start, end)
            alts.append(Variant(accession, location, ref, alt))

        return alts



if __name__ == "__main__":
    p = VCFAdapter("/Users/lenna/Python/PyVCF/vcf/test/walk_left.vcf")
    p.next()
