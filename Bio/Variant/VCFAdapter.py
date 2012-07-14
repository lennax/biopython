import warnings

from vcf import Reader

from Bio.SeqFeature import FeatureLocation
from variant import Variant, Genotype


class VCFRow(object):
    def __init__(self, alts):
        self.alts = alts

    def __str__(self):
        # TODO make str
        pass

class VCFAdapter(object):
    def __init__(self, filename):
        self.parser = Reader(filename=filename)
        # FIXME VCF does not explicity require an accession
        # best way to find the right one in a file?

    def next(self):
        row = self.parser.next()
        alts = self._fmt_alts(row.POS, row.REF, row.ALT)
        #print alts
        vcfrow = VCFRow(alts)
        samples = self._fmt_samples(vcfrow, row.samples)
        vcfrow.samples = samples
        #for samp in samples:
            #print samp
        print samples[0].GT_string
        print samples[0].GT_bases

    def _fmt_samples(self, parent, sample_list):
        samples = []
        for samp in sample_list:
            phased = samp.data.GT.split("|")
            unphased = samp.data.GT.split("/")
            # FIXME this only works for diploid calls
            if len(phased) == 2:
                genotypes = phased
                phases = [True]
            elif len(unphased) == 2:
                genotypes = unphased
                phases = [False]
            else:
                warnings.warn("Can't handle polyploid genotypes yet", FutureWarning)

            #for k, v in samp.data._asdict().iteritems():
                #print k, v
            extra = dict((k, v) for k, v in samp.data._asdict().iteritems() if k != "GT")
            #for k in samp.data._fields:
                #print k, getattr(samp.data, k)
            samples.append(Genotype(parent, genotypes, phases, samp.sample, extra))
        return samples


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
