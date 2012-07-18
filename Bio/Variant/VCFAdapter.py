import warnings

from vcf import Reader

from Bio.SeqFeature import FeatureLocation
from variant import Variant


class VCFRow(object):
    def __init__(self, alts):
        self.alts = alts

    @property
    def samples(self):
        return self._samples

    @samples.setter
    def samples(self, val):
        self._samples = val

    def __str__(self):
        string_list = [self.alts]
        for samp in self.samples:
            string_list.append(samp)
        return "\n".join([str(x) for x in string_list])


class VCFGenotype(object):
    def __init__(self, parent, genotypes, phases, sample, **extra):
        # Reference to the parent VCFRow
        self.parent = parent
        # A pair of genotypes will have one phase, etc.
        assert len(genotypes) == len(phases) + 1
        self.genotypes = genotypes
        self.phases = phases
        # representing VCF requires work from the VCF parser/adapter
        # to keep track of what number means what
        self.sample = sample
        self.extra = extra

    def __str__(self):
        string = "Genotype(sample={sample}, Data('GT': {GT}))".format(
            sample=self.sample,
            GT=self.GT_string,
        )
        extras = []
        for k, v in self.extra.iteritems():
            extras.append("\n{0}: {1}".format(k, v))
        if extras:
            string = "".join([string] + extras)
        return string

    @property
    def GT_string(self, phase_sep="|", unphase_sep="/"):
        gt_list = [self.genotypes[0]]
        for gt, phase in zip(self.genotypes[1:], self.phases):
            if phase:
                gt_list.append(phase_sep)
            else:
                gt_list.append(unphase_sep)
            gt_list.append(gt)

        return "".join(gt_list)

    @property
    def GT_bases(self, phase_sep="|", unphase_sep="/"):
        def gt2base(index):
            if index == 0:
                # XXX will the pre always be the same?
                return str(self.parent.alts[0].pre)
            else:
                return str(self.parent.alts[int(index)].post)
        gt_list = [gt2base(self.genotypes[0])]
        for gt, phase in zip(self.genotypes[1:], self.phases):
            if phase:
                gt_list.append(phase_sep)
            else:
                gt_list.append(unphase_sep)
            gt_list.append(gt2base(gt))

        return "".join(gt_list)


class VCFAdapter(object):
    def __init__(self, filename):
        self.parser = Reader(filename=filename)
        # FIXME VCF does not explicity require an accession
        # best way to find the right one in a file?

    def next(self):
        row = self.parser.next()
        alts = self._fmt_alts(row.POS, row.REF, row.ALT)
        vcfrow = VCFRow(alts)
        samples = self._fmt_samples(vcfrow, row.samples)
        vcfrow.samples = samples
        #print samples[0].GT_string
        #print samples[0].GT_bases
        return vcfrow

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
                warnings.warn("Can't handle polyploid genotypes yet",
                              FutureWarning)

            #for k, v in samp.data._asdict().iteritems():
                #print k, v
            extra = dict((k, v) for k, v in samp.data._asdict().iteritems()
                         if k != "GT")
            #for k in samp.data._fields:
                #print k, getattr(samp.data, k)
            samples.append(VCFGenotype(
                parent, genotypes, phases, samp.sample, **extra))
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
    row = p.next()
    print row
