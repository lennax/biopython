



class Variant(object):
    def __init__(self, accession, location, pre, post, extra=None):
        self.accession = accession
        self.location = location
        self.pre = pre
        self.post = post
        self.extra = extra
        # For string representation
        self.str_name = "Variant"
        self.str_list = [self.accession, self.loc_str,
                           self.pre, str(self.post)]

    @property
    def start(self):
        return self.location.start

    @property
    def end(self):
        return self.location.end

    @property
    def loc_str(self):
        return "[%s,%s)" % ( self.start, self.end )

    def __str__(self):
        # use list to allow addition of arbitrary items to print representation
        return "{0}({1})".format(self.str_name, ", ".join(self.str_list))

    def __repr__(self):
        return self.__str__()


class Genotype(object):
    def __init__(self, parent, genotypes, phases, sample, extra=None):
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
        return "Genotype(sample={sample}, Data('GT': {GT}{extra}))".format(
            sample=self.sample,
            GT = self.GT_string,
            extra = ", " + str(self.extra)[1:-1]
        )

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



if __name__ == "__main__":
    from Bio.SeqFeature import FeatureLocation
    test_var = Variant("DNAaccession", FeatureLocation(303, 304), "G", "C")
    print test_var
