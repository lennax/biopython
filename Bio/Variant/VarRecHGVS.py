sample_HGVS = "NM_004006.1:c.3G>T"

class Parser(object):
    """
    Object to store a generic variant
    

    """
    def __init__(self, filename):
        self.filename = filename
        # open file
        # store header info in object
        self.accession = "NM_004006.1"
        self.seq_type = "coding"

    def next(self):
        """Parse next row of file"""
        # each VCF row goes into a Site
        # XXX how to represent unspecified del/dup?
        return Site(start=3, ref="G", alts="T")

    def fetch(self, index, num=1):
        """Fetch num rows out of an indexed file"""
        pass

class Site(object):
    def __init__(self, start, chrom=None, ref=None, alts=None, **data):
        self.start = start
        # Init other site-related details

        # Create list of Alts at this site
        # XXX 0th Alt should be ref; seq "=" or None?
        # must differentiate between no change and unknown change
        self.alts = [Alt(ref=None, Alt=None)]
        for alt in alts.split(","):
            self.alts.append(Alt(ref=ref, alt=alt))

        # Then create list of Samples at this site

class Alt(object):
    def __init__(self, ref, alt):
        self.ref = ref
        self.alt = alt
        # init other alt data
        # XXX depth may be defined differently per file

class Sample(object):
    def __init__(self, GT):
        # if GT == "0/1": self.genotype = [0, 1]; phased = False
        pass
