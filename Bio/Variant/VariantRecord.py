
class Parser(object):
    """
    Object to store a generic variant
    

    """
    def __init__(self, filename):
        self.filename = filename
        # open file
        # store header info in object

    def next(self):
        """Parse next row of file"""
        # each VCF row goes into a Site
        pass

    def fetch(self, index, num=1):
        """Fetch num rows out of an indexed file"""
        pass

class Site(object):
    def __init__(self, start, chrom=None, **data):
        self.start = start
        # Init other site-related details

        # Create list of Alts at this site
        # XXX 0th Alt should be ref; seq "=" or None?
        # must differentiate between no change and unknown change
        self.alts = [Alt(ref=None, Alt=None)]

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
