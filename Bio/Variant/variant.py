

from Bio.SeqFeature import FeatureLocation


class Variant(object):
    def __init__(self, accession, location, pre, post):
        self.accession = accession
        self.location = location
        self.pre = pre
        self.post = post
        # For string representation
        self.str_name = "Variant"
        self.str_list = [self.accession, self.loc_str,
                           self.pre, self.post]

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

if __name__ == "__main__":
    test_var = Variant("DNAaccession", FeatureLocation(303, 304), "G", "C")
    print test_var
