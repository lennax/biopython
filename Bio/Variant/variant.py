



class Variant(object):
    def __init__(self, accession, location, pre, post, **extra):
        self.accession = accession
        self.location = location
        self.pre = pre
        self.post = post
        self.extra = extra

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
        str_name = "Variant"
        str_list = [self.accession, self.loc_str, self.pre, str(self.post)]
        name = "{0}({1})".format(str_name, ", ".join(str_list))
        extras = []
        for k, v in self.extra.iteritems():
            extras.append("\n{0}: {1}".format(k, v))
        if extras:
            name = "".join([name] + extras)
        return name

    def __repr__(self):
        return self.__str__()


if __name__ == "__main__":
    from Bio.SeqFeature import FeatureLocation
    test_var = Variant("DNAaccession", FeatureLocation(303, 304), "G", "C")
    print test_var
    test_var_2 = Variant("DNAaccession", FeatureLocation(303, 304), "G", "C", ancestral='G')
    print test_var_2
