# Copyright 2012 Lenna X. Peterson (arklenna@gmail.com)
# All rights reserved
#
# This code is part of the Biopython distribution and governed by its
# license.  Please see the LICENSE file that should have been included
# as part of this package.

"""
Wrapper for the Reader and Writer classes of PyVCF.
"""

from Bio.SeqRecord import SeqRecord
from Bio.SeqFeature import SeqFeature
from Bio.SeqIO.Interfaces import SequenceIterator

try:
    from cyvcf import Reader, Writer
except ImportError:
    from vcf import Reader, Writer

class VarSeqRecord(SeqRecord):
    """
    This subclass of SeqRecord allows variant-related items to be retrieved
    from the annotations dictionary as class properties. 

    """
    def __init__(self, **kwargs):
        SeqRecord.__init__(self, **kwargs)

        self.allowed_record_properties = (
            "CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER",
            "INFO", "FORMAT", "alleles", "_sample_indexes",
        )

    def __getattr__(self, name):
        """
        After an AttributeError, __getattr__ is called. If the name matches
        an allowed property, this gets the name from the annotations dict.
        Otherwise, a class AttributeError is raised.

        """
        if name in self.allowed_record_properties:
            return self.annotations.get(name)
        else:
            raise AttributeError("Object '%s' has no attribute '%s'" % \
                                (self.__class__.__name__, name))

class VarSeqFeature(SeqFeature):
    """
    This subclass of SeqFeature allows variant-related items to be retrieved
    from the qualifiers dictionary as class properties.

    """
    def __init__(self, **kwargs):
        SeqFeature.__init__(self, **kwargs)

        self.allowed_feature_properties = (
            "called", "data", "gt_bases", "gt_type", "is_het", "is_variant",
            "phased", "sample", 
        )

    def __getattr__(self, name):
        """
        After an AttributeError, __getattr__ is called. If the name matches
        an allowed property, this gets the name from the qualifiers dict.
        Otherwise, a class AttributeError is raised.

        """
        if name in self.allowed_feature_properties:
            return self.qualifiers.get(name)
        else:
            raise AttributeError("Object '%s' has no attribute '%s'" % \
                                (self.__class__.__name__, name))



class PyvcfIterator(SequenceIterator):
    """
    Wrapper for the Reader class of PyVCF
    """
    
    def __init__(self, handle, compressed=False, prepend_chr=False):
        """
        Create a VCF Reader.
        """
        self._parser = Reader(fsock=handle, compressed=compressed, \
                             prepend_chr=prepend_chr)

    def __iter__(self):
        return self

    def next(self):
        """
        Return the next line in the file.
        """
        #return self._parser.next()
        row = self._parser.next()
        # Build SeqFeatures first
        samples = row.samples
        if samples is not None:
            rec_features = list()
            for samp in samples:
                feat_id = samp.sample
                feat_type = samp.gt_type,
                feat_qualifiers = dict(
                    sample = samp.sample,
                    called = samp.called,
                    data = samp.data,
                    gt_bases = samp.gt_bases,
                    is_het = samp.is_het,
                    is_variant = samp.is_variant,
                    phased = samp.phased
                )
                seq_feat = VarSeqFeature(type=feat_type, id=feat_id, \
                                      qualifiers=feat_qualifiers)
                rec_features.append(seq_feat)

        # Build SeqRecord and return
        rec_id = row.ID
        rec_name = "" # XXX what here?  
        rec_seq = ""  # XXX what here?
        rec_annotations = dict(
            ID = row.ID,
            INFO = row.INFO,
            FORMAT = row.FORMAT,
            CHROM = row.CHROM,
            POS = row.POS,
            FILTER = row.FILTER,
            QUAL = row.QUAL,
            _sample_indexes = row._sample_indexes,
            alleles = row.alleles,
            ALT = row.ALT,
            REF = row.REF,
        )
        seq_rec = VarSeqRecord(id=rec_id, seq=rec_seq, name=rec_name, \
                           features=rec_features, annotations=rec_annotations)
        return seq_rec
            

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python script.py test.vcf")

    filename = sys.argv[1]
    with open(filename, "rb") as fh:
        itervcf = PyvcfIterator(fh)
        rec1 = itervcf.next()
        print rec1.ID, rec1.POS
        feature1 = rec1.features[0]
        print feature1.sample, feature1.gt_bases
        #for feature in rec1.features:
            #print feature
        #for rec in itervcf:
            #print rec
