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


class PyvcfIterator(SequenceIterator):
    """
    Wrapper for the Reader class of PyVCF
    """
    
    def __init__(self, handle, compressed=False, prepend_chr=False):
        """
        Create a VCF Reader.
        """
        self._parser = Reader(fsock=handle, compressed=compressed, 
                             prepend_chr=prepend_chr)


    def __iter__(self):
        return self

    def next(self):
        """
        Return the next line in the file.
        """
        return self._parser.next()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python script.py test.vcf")

    filename = sys.argv[1]
    with open(filename, "rb") as fh:
        itervcf = PyvcfIterator(fh)
        #print itervcf.next()
        for rec in itervcf:
            print rec
