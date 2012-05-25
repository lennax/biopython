# Copyright 2012 Lenna X. Peterson (arklenna@gmail.com)
# All rights reserved
#
# This code is part of the Biopython distribution and governed by its
# license.  Please see the LICENSE file that should have been included
# as part of this package.

import os

#from Bio.SeqIO import PyvcfIO
from PyvcfIO import PyvcfIterator

class VariantIO(object):

    def __init__(self, filename, filetype=None, compressed=False,
                 vcfreader=None, gvfreader=None):
        """
        Currently supported filetypes are "vcf" and "gvf".
        If filetype is not specified, guessed from file extension
        Uses the default parser wrappers unless otherwise specified.
        """
        self.filename = filename
        self.filetype = filetype
        self.compressed = compressed
        if filetype is None:
            self._guess_filetype()
            if self.filetype is None:
                raise RuntimeError("Unknown filetype, please specify.")

        #print self.compressed, self.filename, self.filetype

        parsers = dict(
            vcf = PyvcfIterator,
            #gvf = GvfIterator,
        )

        # the class isn't currently reusable but it's simpler to just set both
        if vcfreader is not None:
            parsers['vcf'] = vcfreader
        if gvfreader is not None:
            parsers['gvf'] = gvfreader

        fh = open(filename, "rb")
        try:
            parsers[self.filetype](fh)
        except KeyError:
            raise RuntimeError('Given filetype is not valid')

    def _guess_filetype(self):
        """
        PRIVATE
        Attempts to determine compression and file type from extension
        """
        compressed_ext = ['.gz']
        vcf_ext = ['.vcf']
        gvf_ext = ['.gvf', '.gff', '.gff3']
        root, ext = os.path.splitext(self.filename)
        if ext in compressed_ext:
            self.compressed = True
            ext = os.path.splitext(root)[1]
        if ext in vcf_ext:
            self.filetype = 'vcf'
        elif ext in gvf_ext:
            self.filetype = 'gvf'


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python script.py testfile")

    filename = sys.argv[1]
    test = VariantIO(filename)
    
 


