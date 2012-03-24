# Copyright (C) 2002, Thomas Hamelryck (thamelry@binf.ku.dk)
# This code is part of the Biopython distribution and governed by its
# license.  Please see the LICENSE file that should have been included
# as part of this package.

"""Turn an mmCIF file into a dictionary."""

import os
import warnings

## Detect and import correct module
lexer_missing = True
try:
    if os.name != 'java':
        # Import C lexer
        from Bio.PDB.mmCIF.MMCIFlex import MMCIFlex as CIFlex
        lexer_missing = False
except ImportError as errc:
    warnings.warn("Could not import C lexer: %s" % errc, RuntimeWarning)
if lexer_missing:
    try:
        # Import python PLY lexer
        from Bio.PDB.mmCIF.CIFlex import CIFlex
        lexer_missing = False
    except ImportError as errpy:
        warnings.warn("Could not import Python lexer: %s" % errpy, RuntimeWarning)
        raise SystemExit


class MMCIF2Dict(dict):
    # The token identifiers
    NAME=1
    LOOP=2
    DATA=3
    SEMICOLONS=4    
    DOUBLEQUOTED=5
    QUOTED=6
    SIMPLE=7

    def __init__(self, filename):
        if not os.path.isfile(filename):
            raise IOError("File not found.")
        # Init module with filename
        self._lexer = CIFlex(filename)
        # Call superclass constructor with class data
        dict.__init__(self, **self._make_mmcif_dict())

    def _make_mmcif_dict(self): 
        """
        Loop through PLY token (type, value) pairs, return a dict.

        """
        # this dict will contain the name/data pairs 
        mmcif_dict = {}
        # entry for garbage
        mmcif_dict[None] = []
        # local copies
        NAME=self.NAME
        LOOP=self.LOOP
        DATA=self.DATA
        SEMICOLONS=self.SEMICOLONS
        DOUBLEQUOTED=self.DOUBLEQUOTED
        QUOTED=self.QUOTED
        SIMPLE=self.SIMPLE
        # Point local function to instance function
        get_token = self._lexer.get_token
        # are we looping?
        loop_flag=0
        # list of names in loop
        temp_list=[]
        # last encountered name
        current_name=None
        # get first token/value pair
        token, value=get_token()
        # print token, value
        # loop until EOF (token==0)
        while token:
            if token==NAME:
                if loop_flag:
                    # Make lists for all the names in the loop
                    while token==NAME:
                        # create  a list for each name encountered in loop
                        new_list=mmcif_dict[value]=[]
                        temp_list.append(new_list)
                        token, value=get_token()  
                        # print token, value
                    loop_flag=0         
                    # nr of data items parsed
                    data_counter=0
                    # corresponding data name
                    pos=0
                    nr_fields=len(temp_list)
                    # Now fill all lists with the data
                    #while token>3:
                    while token in (SEMICOLONS, DOUBLEQUOTED, QUOTED, SIMPLE):
                        pos=data_counter%nr_fields
                        data_counter=data_counter+1
                        temp_list[pos].append(value)
                        token, value=get_token()  
                        # print token, value
                    if pos!=nr_fields-1:
                        warnings.warn("ERROR: broken name-data pair "
                                      "(data missing)!", RuntimeWarning)
                    # The last token was not used, so
                    # don't set token to None! (this means the 
                    # last parsed token goes through the loop again)
                else:   
                    # simple name-data pair (no loop)
                    # so next token should be the data
                    next_token, data=get_token()  
                    # print token, value
                    mmcif_dict[value]=data
                    #if next_token<4:
                    if next_token in (NAME, LOOP, DATA):
                        warnings.warn("ERROR: broken name-data pair "
                                      "(name-non data pair)!", RuntimeWarning)
                        # print token, value
                    else:   
                        # get next token
                        token=None
            elif token==LOOP:
                loop_flag=1
                temp_list=[]
                # get next token
                token=None
            elif token==DATA:
                mmcif_dict[value[0:5]]=value[5:]
                token=None
            else:
                # we found some complete garbage
                warnings.warn("ERROR: broken name-data pair "
                              "(missing name)!\n%s %s" % (token, value),
                              RuntimeWarning)
                mmcif_dict[None].append(value)
                # get next token
                token=None
            if token==None:
                token, value=get_token()
                # print token, value
        return mmcif_dict


if __name__=="__main__":

    import sys

    if len(sys.argv)!=2:
        print "Usage: python MMCIF2Dict filename."

    filename=sys.argv[1]    

    mmcif_dict = MMCIF2Dict(filename)

    entry = ""
    print "Now type a key ('q' to end, 'k' for a list of all keys):"
    #for key, value in mmcif_dict.items():
        #print key, value
    while(entry != "q"):
        entry = raw_input("MMCIF dictionary key ==> ")    
        if entry == "q":
            sys.exit()
        if entry == "k":
            for key in mmcif_dict:
                print key
            continue
        try:
            value=mmcif_dict[entry]
            if type(value)==type([]):
                for item in value:
                    print item
            else:
                print value
        except KeyError:
            print "No such key found."
