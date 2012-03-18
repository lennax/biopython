#!/usr/bin/python

import sys
import time
import ply.yacc as yacc

# Import lexer and tokens, build lexer
from CIFlex2 import CIFlex
m = CIFlex()
m.build()
tokens = m.tokens
#print tokens

yacc_start = time.clock()

def p_data_pairs(p):
    """data_pairs : data_pairs data_pair"""
    p[0] = tuple(p[1]) + (p[2],)

def p_data_pairs_empty(p):
    """data_pairs : empty"""
    p[0] = ()

def p_data_data(p):
    """data_pair : DATA"""
    p[0] = ("data", p[1])

def p_data_pair(p):
    """data_pair : TAG VALUE"""
    p[0] = (p[1], p[2])

def p_tag_tags(p):
    """tags : tags TAG"""
    p[0] = tuple(p[1]) + (p[2],)

def p_tags_empty(p):
    """tags : empty"""
    p[0] = ()

def p_value_values(p):
    """values : values VALUE"""
    p[0] = tuple(p[1]) + (p[2],)

def p_values_empty(p):
    """values : empty"""
    p[0] = ()

def p_loop(p):
    """data_pair : loop_header values"""
    p[0] = p[1] + (p[2],)

def p_loop_header(p):
    """loop_header : LOOP tags"""
    p[0] = (p[1],) + (p[2],)

def p_empty(p):
    """empty :"""
    pass
    
def p_error(p):
    sys.stderr.write("Syntax error in '%s'\n" % p )
    
precedence = (
    # I have no idea how to fix the shift/reduce error
    # fortunately it is picking the right thing
    ('right', "TAG"),
)

parser = yacc.yacc()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        filename = sys.argv[1]
        
        with open(filename) as fh:
            result = parser.parse(fh.read(), debug=1)
            print result
            yacc_end = time.clock() - yacc_start
            print "Runtime: ", yacc_end

# vim:sw=4:ts=4:expandtab
