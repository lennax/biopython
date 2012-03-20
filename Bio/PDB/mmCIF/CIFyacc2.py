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

# Parse entire file into data_pairs
def p_data_pairs(p):
    """data_pairs : data_pairs data_pair"""
    p[0] = tuple(p[1]) + (p[2],)
    #print p[2]

# 1-member data_pairs breaks recursion
def p_data_pairs_first(p):
    """data_pairs : data_pair"""
    p[0] = (p[1],)
    #print p[1]

# Types of data_pair
def p_data_data(p):
    """data_pair : DATA"""
    p[0] = ("data", p[1])

def p_data_pair(p):
    """data_pair : TAG VALUE"""
    p[0] = (p[1], p[2])

def p_loop(p):
    """data_pair : loop_header values"""
    p[0] = p[1] + (p[2],)

# Recursion for loop
def p_loop_header(p):
    """loop_header : LOOP tags"""
    p[0] = (p[1],) + (p[2],)

def p_tag_tags(p):
    """tags : tags TAG"""
    p[0] = tuple(p[1]) + (p[2],)

def p_tags_first(p):
    """tags : TAG"""
    p[0] = (p[1],)

def p_value_values(p):
    """values : values VALUE"""
    p[0] = tuple(p[1]) + (p[2],)

def p_values_first(p):
    """values : VALUE"""
    p[0] = (p[1],)

# Misc rules
def p_error(p):
    sys.stderr.write("Syntax error in '%s'\n" % p)

parser = yacc.yacc()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        filename = sys.argv[1]

        with open(filename) as fh:
            filedump = fh.read()
        result = parser.parse(filedump, debug=1)
        print result
        yacc_end = time.clock() - yacc_start
        print "Runtime: ", yacc_end

# vim:sw=4:ts=4:expandtab
