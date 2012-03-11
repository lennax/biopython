#!/usr/bin/python

import sys
import time
import ply.yacc as yacc

from CIFlex import CIFlex
tokens = CIFlex.tokens
#print tokens

yacc_start = time.clock()

#<DataItems>  :     <Tag> <WhiteSpace> <Value> |
#                   <LoopHeader><LoopBody>
def p_data_pairs(p):
    """data_pairs : data_pair data_pairs"""
    p[0] = (p[1],) + tuple(p[2])

def p_data_pairs_empty(p):
    """data_pairs : empty"""
    p[0] = []

def p_data_header(p):
    """data_pair : DATA"""
    p[0] = ("data", p[1])

def p_tags_tag(p):
    """tags : TAG tags"""
    p[0] = (p[1],) + tuple(p[2])

def p_tags_empty(p):
    """tags : empty"""
    p[0] = []

def p_values_value(p):
    """values : VALUE values"""
    p[0] = (p[1],) + tuple(p[2])

def p_values_empty(p):
    """values : empty"""
    p[0] = []
    
def p_loop(p):
    """data_pair : loop_header values"""
    p[0] = p[1] + (p[2],)

def p_data_pair(p):
    """data_pair : TAG VALUE"""
    p[0] = (p[1], p[2])
    
def p_loop_header(p):
    """loop_header : LOOP tags"""
    p[0] = (p[1],) + (p[2],)
    
def p_empty(p):
    """empty :"""
    pass

#def p_loop_data(p):
    #"""data_pair : LOOP"""
    #p[0] = p[1]

#<Value>  :     { '.' | '?' | <Numeric> | <CharString> | <TextField> }
#def p_value(p):
    #"""value : number
             #| char_string
             #| text_field
             #| UNKNOWN
             #| INAPPLICABLE"""
    #p[0] = p[1]


#<CharString>  :    <UnquotedString> | <SingleQuotedString> | <DoubleQuotedString>
#def p_char_string(p):
    #"""char_string : EOL_UNQUOTED_STRING
                   #| NOTEOL_UNQUOTED_STRING
                   #| SINGLE_QUOTED_STRING
                   #| DOUBLE_QUOTED_STRING"""
    #p[0] = p[1]

#<Number>  :    {<Integer> | <Float> }
#<Numeric>  :   { <Number> | <Number> '(' <UnsignedInteger> ')' }
#def p_number(p):
    #"""number : INTEGER
              #| FLOAT"""
    #p[0] = p[1]
    
#<TextField>  :     { <SemiColonTextField> }
#def p_text_field(p):
    #"""text_field : SEMI_TEXT_FIELD"""
    #p[0] = p[1]
#<CIF>  :   <Comments>? <WhiteSpace>? { <DataBlock> { <WhiteSpace> <DataBlock> }* { <WhiteSpace> }? }?

#<DataBlock>  :     <DataBlockHeading> {<WhiteSpace> { <DataItems> | <SaveFrame>} }*

#<DataBlockHeading>  :  <DATA_> { <NonBlankChar> }+

#<SaveFrame>  :     <SaveFrameHeading> { <WhiteSpace> <DataItems> }+ <WhiteSpace> <SAVE_>

#<SaveFrameHeading>  :  <SAVE_> { <NonBlankChar> }+


#<LoopHeader>  :    <LOOP_> {<WhiteSpace> <Tag>}+

#<LoopBody>  :  <Value> { <WhiteSpace> <Value> }*

def p_error(p):
    sys.stderr.write("Syntax error in '%s'\n" % p )

precedence = (
    ('right', "TAG", "VALUE"),
)

parser = yacc.yacc()

if len(sys.argv) == 2:
    filename = sys.argv[1]
    
    with open(filename) as fh:
        result = parser.parse(fh.read())
        print result
        yacc_end = time.clock() - yacc_start
        print "Runtime: ", yacc_end
