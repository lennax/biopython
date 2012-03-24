#!/usr/bin/python

# Black box:
# in: fh
# token, value = get_token()

import sys

lexer = ""

def open_file(filename):
    tokens = (
        "COMMENT", 
        "NAME", 
        "LOOP", 
        "DATA", 
        "FREE_VALUE", 
        "SINGLE_QUOTE_VALUE", 
        "DOUBLE_QUOTE_VALUE", 
        "SEMICOLON_VALUE",
    )

    def t_COMMENT(t): 
        r"\#.*\n"
        return None
        
    def t_NAME(t): 
        r"_[^ \t\n]+"
        #return 1
        return t
        
    def t_LOOP(t): 
        r"[Ll][Oo][Oo][Pp]_"
        #return 2
        return t
        
    def t_DATA(t): 
        r"[Dd][Aa][Tt][Aa]_[^ \t\n]+"
        #return 3
        return t
        
    def t_FREE_VALUE(t): 
        r"""[^ \t\n\'\"]+"""
        #return 4
        return t
        
    def t_SINGLE_QUOTE_VALUE(t): 
        r"'[^'\n]*'"
        #return 5
        return t
        
    def t_DOUBLE_QUOTE_VALUE(t): 
        r'"[^"\n]*"'
        #return 6
        return t
        
    def t_SEMICOLON_VALUE(t): 
        r"^;(.*\n[^;])*.*\n;"
        #return 7
        return t
        
    def t_WHITESPACE(t):
        r"[ \t\n]+"
        return None

    def t_newline(t):
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_error(t):
        line = t.value.lstrip()
        i = line.find("\n")
        line = line if i == -1 else line[:1]
        print "Failed to parse line %s: %s" % (t.lineno+1, line)
    
    import ply.lex as lex
    global lexer
    lexer = lex.lex()
    with open(filename) as fh:
        lexer.input(fh.read())

def close_file():
    #if fh:
        #fh.close()
    pass
    
def get_token():
    token = lexer.token()
    #print "Type: '%s' Value: '%s'" % (token.type, token.value)
    #print type(token.type)
    if token:
        return (token.type, token.value)
    else:
        return (None, None)
    
#if len(sys.argv) !=2:
    #sys.stderr.write("Usage: python ply_test.py filename\n")
    #raise SystemExit
#filename = sys.argv[1]
#with open(filename) as fh:
    #print get_token()   

# vim:sw=4:ts=4:expandtab
