#!/usr/bin/python

import sys
import warnings
import re
import ply.lex as lex

class MMCIFply:
    # List of token names (REQUIRED)
    tokens = (
        "DATA",
        "LOOP",
        "TAG",
        "COMMENT",
        "LEADING_SEMI",
        "SINGLE_QUOTE",
        "DOUBLE_QUOTE",
        "BARE_STRING",
    )
    
    states = (
        ('data', 'inclusive'),
        ('loop', 'inclusive'),
        ('semi', 'inclusive'),
    )
    
    t_SINGLE_QUOTE = r"'"
    t_DOUBLE_QUOTE = r'"'
    
    def t_DATA(self,t):
        "^data_[^ \t\n]+"
        t.value = t.value[5:]
        return t
    
    # enter loop state, data will be grabbed when loop ends
    def t_LOOP(self,t):
        "^loop_"
        if t.lexer.current_state() == 'loop':
            warnings.warn("ERROR: Illegal nested loop_ found"
                            "(file structure invalid)", RuntimeWarning)
        # begin loop
        t.lexer.push_state('loop')
        t.lexer.loop_start = t.lexer.lexpos
    
    # comments can be ignored unless they end a loop
    def t_loop_COMMENT(self,t):
        r"^\#.*$"
#        if t.lexer.current_state() == 'loop':
        # grab data from current loop
        t.value = t.lexer.lexdata[t.lexer.loop_start:t.lexer.lexpos+1]
        t.type="LOOP"
        t.lexer.lineno += t.value.count('\n')
        # leave loop
        t.lexer.pop_state()
        return t

    def t_COMMENT(self,t):
        r"^\#.*$"
        return None
    
    def t_ANY_TAG(self,t):
        r"_[^ \t\n]+"
        return t
    
    def t_LEADING_SEMI(self,t):
        r"^;"
        if t.lexer.current_state() == "semi":
            t.value = t.lexer.lexdata[t.lexer.semi_start:t.lexer.lexpos+1]
            t.lexer.lineno += t.value.count('\n')
            # leave semicolon section
            t.lexer.pop_state()
            return t
        else:
            # start semicolon section
            t.lexer.push_state('semi')
            t.lexer.semi_start = t.lexer.lexpos
    
    def t_BARE_STRING(self,t):
        r"""[^ \t\n\;\'\"]+"""
        return t
        
            
    
    # Ignored characters: spaces and tabs
    t_ignore  = ' \t'

    # Newline rule to track line numbers
    def t_newline(self,t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    
    # Error handling rule
    def t_error(self,t):
        print "Illegal character '%s'" % t.value[0]
        t.lexer.skip(1)
    
    
    def build(self, **kwargs):
        # set re.MULTILINE while preserving any user reflags
        if "reflags" in kwargs.keys():
            re_old = kwargs["reflags"]
        else:
            re_old = 0
        kwargs["reflags"] = re_old | re.MULTILINE | re.I
        
        self.lexer = lex.lex(module=self,**kwargs)
        
    def test(self, data):
        self.lexer.input(data)
        while True:
            token = self.lexer.token()
            if not token:
                break
            print token
    
    # Note: call lexer with reflags=re.MULTILINE
    

if len(sys.argv) == 2:
    filename = sys.argv[1]

    with open(filename) as fh:
        m = MMCIFply()  
        m.build()
        m.test(fh.read())
