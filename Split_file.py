# -*- coding: utf-8 -*-
"""
Created on Mon Dec  3 14:37:00 2018

@author: prash
"""

import os
import sys

def split_file(filepath, total_no_spilts=1):
    global total_lines
    out_list = []
    no_splits = total_no_spilts
    path, filename = os.path.split(filepath)

    with open(filepath, 'r') as r:
        for i,line in enumerate(r):
            continue

    r.close()

    total_lines = i+1
    print("total lines - "+str(total_lines))
    lps = total_lines/no_splits
    remainder = total_lines % no_splits
    
    i = 0
    while(i<total_lines):
    #for i in range(0, total_lines):
        if(len(out_list) == no_splits - 1):
            lps = lps + remainder
        out_list.append((i,i+lps-1))
        i = i + lps

    return out_list
if __name__ == '__main__':
	out_list = split_file(sys.argv[2], int(sys.argv[1]))
	print(out_list)
