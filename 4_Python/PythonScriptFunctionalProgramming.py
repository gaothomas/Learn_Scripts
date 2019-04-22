#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from functools import reduce


def normalize(name):
    return name.capitalize()


L1 = ['adams', 'LISA', 'barT']
L2 = list(map(normalize, L1))
print(L2)


def prod(L):
    return reduce(lambda x, y: x*y, L)


print(prod([3, 5, 7, 9]))


def str2float(s):
    digits = {
		'0': 0,
		'1': 1,
		'2': 2,
		'3': 3,
		'4': 4,
		'5': 5,
		'6': 6,
		'7': 7,
		'8': 8,
		'9': 9,
		'.': -1
		}
    reduce(lambda x, y: x*10 + y, map(lambda ch: digits[ch],s))
        
    map(lambda ch: digits[ch],s)
	
