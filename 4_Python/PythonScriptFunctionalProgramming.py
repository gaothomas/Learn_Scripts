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
    nums = map(lambda ch: digits[ch], s)
    point = 0

    def to_float(a, b):
        nonlocal point
        if b == -1:
            point = 1
            return a
        if point == 0:
            return a * 10 + b
        else:
            point = point/10
            return a + b * point
    return reduce(to_float, nums)


print(str2float('12.41'))


def str2float_new(s):
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
    point = 0
    nums = list(map(lambda ch: digits[ch], s))
    if '.' in s:
        point = len(s)-s.find('.')-1
        nums.remove(-1)
    return reduce(lambda x, y: x * 10 +y, nums)/(10 ** point)


print(str2float_new('12.4144'))
