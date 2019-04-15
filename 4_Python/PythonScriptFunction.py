#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math

def quadratic(a, b, c):
    if a == 0 and b == 0 and c != 0:
        raise ValueError ('Bad IO')
    elif a == 0 and b == 0 and c == 0:
        return 'Infinite Answer'
    elif a == 0:
        return -c/b
    elif b*b-4*a*c == 0:
        return -b/2/a
    elif b*b-4*a*c > 0:
        return -b/2/a+math.sqrt(b*b-4*a*c)/2/a, -b/2/a-math.sqrt(b*b-4*a*c)/2/a
    else:
        return 'No Answer'
#print(quadratic(0,0,1))
print(quadratic(0,0,0))
print(quadratic(0,2,1))
print(quadratic(1,2,1))
print(quadratic(1,3,2))
print(quadratic(1,2,2))

def product(*nums):
    multiply_num = 1
    for num in nums:
        multiply_num = multiply_num * num
    return multiply_num

array = [1,3,5,8]
print(product(*array))

def move(n, a, b, c):
    if n == 1:
        print(a,'-->',c)
    else:
        move(n-1, a, c, b)
        move(1, a, b, c)
        move(n-1, b, a, c)

print(move(3,'A','B','C'))
