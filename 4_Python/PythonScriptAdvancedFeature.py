#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def trim(s):
    if s:
        while s[:1] == ' ': # 切片不会出现下标越界
            s = s[1:]
        while s[-1:] == ' ':
            s = s[:-1]
    return s


def findMinAndMax(s):
    if s:
        max = min = s[0]
        for i in s:
            if i > max:
                max = i
            if i < min:
                min = i
        return min, max
    return None, None


s = ['Hello', 'World', 18, 'Apple', None]
print([x.lower() for x in s if isinstance(x, str)])


