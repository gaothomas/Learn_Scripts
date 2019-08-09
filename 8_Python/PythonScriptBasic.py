#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# name = input('Please enter your name:')
# print('Hello, %s!' % name.encode('utf-8').decode('ascii', errors='ignore'))
# print('%07d * %.2f%% = %7d%%' % (1024, 3.1415926, 1024*3.1415926))
# print(ord('A'),chr(90))
# print('小明去年成绩：{0}，今年成绩：{1}，成绩提升了{2:.1f}%。'.format(72, 85, (85/72-1)*100))
# height = 1.78
# weight = 78
# bmi = weight/height/height
# if bmi > 32:
    # print('严重肥胖')
# elif bmi >= 28:
    # print('肥胖')
# elif bmi >= 25:
    # print('过重')
# elif bmi >= 18.5:
    # print('正常')
# else:
    # print('过轻')
# 计算1*3*...*99
# multiple_stage = 1
# for num in range(100):
    # if num % 2 == 0:
        # continue
    # multiple_stage = multiple_stage * num
# print(multiple_stage)


def factorial(n):
    if n == 1:
        return n
    return factorial(n-1) * n


print(factorial(5))


def num_contain_factornum(n, factor):
    max_iteration = 1
    while n >= factor ** max_iteration:
        max_iteration = max_iteration + 1
    n_num = 0
    for i in range(1, max_iteration+1):
        n_num = n_num + n//factor ** i
    return n_num


print(num_contain_factornum(10000, 5))
s1 = dict()
s2 = {1, 2, 3}
print(type(s1), type(s2))
