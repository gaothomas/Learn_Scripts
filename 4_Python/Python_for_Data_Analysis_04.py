#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import matplotlib.pyplot as plt
import pylab
import random
#%%
# #ndarray一种多维数组对象
# 创建ndarray
data1 = [6, 7.5, 8, 0, -1]
arr1 = np.array(data1)
data2 = [[1, 2, 3, 4], [5, 6, 7, 8]]
arr2 = np.array(data2)
print(arr1)
print(arr2)
print(arr2.ndim)  # 维度
print(arr2.shape)  # 形状
print(arr1.dtype)  # 数据类型
print(arr2.dtype)
print(np.zeros(10))
print(np.ones((2, 4)))
print(np.empty((2, 3, 2)))
print(np.arange(15).reshape((3, 5)))
#%%
# ndarray的数据类型
arr1 = np.array([1, 2, 3], dtype=np.float64)
arr2 = np.array([1, 2, 3], dtype=np.int32)
print(arr1.dtype, arr2.dtype)
arr = np.array([1, 2, 3, 4, 5])
print(arr.dtype)
float_arr = arr.astype(np.float64)  # 数据类型转换，创建一个新数组（一个数据的备份）
print(float_arr.dtype)
print(arr.dtype)
arr = np.array([3.7, -1.2, -2.6, 0.5, 12.9, 10.1])
print(arr.dtype)
print(arr.astype(np.int32))
numeric_strings = np.array(['1.25', '-9.6', '42'], dtype=np.string_)
print(numeric_strings.astype(np.float64))  # 写float也可以，Numpy可自动转换
int_array = np.arange(10)
calibers = np.array([.22, .270, .357, .380, .44, .50], dtype=np.float64)
print(int_array.astype(calibers.dtype))
#%%
# 数组与标量间的运算
arr = np.array([[1, 2, 3], [4, 5, 6]])
print(arr * arr)
print(arr - arr)
print(1 / arr)
print(arr ** 0.5)
#%%
# 基本的索引和切片
arr = np.arange(10)  # 一维数组
print(arr)
print(arr[5])
print(arr[5:8])
arr[5:8] = 12  # ndarray切片是原始数组的视图
print(arr)
arr_slice = arr[5:8]
arr_slice[1] = 12345
print(arr)
arr_slice[:] = 64
print(arr)
arr[5:8].copy()  # 创建ndarray切片的一份副本而非视图
arr2d = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])  # 二维数组
print(arr2d[2])
print(arr2d[0][2])
print(arr2d[0, 2])
arr3d = np.array([[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]])
print(arr3d)
print(arr3d[0])
old_values = arr3d[0].copy()
arr3d[0] = 42
print(arr3d)
arr3d[0] = old_values
print(arr3d)
print(arr3d[1, 0])
print(arr[1:6])  # 切片索引
print(arr2d[:2])
print(arr2d[:2, 1:])
print(arr2d[1, :2])
print(arr2d[2, :1])
print(arr2d[:, :1])
print(arr2d[1, :2].shape)  # ':'代表选取整轴
print(arr2d[1:2, :2].shape)
#%%
# 布尔型索引
names = np.array(['Bob', 'Joe', 'Will', 'Bob', 'Will', 'Joe', 'Joe'])
data = np.random.randn(7, 4)
print(names)
print(data)
print(names == 'Bob')
print(data[names == 'Bob'])  # 布尔型数组可用作数组的索引
print(data[names == 'Bob', 2:])
print(data[names == 'Bob', 3].shape)
print(names != 'Bob')
print(data[~(names == 'Bob')])  # ~代表逻辑关系非
print(data[(names == 'Bob') | (names == 'Will')])  # 与&，或|运算优先级高于==
data[data < 0] = 0
print(data)
data[names != 'Joe'] = 7
print(data)
new_data = data[names != 'Joe']  # 指定另一变量时，布尔型索引创建数据的副本，与切片不同
new_data[:] = 8
print(new_data)
print(data)
#%%
# 花式索引
arr = np.empty((8, 4))
for i in range(8):
    arr[i] = i
print(arr)
print(arr[[4, 3, 0, 6]])
print(arr[[-3, -5, -7]])
arr = np.arange(32).reshape((8, 4))
print(arr)
print(arr[[1, 5, 7, 2], [0, 3, 1, 2]])  # 返回一维数组
print(arr[[1, 5, 7, 2]][:, [0, 3, 1, 2]])
print(arr[np.ix_([1, 5, 7, 2], [0, 3, 1, 2])])
new_arr = arr[[1, 5, 7, 2]]  # 指定另一变量时，花式索引创建数据的副本，与切片不同
new_arr[:, [0, 3, 1, 2]] = 0
print(new_arr)
print(arr)
#%%
# 数组转置和轴对换
arr = np.arange(15).reshape((3, 5))
print(arr)
print(arr.T)  # 返回源数组的视图
arr = np.random.randn(6, 3)
print(np.dot(arr.T, arr))  # 计算矩阵内积
arr = np.arange(16).reshape((2, 2, 4))
print(arr)
print(arr.transpose((1, 0, 2)))
print(arr.swapaxes(1, 2))
#%%
# 通用函数：快速的元素级数组函数 ufunc
arr = np.arange(10)
print(np.sqrt(arr))
print(np.exp(arr))
x = np.random.randn(8)
y = np.random.randn(8)
print(x, y)
print(np.maximum(x, y))
arr = np.random.randn(7) * 5
print(np.modf(arr))
# abs,fabs
# sqrt
# square
# exp
# log,log10,log2,log1p
# sign
# ceil
# floor
# rint
# modf
# isnan
# isfinite, isinf
# cos,cosh,sin,sinh,tan,tanh
# arccos,arccosh,arcsin,arcsinh,arctan,arctanh
# logical_not
# add
# subtract
# multiply
# divide,floor_divide
# power
# maximum,fmax
# minimun,fmin
# mod
# copysign
# greater,greater_equal,less,less_equal,equal,not_equal
# logical_and,logical_or,logical_xor
#%%
# 利用数组进行数据处理
points = np.arange(-5, 5, 0.01)
xs, ys = np.meshgrid(points, points)
print(ys)
z = np.sqrt(xs**2+ys**2)
print(z)
plt.imshow(z, cmap=plt.cm.gray)
plt.colorbar()
plt.title('Image plot of $\\sqrt{x^2 + y^2}$ for a grid of values')
pylab.show()
#%%
# 将条件逻辑表述为数组运算
xarr = np.array([1.1, 1.2, 1.3, 1.4, 1.5])
yarr = np.array([2.1, 2.2, 2.3, 2.4, 2.5])
cond = np.array([True, False, True, True, False])
results = np.array([a if c else b for a, b, c in zip(xarr, yarr, cond)])
print(results)
print(np.where(cond, xarr, yarr))
arr = np.random.randn(4, 4)
print(arr)
print(np.where(arr > 0, 2, -2))
print(np.where(arr > 0, 2, arr))
#%%
# 数学和统计方法
arr = np.random.randn(5, 4)
print(arr)
print(arr.mean())
print(np.mean(arr))
print(arr.sum())
print(arr.mean(axis=1))
print(arr.sum(0))
arr = np.arange(9).reshape((3, 3))
print(arr.cumsum(0))
print(arr.cumsum(1))
print(arr.cumprod(axis=1))
# sum
# mean
# std,var
# min,max
# argmin,argmax
# cumsum
# cumprod
#%%
# 用于布尔型数组的方法
arr = np.random.randn(100)
print((arr > 0).sum())
bools = np.array([False, False, True, False])
print(bools.any())
print(bools.all())
#%%
# 排序
arr = np.random.randn(8)
print(arr)
arr.sort()
print(arr)
arr = np.random.randn(5, 3)
print(np.sort(arr, axis=0))  # np.sort返回数组已排序副本, axis默认-1
print(arr)
arr.sort(1)
print(arr)
large_arr = np.random.randn(1000)
large_arr.sort()
print(large_arr[int(0.05*len(large_arr))])  # 5%分位数
#%%
# 唯一化以及其他的集合逻辑
names = np.array(['Bob', 'Joe', 'Will', 'Bob', 'Will', 'Joe', 'Joe'])
print(np.unique(names))
ints = np.array([3, 3, 3, 2, 2, 1, 1, 4, 4])
print(np.unique(ints))
print(sorted(set(names)))
values = np.array([6, 0, 0, 3, 2, 5, 6])
print(np.in1d(values, [2, 3, 6]))
# unique(x)
# intersect1d(x,y)
# union1d(x,y)
# in1d(x,y)
# setdiff1d(x,y)
# setxor1d(x,y)
#%%
# 用于数组的文件输入输出
arr = np.arange(10)
np.save('some_array', arr)  # 保存数组文件
print(np.load('some_array.npy'))
np.savez('array_archive.npz', a=arr, b=arr)
arch = np.load('array_archive.npz')
print(arch['a'])
# np.loadtxt()
# np.savetxt()
#%%
# 线性代数（暂略）
#%%
# 随机数生成（具体函数暂略）
samples = np.random.normal(size=(4, 4))
print(samples)
# 随机漫步
position = 0
walk = [position]
steps = 100
for i in range(steps):
    step = 1 if random.randint(0, 1) else -1
    position = position + step
    walk.append(position)
print(walk)
nsteps = 100
draws = np.random.randint(0, 2, size=nsteps)  # np.random.randint前闭后开
steps = np.where(draws > 0, 1, -1)
walk = steps.cumsum()
print(walk)
print(walk.min())
print(walk.max())
print((np.abs(walk) >= 10).argmax())
nwalks = 5000
nsteps = 1000
draws = np.random.randint(0, 2, size=(nwalks, nsteps))
steps = np.where(draws > 0, 1, -1)
walks = steps.cumsum(1)
print(walks)
print(walks.max())
print(walks.min())
hits30 = (np.abs(walks) >= 30).any(1)
print(hits30)
print(hits30.sum())
crossing_times = (np.abs(walks[hits30]) >= 30).argmax(1)
print(crossing_times)
print(crossing_times.mean())
