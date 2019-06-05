#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
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
# 通用函数：快速的元素级数组函数
