# coding:utf-8
from array import array
from random import random
floats = array('d', (random() for i in range(10**7)))
print(floats[-1])
choice = '1'
print('小精灵:推荐你去存取款窗口。' if choice == '1' else '小精灵:金加隆和人民币的兑换率为1：51.3，即1金加隆 = 51.3人民币')