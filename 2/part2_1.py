# import numpy as np
# from matplotlib import pyplot as plt

# data1  = np.array([[2, 5], [1, 2]]
#     )
# data2 =  np.array([[2, 10], [5, 8], [4, 9]]
#     )
# data3 =  np.array([[8, 4], [7, 5], [6, 4]]
#     )
# x1, y1 = data1.T
# x2, y2 = data2.T
# x3, y3 = data3.T
# plt.scatter(x1,y1,color='r')
# plt.scatter(x2,y2,color='b')
# plt.scatter(x3,y3,color='g')
# plt.show()

# import math

# def filter_points(points, origin):
#   x1, y1 = origin
#   return [round(math.sqrt((x1-x2)**2+(y1-y2)**2),3) for x2,y2 in points]

# p = [(2, 10), (2, 5), (8, 4), (5, 8), (7, 5), (6, 4), (1, 2), (4, 9)]
# result = filter_points(p, (3.667, 9))
# print(*result, sep=" & ")

import matplotlib.pyplot as plt

from scipy.cluster.hierarchy import linkage, dendrogram

sim = [
  [1.00, 0.10, 0.41, 0.55, 0.35],
  [0.10, 1.00, 0.64, 0.47, 0.98],
  [0.41, 0.64, 1.00, 0.44, 0.85],
  [0.55, 0.47, 0.44, 1.00, 0.76],
  [0.35, 0.98, 0.85, 0.76, 1.00]
]

# plt.imshow(sim, cmap='hot', interpolation='nearest')
# plt.show()


linkage_matrix1 = linkage(sim, "single")
print(linkage_matrix1)
plt.subplot(1,2,1)
plt.title("Single link")
dendrogram(linkage_matrix1,
          labels=["p1", "p2", "p3","p4","p5"],
          show_leaf_counts=True)

linkage_matrix2 = linkage(sim, "complete")
print(linkage_matrix2)

plt.subplot(1,2,2)
plt.title("Complete lint")
dendrogram(linkage_matrix2,
          labels=["p1", "p2", "p3","p4","p5"],
                    show_leaf_counts=True)


plt.show()