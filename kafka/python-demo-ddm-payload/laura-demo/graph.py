import numpy as np
import matplotlib.pyplot as plt

def mean(i1,i2,i3):
    avg = np.mean([i1,i2,i3])
    return avg

one = mean(366.12,437.14,447.36)
two = mean(630.16,629.58,514.77)
three = mean(717.85,874.46,774.30)
four = np.mean([829.64, 1223.35, 1203.74])
five = np.mean([1017.90, 1981.29, 971.97])
six = np.mean([1047.08, 1078.03, 1107.03])
seven = np.mean([1157.50, 1184.65, 1506.94])
eight = np.mean([1328.08, 1303.79, 1372.53])
nine = np.mean([1733.82, 1355.89, 1942.40])

avgs = np.array([one,two,three,four,five,six,seven,eight,nine])
sizes = np.array([0.093,0.8,2.1,2.8,3.4,4.3,5.1,6,6.8])

plt.plot(sizes,avgs)
plt.xlabel("Image Sizes (MB)")
plt.ylabel("Latency (ms)")
plt.savefig('plot1.png')
plt.show()