import os
import sys
from random import randint, sample, shuffle
from math import log
import numpy as np
import shutil


class constForSyn():
    caseN = 10
    Range = 10 ** 7
    dim = 2
    mu = 0.5 * Range
    sigma = 0.10 * Range
    pointFiles = ["uni", "nor", "exp", "skew"]
    numList = [5 * 10 ** 3]
    alpha = 2


class CFR(constForSyn):
    pass


class baseGenerator:
    def gen(self, n):
        pass


class randomGenerator(baseGenerator):

    def __init__(self, mx):
        self.mx = mx

    def setMx(self, mx):
        self.mx = mx

    def gen(self, n):
        ret = [0] * n
        for i in range(n):
            x = randint(-self.mx, self.mx)
            ret[i] = x
        return ret


class normalGenerator(baseGenerator):

    def __init__(self, mu, sigma):
        self.mu = mu
        self.sigma = sigma

    def gen(self, n, lb=None, rb=None):
        ret = np.random.normal(self.mu, self.sigma, n)
        for i in range(n):
            if lb is not None and ret[i] < lb:
                ret[i] = lb
            if rb is not None and ret[i] > rb:
                ret[i] = rb
        return ret

    def setMu(self, mu):
        self.mu = mu

    def setSigma(self, sigma):
        self.sigma = sigma


class uniformGenerator(baseGenerator):

    def __init__(self, low, high):
        self.low = low
        self.high = high

    def gen(self, n, lb=None, rb=None):
        ret = np.random.uniform(self.low, self.high, n)
        for i in range(n):
            if lb is not None and ret[i] < lb:
                ret[i] = lb
            if rb is not None and ret[i] > rb:
                ret[i] = rb
        return ret

    def setLow(self, low):
        self.low = low

    def setHigh(self, high):
        self.high = high


class expGenerator(baseGenerator):

    def __init__(self, mu):
        self.mu = mu

    def gen(self, n, lb=None, rb=None):
        ret = np.random.exponential(self.mu, n)
        for i in range(n):
            if lb is not None and ret[i] < lb:
                ret[i] = lb
            if rb is not None and ret[i] > rb:
                ret[i] = rb
        return ret

    def setMu(self, mu):
        self.mu = mu


def genPoints(gtor, n):
    points = []
    vals = []
    for j in range(CFR.dim):
        vals.append(gtor.gen(n))
    for i in range(n):
        tmp = []
        for j in range(CFR.dim):
            tmp.append(vals[j][i])
        points.append(tuple(tmp))
    ret = list(set(points))
    return ret


def genSynData(points, desPath, prefix, size, databaseID):
    csvFileName = "spatial.csv"
    scmFileName = "spatial.scm"
    csvFileName = os.path.join(desPath, csvFileName)
    scmFileName = os.path.join(desPath, scmFileName)
    with open(csvFileName, "w") as fout:
        fout.write("S_ID | S_POINT\n")
        for i in range(len(points)):
            fout.write(str(size * databaseID + i) + " | (")
            fout.write(" ".join(map(str, points[i])))
            fout.write(")\n")
    with open(scmFileName, "w") as fout:
        fout.write("S_ID | S_POINT\n")
        fout.write("LONG | POINT\n")



def genBeta(alpha, n):
    lnk = log(alpha, 10)
    base, beg = 1000, 1
    dx = (alpha - beg) * 1.0 / 1000
    P = [0.0] * base
    sum = 0.0

    for i in range(base):
        P[i] = dx / (lnk * (beg + i * dx))
        sum += P[i]

    for i in range(base):
        P[i] /= sum
        if i > 0:
            P[i] += P[i - 1]

    retList = []
    for j in range(n):
        ret = alpha
        randf = randint(0, base * 10 - 1) / (base * 10.0)
        for i in range(base):
            if randf <= P[i]:
                ret = beg + i * dx
                break
        ret /= alpha
        ret = min(ret, 1.0)
        ret = max(1.0 / alpha, ret)
        retList.append(ret)

    return retList


def genParameter(desPath):
    k = CFR.alpha
    tmpFilePath = os.path.join(desPath, str(k))
    if not os.path.exists(tmpFilePath):
        os.mkdir(tmpFilePath)
    betas = genBeta(k, CFR.caseN)
    for i in range(CFR.caseN):
        desFileName = "data_%02d.txt" % (i)
        desFileName = os.path.join(tmpFilePath, desFileName)
        with open(desFileName, "w") as fout:
            fout.write("%.4f %d\n" % (betas[i], k))


def genSynDataSet(desPath, prefix, databaseNum, dataSize, params):
    if os.path.exists(desPath):
        shutil.rmtree(desPath)
    os.mkdir(desPath)
    # gen data first
    for i in range(databaseNum):
        ownerPath = os.path.join(desPath, "database" + str(i))
        os.mkdir(ownerPath)
        if prefix == "uni":
            gtor = uniformGenerator(params.get("low"), params.get("high"))
            points = genPoints(gtor, dataSize)
            genSynData(points, ownerPath, prefix, dataSize, i)
        elif prefix == "nor":
            mu, sig = params.get("mu"), params.get("sigma")
            gtor = normalGenerator(mu, sig)
            points = genPoints(gtor, dataSize)
            genSynData(points, ownerPath, prefix, dataSize, i)
        elif prefix == "exp":
            mu = params.get("mu")
            gtor = expGenerator(mu)
            points = genPoints(gtor, dataSize)
            genSynData(points, ownerPath, prefix, dataSize, i)
        else:
            print("don't support: " + prefix)
            exit(0)

    # gen parameter beta second
    # genParameter(desPath)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("please input at least 2 params, like\npython3 ./scripts/test/genSyntheticData.py 3 200")
        exit(0)
    desPath = "dataset/SynData"
    databaseNum = int(sys.argv[1])
    dataSize = int(sys.argv[2])
    distribution = None
    params = {}
    if len(sys.argv) == 3 or sys.argv[3] == "uni":
        distribution = "uni"
        if len(sys.argv) < 5:
            params["low"] = CFR.Range * (-1)
            params["high"] = CFR.Range
        elif len(sys.argv) == 6:
            params["low"] = int(sys.argv[4])
            params["high"] = int(sys.argv[5])
        else:
            print("wrong format for uni!")
            exit(0)
    elif sys.argv[3] == "nor":
        distribution = "nor"
        if len(sys.argv) < 5:
            params["mu"] = 0
            params["sigma"] = 10 ** 5
        elif len(sys.argv) == 6:
            params["mu"] = int(sys.argv[4])
            params["sigma"] = int(sys.argv[5])
        else:
            print("wrong format for nor!")
            exit(0)
    elif sys.argv[3] == "exp":
        distribution = "exp"
        if len(sys.argv) < 5:
            params["mu"] = 5 * 10 ** 6
        elif len(sys.argv) == 5:
            params["mu"] = int(sys.argv[4])
        else:
            print("wrong format for exp!")
            exit(0)
    else:
        print("not support distribution: " + sys.argv[3])
        exit(0)
    genSynDataSet(desPath, distribution, databaseNum, dataSize, params)
