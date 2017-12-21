#!usr/bin/python
#Shefali
#Sharma
#sharma92

from pyspark import SparkConf, SparkContext
import sys

def MapLargeStarFirst(line):
    l = line.split(" ")
    v = [int(x) for x in l]
    return [(v[0], v[1]), (v[1], v[0])]

def MapLargeStar(lines):
    node_u = lines[0]
    node_v = lines[1]
    yield node_u,node_v
    yield node_v,node_u

def findMin(line):
    minVal = line[0]
    tuple = line[1]
    minVal1 = min(tuple)
    if(minVal1 < minVal):
        minVal = minVal1

    yield minVal, line[0], tuple

def LargeStar(line):
    minVal2 = line[0]
    b = line[1]
    v = line[2]
    for x in v:
        if(b <= x):
            yield x, minVal2

def MapSmallStar(line):
    node_u = line[0]
    node_v = line[1]
    if node_v<=node_u:
        yield node_u,node_v
    else:
        yield node_v,node_u

def SmallStar(line):
    minVal2 = line[0]
    b = line[1]
    v = line[2]
    v.append(b)
    for x in v:
        #if x != minVal2:
        yield x, minVal2

def callLargeStarFirst(lines):
    #-----------------------Map Large Star-------------------------#
    W = lines.flatMap(MapLargeStarFirst)
    
    groupValues = W.groupByKey().mapValues(list)
    
    #----------------Find Min and Final Large Star------------------#
    V = groupValues.flatMap(findMin) 
    
    AfterLargeStar = V.flatMap(LargeStar).distinct()

    #----------------------Large Star End---------------------------#
    return AfterLargeStar

def callLargeStar(lines):
    #-----------------------Map Large Star-------------------------#
    W = lines.flatMap(MapLargeStar)
    
    groupValues = W.groupByKey().mapValues(list)
    
    #----------------Find Min and Final Large Star------------------#
    V = groupValues.flatMap(findMin)
    
    AfterLargeStar = V.flatMap(LargeStar).distinct()
    
    #----------------------Large Star End---------------------------#
    return AfterLargeStar

def callSmallStar(lines):
    #------------------------Map Small Star-------------------------#
    applyingSmallStarMap = AfterLargeStar.flatMap(MapSmallStar)
    
    groupValues2 = applyingSmallStarMap.groupByKey().mapValues(list)
    
    #----------------Find Min and Final Small Star------------------#
    X = groupValues2.flatMap(findMin)
    
    AfterSmallStar = X.flatMap(SmallStar).distinct()
    #----------------------Small Star End---------------------------#
    return AfterSmallStar


def checkDifference(val1, val2):
    rdd1 = val1.subtract(val2)
    rdd2 = val2.subtract(val1)
    
    rddFinal = rdd1.union(rdd2)
    countElements = rddFinal.count()
    
    return countElements

if __name__ == "__main__":
    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf = conf)
    lines = sc.textFile(sys.argv[1])

    AfterLargeStar = callLargeStarFirst(lines)
    AfterSmallStar = callSmallStar(AfterLargeStar)

    val = 1
    
    while val:
        var = checkDifference(AfterLargeStar, AfterSmallStar)
        AfterLargeStar = callLargeStar(AfterSmallStar)
        AfterSmallStar = callSmallStar(AfterLargeStar)
        if var == 0:
            val = 0
            break

    AfterSmallStar.saveAsTextFile("output");
    sc.stop()
