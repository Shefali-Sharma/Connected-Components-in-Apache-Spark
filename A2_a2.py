#!usr/bin/python
#Shefali
#Sharma
#sharma92
#50247677

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
    #print("Min : ", minVal, " tuple : ", tuple)

    yield minVal, line[0], tuple

"""
    for x in tuple:
        if line[0] < x:
            yield x, minVal
"""

def LargeStar(line):
    minVal2 = line[0]
    b = line[1]
    #print("\n min = ", minVal2, " b = ", b)
    v = line[2]
    #print("\n min = ", minVal2, " b = ", b)
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
    #print("\n min = ", minVal2, " b = ", b)
    v = line[2]
    v.append(b)
    #print("\n min = ", minVal2, " b = ", b)
    for x in v:
        #if x != minVal2:
        yield x, minVal2

def callLargeStarFirst(lines):
    #-----------------------Map Large Star-------------------------#
    W = lines.flatMap(MapLargeStarFirst)
    #print(W.collect())
    
    groupValues = W.groupByKey().mapValues(list)
    
    #print("\nPrinting Min: ")
    
    #----------------Find Min and Final Large Star------------------#
    V = groupValues.flatMap(findMin) #.reduceByKey(lambda a, b, c: LargeStar(a, b, c))
    #print(V.collect())
    
    AfterLargeStar = V.flatMap(LargeStar).distinct()
    #print(AfterLargeStar.collect())

    #----------------------Large Star End---------------------------#
    return AfterLargeStar

def callLargeStar(lines):
    #-----------------------Map Large Star-------------------------#
    W = lines.flatMap(MapLargeStar)
    #print(W.collect())
    
    groupValues = W.groupByKey().mapValues(list)
    
    #print("\nPrinting Min: ")
    
    #----------------Find Min and Final Large Star------------------#
    V = groupValues.flatMap(findMin) #.reduceByKey(lambda a, b, c: LargeStar(a, b, c))
    #print(V.collect())
    
    AfterLargeStar = V.flatMap(LargeStar).distinct()
    #print(AfterLargeStar.collect())
    
    #----------------------Large Star End---------------------------#
    return AfterLargeStar

def callSmallStar(lines):
    #------------------------Map Small Star-------------------------#
    applyingSmallStarMap = AfterLargeStar.flatMap(MapSmallStar)
    #print(applyingSmallStarMap.collect())
    
    groupValues2 = applyingSmallStarMap.groupByKey().mapValues(list)
    #groupValues2.foreach(printRecord)
    
    #----------------Find Min and Final Small Star------------------#
    X = groupValues2.flatMap(findMin)
    #print(X.collect())
    
    AfterSmallStar = X.flatMap(SmallStar).distinct()
    #print(AfterSmallStar.collect())
    #----------------------Small Star End---------------------------#
    return AfterSmallStar


def checkDifference(val1, val2):
    rdd1 = val1.subtract(val2)
    rdd2 = val2.subtract(val1)
    
    #print("\n Checkinf Difference\n")
    #print(rdd1.collect())
    #print(rdd2.collect())
    
    rddFinal = rdd1.union(rdd2)
    #print("\n After Union: ")
    #print(rddFinal.collect())

    countElements = rddFinal.count()
    
    #if countElements == 0:
    #print("\n No elements in union. Count = 0")
    return countElements

if __name__ == "__main__":
    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf = conf)
    lines = sc.textFile(sys.argv[1])
    #print("\nValue of lines : ")
    #print(lines.collect())

    AfterLargeStar = callLargeStarFirst(lines)
    AfterSmallStar = callSmallStar(AfterLargeStar)

    val = 1
    
    while val:
        var = checkDifference(AfterLargeStar, AfterSmallStar)
        AfterLargeStar = callLargeStar(AfterSmallStar)
        AfterSmallStar = callSmallStar(AfterLargeStar)
        if var == 0:
            val = 0
            print("\n\n Converged.")
            break

    AfterSmallStar.saveAsTextFile("output");
    #AfterSmallStar.saveAsTextFile(sys.argv[2]);
    sc.stop()
