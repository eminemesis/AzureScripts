#Thread based parallelization, utilizes only 1 cpu core.

import concurrent.futures as cf
import os
import time

def findSize(paths):
    size = 0
    nextPaths = []
    for path in paths:
        if not path.isDir():
            size += path.size
        else:
            nextPaths.append(path.path.lstrip("dbfs:/"))
    
    if len(nextPaths) == 0:
        return size
    else:
        for path in nextPaths:
            size += findSize(dbutils.fs.ls(path))
    return size

#returns multiple fileinfo objects by splitting the search directory, based on the number of cpu cores.
def mapLister(pathToSearch, cores):
    mapList = [item.path.lstrip(f"dbfs:/").rstrip("/") for item in dbutils.fs.ls(pathToSearch)]
    while len(mapList)<cores:
        newMapList = []
        for path in mapList:
            for i in [item.path.lstrip(f"dbfs:/").rstrip("/") for item in dbutils.fs.ls(path)]:
                newMapList.append(i)
        mapList = newMapList
    return [dbutils.fs.ls(item) for item in mapList]


#Input path here
pathToSearch = "/mnt/mount_point/directory"
#cores = os.cpu_count()

startTime = time.time()
with cf.ThreadPoolExecutor() as executor:
    #result = executor.map(findSize, mapLister(pathToSearch, cores))
    result = executor.map(findSize, [dbutils.fs.ls(pathToSearch)])

totalSize = sum([int(size) for size in result])
print(f"Total size of {pathToSearch} is {totalSize/(1024*1024)} Mb")
print(f"time taken: {time.time()-startTime}s")
