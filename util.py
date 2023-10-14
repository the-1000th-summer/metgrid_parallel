"""_"""

import psutil

def all_equal(iterator):
    iterator = iter(iterator)
    try:
        first = next(iterator)
    except StopIteration:
        return True
    return all(first == x for x in iterator)

def getPhysicalCPUCoreNum():
    """_"""
    return psutil.cpu_count(logical=False)
