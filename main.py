import os
import time
import sys

process_num = 5;

def write(blocksize, blockcount):
    chunk = os.urandom(blocksize)
    name = str(os.getpid())
    out = os.open("./out/" + name, os.O_CREAT | os.O_WRONLY | os.O_SYNC)
    count = 0
    start = time.clock()
    for _ in range(blockcount):
        os.write(out, chunk)
        count += 1
    time_elapsed = time.clock() - start
    return count / time_elapsed


if __name__ == "__main__":
    blocksize = int(sys.argv[1]) * 1024
    blockcount = int(sys.argv[2])
    for i in range(process_num):
        t = os.fork()
        if t == 0:
            #print os.getpid()
            result = write(blocksize, blockcount)
            print ("{}ops/s").format(result)
            os._exit(0)

