import os
import time
import sys

process_num = 10;

def write(blocksize, blockcount):
    chunk = os.urandom(blocksize)
    name = str(os.getpid())
    out = os.open("./out/" + name, os.O_CREAT | os.O_WRONLY)
    count = 0
    start = time.clock()
    for _ in range(blockcount):
        os.write(out, chunk)
        count += 1
    os.fsync(out)
    time_elapsed = time.clock() - start
    print ("average latency:{}ms").format((time_elapsed / count) * 1000)
    return count / time_elapsed


def summerize(pid_list):
    stats = []
    for pid in pid_list:
        name = "./log/" + str(pid) + "stat.out"
        log_in = open(name, "r")
        stats.append(float(log_in.read()))
    return sum(stats)


if __name__ == "__main__":
    blocksize = int(sys.argv[1]) * 1024
    blockcount = int(sys.argv[2])
    processes = []
    read_source = []

    if os.path.exists("./out"):
        os.system("rm -rf ./out")
    os.mkdir("./out")

    if os.path.exists("./log"):
        os.system("rm -rf ./log")
    os.mkdir("./log")
    for i in range(process_num):
        t = os.fork()
        if t == 0:
            # Child process
            name = str(os.getpid()) + "stat.out"
            result = write(blocksize, blockcount)
            #print "{}Mb/s".format(result * blocksize/ 1024**2)
            log_out = open("./log/" + name, "wb")
            log_out.write(str(result))
            log_out.close()
            os._exit(0)
        else:
            # Parent process
            # read_source.append(r)
            processes.append(t)

    for pid_t in processes:
        os.waitpid(pid_t, 0)

    stat = summerize(processes)
    print stat 
    