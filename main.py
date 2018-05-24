import os
import time
import sys
from random import shuffle

# process_num = 4;

#class io_benchmark:


def dump(blocksize, blockout):
    """ Dump a bianry file to temp directory
    The file is used to test read speed
    """
    chunk = os.urandom(blocksize)
    dump = os.open("/tmp/source", os.O_CREAT | os.O_WRONLY)
    for _ in range(blockcount):
        os.write(dump, chunk)
    os.close(dump)


def read_test(blocksize, blockcount, random_access = True):
    """ Test read speed(single process)
    
    Read a temporary file under ./tmp created by dump()

    Args:
        blocksize : size of a single block
        blockcount : number of blocks to read
        random_access: read block by block or in random order(default:True)

    Returns:
        tuple: (operations per second,
                average latency(millisecond))
    """
    count = 0
    dump(blocksize, blockcount)
    out = os.open("/tmp/source", os.O_RDONLY)
    offsets = list(range(0, blockcount * blocksize, blocksize))
    if (random_access):
        shuffle(offsets)
    took = []

    for offset in offsets:
        start = time.time()
        os.lseek(out, offset, os.SEEK_SET)
        buf = os.read(out, blocksize)
        time_elapsed = time.time() - start
        if not buf:
            break
        count += 1
        took.append(time_elapsed)
    os.close(out)
    return (count / sum(took), (sum(took) / count) * 1000)

def write_test(blocksize, blockcount):
    """ Test write speed(single process)
    
    Write to a temporary file under the ./tmp/ named by
    current process's PID

    Args:
        blocksize : size of a single block
        blockcount : number of blocks to read

    Returns:
        tuple: (operations per second,
                average latency(millisecond))
    """
    took = []
    chunk = os.urandom(blocksize)
    name = str(os.getpid())
    out = os.open("/tmp/" + name, os.O_CREAT | os.O_WRONLY)
    count = 0
    for _ in range(blockcount):
        start = time.time()
        os.write(out, chunk)
        time_elapsed = time.time() - start
        count += 1
        took.append(time_elapsed)
    os.close(out)
    return (count / sum(took), (sum(took) / count) * 1000)


def summerize(pid_list):
    stats = []
    for pid in pid_list:
        name = "./log/" + str(pid) + "stat.out"
        log_in = open(name, "r")
        iops, latency = log_in.read().strip('()').split(',')
        stat = (float(iops), float(latency))
        stats.append(stat)
    return stats


def cleanup():
    if os.path.exists("./out"):
        os.system("rm -rf ./out")
    if os.path.exists("./log"):
        os.system("rm -rf ./log")
    if os.path.exists("./tmp"):
        os.system("rm -rf ./tmp")

def initial():
    cleanup()
    os.mkdir("./log")
    os.mkdir("./out")

if __name__ == "__main__":
    blocksize = int(sys.argv[1]) * 1024
    blockcount = int(sys.argv[2])
    process_num = int(sys.argv[3])
    test_mode = sys.argv[4]
    processes = []
    read_source = []

    initial()
   
    for i in range(process_num):
        t = os.fork()
        if t == 0:
            # Child process
            name = str(os.getpid()) + 'stat.out'
            if (test_mode == 'R'):
                result = read_test(blocksize, blockcount)
            else:
                result = write_test(blocksize, blockcount)
            with open("./log/" + name, 'w') as out:
                out.write(str(result))
            os._exit(0)
        else:
            # Parent process
            processes.append(t)

    for pid_t in processes:
        os.waitpid(pid_t, 0)

    stats = summerize(processes)
    avg_iops = []
    avg_latency = []
    for stat in stats:
        avg_iops.append(stat[0])
        avg_latency.append(stat[1])
    print sum(avg_iops) * blocksize / 1024 ** 2
    print float(sum(avg_latency) / len(avg_latency))

    #remove all temp files
    cleanup()