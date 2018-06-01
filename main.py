import os
import time
import sys


# process_num = 4;

#class io_benchmark:

def shuffle(a):
    """ Shuffle function specialized for I/O test
    """
    for i in range(1, len(a) / 2):
        a[i], a[len(a) - i] = a[len(a) - i], a[i] 


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

def write_test(blocksize, blockcount, random_access = True):
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
    offsets = list(range(0, blockcount * blocksize, blocksize))
    if (random_access):
        shuffle(offsets)

    for offset in offsets:
        start = time.time()
        os.lseek(out, offset, os.SEEK_SET)
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

def auto_test(blocksize, blockcount, process_num, test_mode):
    """ Perform auto test with given configuration
    Return a tuple: (iops, average_latency(ms))
    """
    processes = []
    stats = []
    initial()
   
    for _ in range(process_num):
        t = os.fork()
        if t == 0:
            # Child process
            name = str(os.getpid()) + 'stat.out'
            if (test_mode == 'RR'):
                result = read_test(blocksize, blockcount)
            elif test_mode == 'SR':
                result = read_test(blocksize, blockcount, False)
            elif test_mode == 'RW':
                result = write_test(blocksize, blockcount)
            elif test_mode == 'SW':
                result = write_test(blocksize, blockcount, False)
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
    result = (sum(avg_iops),
            float(sum(avg_latency) / len(avg_latency)))

    #remove all temp files
    cleanup()
    return result




if __name__ == "__main__":
    # blocksize = int(sys.argv[1]) * 1024
    # blockcount = int(sys.argv[2])
    # process_num = int(sys.argv[3])
    # test_mode = sys.argv[4]
    log = open("./stats.csv", "w+")
    
    blockcount = 40000
    test_mode = ["SR", "SW", "RR", "RW"]

    #bs_list: all blocksize to be tested(bytes)
    bs_list = []
    for p in range(8, 9):
        bs_list.append(2 ** p)

    #num_list: numbers of process to be tested
    num_list = [1, 2, 4, 8, 16, 32, 60]
    
    #title for the csv file
    title = "process_number,blocksize,avg_latency,IOPS, mode"

    #for accuracy, we repeat each experiment a few times
    repeat_time = 2

    for mode in test_mode:
        for num in num_list:
            for blocksize in bs_list:
                iops = []
                latency = []
                print "Performing test: {} processes, blocksize {}, mode {}...".format(num, blocksize / 1024.0, mode)
                for i in range(repeat_time):
                    a, b = auto_test(blocksize, blockcount, num, mode)
                    iops.append(a)
                    latency.append(b)
                log.write(str(num) + ',' + 
                            str(blocksize / 1024.0) + ',' + 
                            str(1.0 * sum(iops) / repeat_time) + ',' +
                            str(1.0 * sum(latency) / repeat_time) + ',' +
                            mode + "\n")

    print "writing log..."
    log.close()
    print "Done!"