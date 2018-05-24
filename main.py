import os
import time
import sys

# process_num = 4;

def write(blocksize, blockcount):
    """
    Wi
    """
    took = []
    chunk = os.urandom(blocksize)
    name = str(os.getpid())
    out = os.open("./out/" + name, os.O_CREAT | os.O_WRONLY)
    count = 0
    for _ in range(blockcount):
        start = time.time()
        os.write(out, chunk)
        time_elapsed = time.time() - start
        count += 1
        took.append(time_elapsed)
    os.close(out)
    #print ("average latency:{}ms").format((time_elapsed / count) * 1000)
    print sum(took)
    return (count / time_elapsed, (sum(took) / count) * 1000)


def summerize(pid_list):
    stats = []
    for pid in pid_list:
        name = "./log/" + str(pid) + "stat.out"
        log_in = open(name, "r")
        iops, latency = log_in.read().strip('()').split(',')
        stat = (float(iops), float(latency))
        stats.append(stat)
    return stats


if __name__ == "__main__":
    blocksize = int(sys.argv[1]) * 1024
    blockcount = int(sys.argv[2])
    process_num = int(sys.argv[3])
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

    stats = summerize(processes)
    avg_iops = []
    avg_latency = []
    for stat in stats:
        avg_iops.append(stat[0])
        avg_latency.append(stat[1])
    print sum(avg_iops) * blocksize / 1024 ** 2
    print float(sum(avg_latency) / len(avg_latency))