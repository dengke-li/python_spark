## usage: sc = sparkContext()
## rdd = sc.textfromfile(path)
## transformation: rdd = rdd.map(function), rdd = rdd.filter(function)
## action:  rdd.count()
from core.local_scheduler import local_scheduler
from core.RDD import parallelizeRDD
class sparkContext:
    def __init__(self):
        self.scheduler = local_scheduler()
        #self.scheduler.start()
        pass
    def textfromfile(self, path, minisplit):
        return RDD()

    def parallelize(self, list, nbsplit):
        interval = len(list) // nbsplit
        splits = []
        for i in range(nbsplit):
            splits.append(list[i*interval:interval*i+interval])
        return parallelizeRDD(self, splits)

    def runjob(self,rdd, func,indice=None,scan=False):
        if scan:
            newlist = []
            result = []
        else:
            partitions = range(len(rdd.splits))
            print('start')
            print(rdd.splits)
            result = self.scheduler.runJob(rdd, func, partitions, allowLocal=True)
            # rdd.splits
            # interval = len(rdd.prev.seq)//nbsplit
            # for i in range(nbsplit):
            #     partitions.append(rdd.prev.seq[i*interval:interval*i+interval])
            # newlist = [[action_f(rdd.f(e)) for e in partlist] for partlist in partitions]
        print(result)
        return result
