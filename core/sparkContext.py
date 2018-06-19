## usage: sc = sparkContext()
## rdd = sc.textfromfile(path)
## transformation: rdd = rdd.map(function), rdd = rdd.filter(function)
## action:  rdd.count()
from core.local_scheduler import local_scheduler
from core.RDD import RDD
class sparkContext:
    def __init__(self):
        self.scheduler = local_scheduler()
        #self.scheduler.start()
        pass
    def textfromfile(self,path,minisplit):
        return RDD()

    def paralize(self,list,nbsplit):
        return RDD(list, nbsplit)

    def runjob(self,partition_job,indice=None,scan=False):
        if not scan:
            pass
