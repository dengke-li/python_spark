class scheduler:
    def __init__(self):
        pass
    def runJob(self, rdd, func , partitions, allowLocal=True):
        return rdd.compute()