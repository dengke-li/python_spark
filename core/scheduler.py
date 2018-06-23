class scheduler:
    def __init__(self):
        pass
    ## call the final RDD's action can provoque parent's rdd action
    def runJob(self, rdd, func , partitions, allowLocal=True):
        return rdd.compute()