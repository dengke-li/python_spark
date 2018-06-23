class scheduler:
    def __init__(self):
        pass
    ## call the final RDD's action can provoque parent's rdd action
    def runJob(self, rdd, func , partitions, allowLocal=True):
        result = []
        for partition in partitions:
            print(partition)
            split = rdd.splits[partition]
            result.append(rdd.compute(split))
        return result