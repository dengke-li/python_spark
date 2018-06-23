
class RDD:
    def __init__(self, sc, splits):
        self.nb_line = 0
        self.splits = splits
        self.sc = sc
        pass
    ## transformation should not do the execution, ie: lazy evaluation, just return back a object with all data
    ## and method to calculate, then it is later steps will decide to call these method or not
    def map(self,function):
        # newlist = [function(e) for e in self.seq]
        # return RDD(newlist)
        ## return MappedRDD(self.seq, function) ## here you can pass seq, but more clever is pass the object,
                                               ## then you can get more information
        return MappedRDD(self, self.sc, function, self.splits)

    def filter(self, function):
        return FilterRDD(self, self.sc, function, self.splits)

    # def count(self):
    #     #return len(self.seq)
    #     def count_f(iterator): ## here should be action on each split data
    #         length = 0
    #         while(iterator.hasnext):
    #             length +=1
    #         return length
    #     return sum(self.compute(count_f, self.splits))
    #
    # def take(self, n):
    #     def take_f(iterator):
    #         #for p in range(self.nbsplit)
    #         length = 0
    #         while iterator.hasnext:
    #             length +=1
    #             if length==n:
    #                 return
    #
    #     list_of_list = self.compute(take_f, self.splits, True)

        #return self.collect()[:n] ## here you can see if the compute result is iterator, then take method() don't need to first call collect
    ## result is list of list, same for different RDD
    def compute(self):
        pass

    def collect(self):
        #return self.seq
        def collect_f(iterator):
            return iterator
        list_of_list = self.sc.runjob(self, collect_f)
        print(123)
        print(list_of_list)
        return sum(list_of_list, []) ## concatenate list of list


class parallelizeRDD(RDD):
    def compute(self):
        result = []
        return self.splits


class MappedRDD(RDD):
    def __init__(self, rdd, sc, f, splits):
        self.prev = rdd
        self.sc = sc
        self.f = f
        self.splits = splits

    def compute(self, split):
        return list(map(self.f, split))

class FilterRDD(RDD):
    def __init__(self, rdd, sc, f, splits):
        self.prev = rdd
        self.sc = sc
        self.f = f
        self.splits = splits

    def compute(self, split):

        return [e for e in split if self.f(e)]