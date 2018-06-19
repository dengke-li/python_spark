
class RDD:
    def __init__(self, seq, nbsplit):
        self.nb_line = 0
        self.seq = seq
        self.nbsplit = nbsplit
        pass
    ## transformation should not do the execution, ie: lazy evaluation, just return back a object with all data
    ## and method to calculate, then it is later steps will decide to call these method or not
    def map(self,function):
        # newlist = [function(e) for e in self.seq]
        # return RDD(newlist)
        ## return MappedRDD(self.seq, function) ## here you can pass seq, but more clever is pass the object,
                                               ## then you can get more information
        return MappedRDD(self, function, self.nbsplit)

    def filter(self, function):
        return RDD()

    def count(self):
        #return len(self.seq)
        def count_f(iterator): ## here should be action on each split data
            length = 0
            while(iterator.hasnext):
                length +=1
            return length
        return sum(self.compute(count_f, self.nbsplit))

    def take(self, n):
        def take_f(iterator):
            #for p in range(self.nbsplit)
            length = 0
            while iterator.hasnext:
                length +=1
                if length==n:
                    return

        list_of_list = self.compute(take_f, self.nbsplit, True)

        #return self.collect()[:n] ## here you can see if the compute result is iterator, then take method() don't need to first call collect

    def compute(self,action_f, nbsplit, scan_onebyone=False):
        pass

    def collect(self):
        #return self.seq
        def collect_f(iterator):
            return iterator
        list_of_list = self.compute(collect_f,self.nbsplit)
        return sum(list_of_list, []) ## concatenate list of list


class MappedRDD(RDD):
    def __init__(self,rdd,f,nbsplit):
        self.prev = rdd
        self.f = f
        self.nbsplit = nbsplit

    def compute(self,action_f, nbsplit, scan_onebyone=False):## here merge the mapping function f and action function
        #  together, in face, we should use sc.runjob to use the call action function, and let here only focus on the mapping part
        if scan_onebyone:
            newlist = []
        else:
            partitions = []
            interval = len(self.prev.seq)//nbsplit
            for i in range(nbsplit):
                partitions.append(self.prev.seq[i*interval:interval*i+interval])
            newlist = [[action_f(self.f(e)) for e in partlist] for partlist in partitions]
        print(newlist)
        return newlist