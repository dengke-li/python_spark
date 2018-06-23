from core.sparkContext import sparkContext
def test1():
    sc = sparkContext()
    # rdd = sc.textfromfile('/home/ubuntu',6)
    list = [2,3,4,5]
    rdd = sc.parallelize(list,2)

    def a(x):
        return x+1
    rdd = rdd.map(a)
    #r = rdd.count()
    r = rdd.collect()
    print(r)

test1()
