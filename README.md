参考[http://oserror.com/distributed/mapreduce-implementation-in-golang/?utm_source=tuicool&utm_medium=referral](http://oserror.com/distributed/mapreduce-implementation-in-golang/?utm_source=tuicool&utm_medium=referral)  这篇文章 , 把它从单机版本, 改成了多机分布式版本.(单master, 多worker)

主要流程是: 

1. master接收数据分片, 下发文件路径给map worker

2. map worker根据路径向master获取分片数据, 以分片文件名保存到本地

3. 每次map worker计算完数据后, 将本地的结果中间文件路径注册到master中
    
4. reduce worker向master查询map结果中间文件所有的map worker路径. 然后向map worker获取中间结果文件. 

5. reduce worker处理完数据后, 将本地的结果文件路径再汇报给master

6. master在合并文件时, 向所有的reduce worker索要结果文件, 下载到本地, 执行合并


功能比较low, 就图个好玩哈. 
