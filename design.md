# ipfs pin manager

## briefing design
- 接受http请求,处理请求中包含的ipfs cid
- 用消息队列接受http请求中cid,消息队列接口化,可以替换成不同的实现,最好有本地有持久化的内嵌式消息队列(不确定有没有合适的框架),提供监控或api查询队列中消息数量
- 有一个pin工作池用来处理队列中的cid,可以配置工作池的worker数量
- 增加一个模块,这个模块会根据cid对应文件的大小来给cid设定pin的时间,小文件周期保存的长,大文件保存的短暂,可以设计一个公式或者一个配置列表,用公式时要设定一个上下限
- 还需要一个独立的gc线程用来周期性的触发ipfs的gc用来回收硬盘空间,需要记录每次gc开始的时间戳和结束的时间戳,gc前的磁盘容量,gc后的磁盘容量等
- 需要一个内嵌数据库(初步考虑用rocksdb)保存每个cid的生命周期的状态,如接受到http请求的时间戳,开始执行pin的时间戳,pin成功的时间戳,pin失败的时间戳,pin失败的次数,unpin的时间戳...
- 有一个vector用来保存活跃cid(还没有执行unpin成功的),设计中用cids表示
- 需要一个unpin工作池,用来轮询cids,根据每个cid能否存活(pin状态的持续时间),当pin的周期结束了就调用ipfs的unpin api,需要记录unpin的时间戳,unpin成功的时间戳等
- 增加pin处理的时常的监控(prometheus)
- 需要考虑资源互斥管理问题,ipfs的gc是全局的,当对一个新的cid执行pin操作时,假设文件下载到本地了,这个文件很大(比如有100000个子cid),会不会有部分cid还没来得及pin,此时发生了gc,导致已经下载到本地的文件的部分内容被gc?请结合最新的ipfs的设计来判断和设计,要保证文件的完整性
- 需要考虑ipfs pin的机制,假设一个cid对应的文件有100个子cid,当我对主cid执行ipfs 的pin api时是否所有的子cid都会pin,如果不是那么当gc后文件就不完整了
- 在设计整个系统时,你需要对ipfs的api的输入输出有充分完整的理解

## 状态管理对象设计(cid state management object-->CSMO)
TODO: 对每个cid设计整个管理周期中需要的状态

## module design
TODO: 根据CSMO和,briefing design列出可能需要的模块,以及模块之间的关系

## module detail design
TODO: 根据CSMO,briefing design和module design对每个模块进行详细设计