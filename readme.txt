总体代码结构说明文档：
  |
  +--- port           <=== 提供各个平台的基本接口
  |
  +--- util           <=== 提供一些通用工具类
  |
  +--- helpers
  |      |
  |      +--- memenv  <=== Env的一个具体实现(Env是leveldb封装的运行环境)
  |
  +--- table          <=== 磁盘数据结构
  |
  +--- db             <=== db的所有实现
  |
  +--- doc
  |     |
  |     +--- table_format.txt   <=== 磁盘文件数据结构说明
  |     |
  |     +--- log_format.txt     <=== 日志文件（用于宕机恢复未刷盘的数据）数据结构说明
  |     |
  |     +--- impl.html          <=== 一些实现
  |     |
  |     +--- index.html         <=== 使用说明
  |     |
  |     +--- bench.html         <=== 测试数据
  |
  +--- include
         |
         +--- leveldb           <=== 所有头文件
                |
                +--- c.h               <=== 提供给C的接口
                +--- cache.h
                +--- comparator.h
                +--- db.h
                +--- env.h
                +--- filter_policy.h
                +--- iterator.h
                +--- options.h
                +--- slice.h
                +--- status.h
                +--- table.h
                +--- table_builder.h
                +--- write_batch.h



util目录介绍：
  |
  +--- random.h          <=== 一个简单的随机数生成器
  |
  +--- status.cc         <=== leveldb读写API的返回状态(头文件: include/leveldb/status.h)
  |
  +--- crc32c.h          <=== crc32 hash算法，本系列文章不予分析
  +--- crc32c.cc
  |
  +--- hash.h            <=== hash函数，具体的实现类似murmur hash
  +--- hash.cc
  |
  +--- mutexlock.h       <=== 封装Mutex
  |
  +--- options.cc        <=== 数据库的属性选项(头文件: include/leveldb/options.h)
  |
  +--- histogram.cc      <=== 直方图，提供给性能评测工具使用
  +--- histogram.h
  |
  +--- coding.h          <=== 数字编码的实现(如变长编码)
  +--- coding.cc
  |
  +--- filter_policy.cc  <=== filter, (头文件: include/leveldb/filter_policy.h)
  |
  +--- bloom.cc          <=== bloom filter (布隆过滤器)
  |
  +--- logging.h         <=== 日志中的数据处理(用于commitlog)
  +--- logging.cc
  |
  +--- posix_logger.h    <=== 系统写日志的操作(可读日志)
  |
  +--- comparator.cc     <=== 比较器(头文件: include/leveldb/comparator.h)
  |
  +--- env.cc            <=== 抽象的系统运行环境(头文件: include/leveldb/env.h)
  +--- env_posix.cc      <=== 基于posix实现的具体运行环境
  |
  +--- arena.h           <=== arena是一个用于内存分配的环形缓冲区
  +--- arena.cc
  |
  +--- cache.cc          <=== LRUCache实现(头文件: include/leveldb/cache.h)


table目录介绍：
  |
  +--block.h    // block在内存中的数据结构
  +--block.cc
  |
  +--block_builder.h   // 
  +--block_builder.cc
  |
  +--filter_block.h
  +--filter_block.cc
  |
  +--format.h
  +--format.cc
  |
  +--iterator.cc
  +--iterator_wrapper.h
  |
  +--merger.h
  +--merger.cc
  |
  +--table.cc
  +--table_builder.cc
  |
  +--two_level_iterator.h
  +--two_level_iterator.cc

port目录介绍：
  |
  +--- port.h   <=== 对外提供的头文件，根据不同平台include不同的头文件，无需分析
  |
  +--- atomic_pointer.h   <=== 支持原子操作的指针
  |
  +--- port_posix.h       <=== posix平台
  +--- port_posix.cc
  |
  +--- port_android.h     <=== android平台
  +--- port_android.cc
  |
  +--- port_example.h     <=== 为支持leveldb在其他平台提供的一个实现接口的例子
  |
  +--- win
        |
        +--- stdint.h     <=== windows平台（关于windows平台的知识我不懂，因此不分析）



