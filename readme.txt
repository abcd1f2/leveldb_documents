�������ṹ˵���ĵ���
  |
  +--- port           <=== �ṩ����ƽ̨�Ļ����ӿ�
  |
  +--- util           <=== �ṩһЩͨ�ù�����
  |
  +--- helpers
  |      |
  |      +--- memenv  <=== Env��һ������ʵ��(Env��leveldb��װ�����л���)
  |
  +--- table          <=== �������ݽṹ
  |
  +--- db             <=== db������ʵ��
  |
  +--- doc
  |     |
  |     +--- table_format.txt   <=== �����ļ����ݽṹ˵��
  |     |
  |     +--- log_format.txt     <=== ��־�ļ�������崻��ָ�δˢ�̵����ݣ����ݽṹ˵��
  |     |
  |     +--- impl.html          <=== һЩʵ��
  |     |
  |     +--- index.html         <=== ʹ��˵��
  |     |
  |     +--- bench.html         <=== ��������
  |
  +--- include
         |
         +--- leveldb           <=== ����ͷ�ļ�
                |
                +--- c.h               <=== �ṩ��C�Ľӿ�
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



utilĿ¼���ܣ�
  |
  +--- random.h          <=== һ���򵥵������������
  |
  +--- status.cc         <=== leveldb��дAPI�ķ���״̬(ͷ�ļ�: include/leveldb/status.h)
  |
  +--- crc32c.h          <=== crc32 hash�㷨����ϵ�����²������
  +--- crc32c.cc
  |
  +--- hash.h            <=== hash�����������ʵ������murmur hash
  +--- hash.cc
  |
  +--- mutexlock.h       <=== ��װMutex
  |
  +--- options.cc        <=== ���ݿ������ѡ��(ͷ�ļ�: include/leveldb/options.h)
  |
  +--- histogram.cc      <=== ֱ��ͼ���ṩ���������⹤��ʹ��
  +--- histogram.h
  |
  +--- coding.h          <=== ���ֱ����ʵ��(��䳤����)
  +--- coding.cc
  |
  +--- filter_policy.cc  <=== filter, (ͷ�ļ�: include/leveldb/filter_policy.h)
  |
  +--- bloom.cc          <=== bloom filter (��¡������)
  |
  +--- logging.h         <=== ��־�е����ݴ���(����commitlog)
  +--- logging.cc
  |
  +--- posix_logger.h    <=== ϵͳд��־�Ĳ���(�ɶ���־)
  |
  +--- comparator.cc     <=== �Ƚ���(ͷ�ļ�: include/leveldb/comparator.h)
  |
  +--- env.cc            <=== �����ϵͳ���л���(ͷ�ļ�: include/leveldb/env.h)
  +--- env_posix.cc      <=== ����posixʵ�ֵľ������л���
  |
  +--- arena.h           <=== arena��һ�������ڴ����Ļ��λ�����
  +--- arena.cc
  |
  +--- cache.cc          <=== LRUCacheʵ��(ͷ�ļ�: include/leveldb/cache.h)


tableĿ¼���ܣ�
  |
  +--block.h    // block���ڴ��е����ݽṹ
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

portĿ¼���ܣ�
  |
  +--- port.h   <=== �����ṩ��ͷ�ļ������ݲ�ͬƽ̨include��ͬ��ͷ�ļ����������
  |
  +--- atomic_pointer.h   <=== ֧��ԭ�Ӳ�����ָ��
  |
  +--- port_posix.h       <=== posixƽ̨
  +--- port_posix.cc
  |
  +--- port_android.h     <=== androidƽ̨
  +--- port_android.cc
  |
  +--- port_example.h     <=== Ϊ֧��leveldb������ƽ̨�ṩ��һ��ʵ�ֽӿڵ�����
  |
  +--- win
        |
        +--- stdint.h     <=== windowsƽ̨������windowsƽ̨��֪ʶ�Ҳ�������˲�������



