# UniKV atop RocksDB DevLog

- [UniKV atop RocksDB DevLog](#unikv-atop-rocksdb-devlog)
    - [[done]增加level0文件数量](#done增加level0文件数量)
    - [[todo]索引结构定义](#todo索引结构定义)
    - [[todo]索引接口实现](#todo索引接口实现)
    - [[todo]接口嵌入](#todo接口嵌入)



### [done]增加level0文件数量

由参数 level0_file_num_compaction_trigger 控制
file path: rocksdb/include/rocksdb/options.h

```
  // Dynamically changeable through SetOptions() API
  int level0_file_num_compaction_trigger = 4;
```

change: modified to 40



### [todo]索引结构定义
file path: rocksdb/include/rocksdb/uni_index.h (new created)


### [todo]索引接口实现

### [todo]接口嵌入