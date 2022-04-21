# UniKV atop RocksDB DevLog

[TOC]



### [done]增加level0文件数量

由参数 level0_file_num_compaction_trigger 控制

file path: rocksdb/include/rocksdb/options.h

```
  // Dynamically changeable through SetOptions() API
  int level0_file_num_compaction_trigger = 4;
```

change: modified to 40



### [todo]索引结构定义

### [todo]索引接口实现

### [todo]接口嵌入