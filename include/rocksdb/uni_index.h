// headerfile for new index based unikv
// author: ADSL developers


using namespace std;
typedef unsigned char byte;

struct ListIndexEntry{
    byte KeyTag[2];
    byte TableNum[2];
    ListIndexEntry* nextEntry;
};


const int NumPartitionMax = 50;// should >= level0_file_num_compaction_trigger?
struct ListIndexEntry *CuckooHashIndex[NumPartitionMax];

void initHashIndex();
void persistentHashTable();
void compactHashIndexTable();
void compactHashIndexPartitionTable(int partition);
void recoveryHashTable();
static void* recordHashIndex(void *paraData);