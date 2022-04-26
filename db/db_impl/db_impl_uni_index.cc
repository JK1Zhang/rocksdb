#include <stdio.h>
#include <fstream>
#include "rocksdb/hashfunc.h"
#include "db_impl.h"

using namespace std;
namespace rocksdb {



void DBImpl::initHashIndex(){
    for(int p=0;p<=NewestPartition;p++){
      CuckooHashIndex[p]=new ListIndexEntry[config::bucketNum];
      //overflowBucket[p]=new ListIndexEntry[config::bucketNum];
      for(int i=0;i<config::bucketNum;i++){
        CuckooHashIndex[p][i].KeyTag[0]=0;
        CuckooHashIndex[p][i].KeyTag[1]=0;
        CuckooHashIndex[p][i].TableNum[0]=0;
        CuckooHashIndex[p][i].TableNum[1]=0;
        CuckooHashIndex[p][i].TableNum[2]=0;
        CuckooHashIndex[p][i].nextEntry=NULL;
        //HashIndex[i].nextEntry=NULL;
      }
    }
}

//persistent hash index into a disk file
void DBImpl::persistentHashTable(){
  ofstream hashFile;
  hashFile.open(config::HashTableFile);
  for(int p=0;p<=NewestPartition;p++){
    for(int i=0;i<config::bucketNum;i++){
      ListIndexEntry *lastEntry=&CuckooHashIndex[p][i];
          if(bytes2ToInt(lastEntry->TableNum)!=0){
            while(lastEntry!=NULL){
              hashFile<<p<<","<<i<<",";
                hashFile<<(unsigned int)lastEntry->KeyTag[0]<<","<<(unsigned int)lastEntry->KeyTag[1]<<",";
                hashFile<<bytes2ToInt(lastEntry->TableNum)<<";";	  
                lastEntry=lastEntry->nextEntry;
            }
            hashFile<<"\n";
          }
          lastEntry=&CuckooHashIndex[p][i];
          if(bytes2ToInt(lastEntry->TableNum)==0 && lastEntry->nextEntry!=NULL){
            lastEntry=lastEntry->nextEntry;
            while(lastEntry!=NULL){
                hashFile<<p<<","<<i<<",";
                hashFile<<(unsigned int)lastEntry->KeyTag[0]<<","<<(unsigned int)lastEntry->KeyTag[1]<<",";
                hashFile<<bytes2ToInt(lastEntry->TableNum)<<";";
                lastEntry=lastEntry->nextEntry; 
            }
            hashFile<<"\n";
          }
    }
  }
  hashFile.close();
}
/*
void DBImpl::compactHashIndexTable(){
  for(int k=0;k<=NewestPartition;k++){
      if(versions_->NumLevelFiles(0,k) >=config::kL0_CompactionTrigger*4/5){
        Compaction* c = versions_->PickSomeHashIndexFileCompaction(k);///
        CompactionState* compact = new CompactionState(c);
        fprintf(stderr,"before compactHashIndexTable L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,k),versions_->NumLevelFiles(1,k),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,k));
        Status  status= DoCompactionWork(compact,k,false);
        if (!status.ok()) {
          RecordBackgroundError(status);
        }
        CleanupCompaction(compact,k);
        c->ReleaseInputs();
        DeleteObsoleteFiles(k);//delete old SSTables
        delete c;
	    }   
  }
}

void DBImpl::compactHashIndexPartitionTable(int partition){
	  Compaction* c = versions_->PickSomeHashIndexFileCompaction(partition);///
	  CompactionState* compact = new CompactionState(c);
	  printf("before compactHashIndexPartitionTable L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
	  Status  status= DoCompactionWork(compact,partition,false);
	  if (!status.ok()) {
	    RecordBackgroundError(status);
	  }
	  CleanupCompaction(compact,partition);
	  c->ReleaseInputs();
	  DeleteObsoleteFiles(partition);//delete old SSTables
	  delete c;
}
*/

//recovery the hash index from a disk file
void DBImpl::recoveryHashTable(){
  ifstream hashFile;
  hashFile.open(config::HashTableFile);
  if (!hashFile){ 
      printf("open fail\n");     
      fprintf(stderr,"open fail\n");     
  }
  unsigned int partitionNumber=0,tag0,tag1,fileID,bucketNumber=0;
  char temp;
  while(hashFile>>partitionNumber){
      hashFile>>temp>>bucketNumber >> temp >> tag0 >> temp>>tag1>>temp>>fileID>>temp;
          //fprintf(stderr,"partitionNumber:%d,bucketNumber:%d,tag0:%d,tag1:%d,fileID:%d\n",partitionNumber,bucketNumber,tag0,tag1,fileID);
          byte* tableNumBytes=new byte[2];
          intTo2Byte(fileID,tableNumBytes);
          ListIndexEntry *lastEntry=&(CuckooHashIndex[partitionNumber][bucketNumber]);
          int mytableNum=bytes2ToInt(lastEntry->TableNum);
        if(mytableNum==0){
          //fprintf(stderr,"--partitionNumber:%d,bucketNumber:%d,tag0:%d,tag1:%d,fileID:%d\n",partitionNumber,bucketNumber,tag0,tag1,fileID);
          lastEntry->KeyTag[0]=(byte)tag0;
          lastEntry->KeyTag[1]=(byte)tag1;
          lastEntry->TableNum[0]=tableNumBytes[0];
          lastEntry->TableNum[1]=tableNumBytes[1];
          lastEntry->TableNum[2]=tableNumBytes[2];
        }else{
          ListIndexEntry *beginNextEntry=lastEntry->nextEntry;		
          ListIndexEntry *addEntry=new ListIndexEntry;
          addEntry->KeyTag[0]=(byte)tag0;
          addEntry->KeyTag[1]=(byte)tag1;
          addEntry->TableNum[0]=tableNumBytes[0];
          addEntry->TableNum[1]=tableNumBytes[1];
          addEntry->TableNum[2]=tableNumBytes[2];
          
          lastEntry->nextEntry=addEntry;
          addEntry->nextEntry=beginNextEntry;
          // printf("addEntry bucket:%u,tag0:%u,tag1:%u,fileID:%u\n",bucketNumber,addEntry->KeyTag[0],addEntry->KeyTag[1],bytes3ToInt(addEntry->TableNum));
        }
        delete []tableNumBytes;
    } 
  hashFile.close();
}

//record the location of KV pairs in hash index
void* DBImpl::recordHashIndex(void* paraData ){
  struct recordIndexPara *myPara=(struct recordIndexPara*)paraData;
  HashFunc curHashfunc;
  ListIndexEntry* curHashIndex=myPara->curHashIndex;
  //ListIndexEntry* overflowBucket=myPara->overflowBucket;
  byte* tableNumBytes=new byte[2];
  intTo2Byte(int(myPara->number),tableNumBytes);
  Iterator* iter=myPara->memIter;
  iter->SeekToFirst();
   for (; iter->Valid(); iter->Next()){
      Slice key = iter->key();    
      byte* keyBytes=new byte[4];
      unsigned int intKey;
      unsigned int hashKey;
      if(strlen(key.data())>20){
            intKey=strtoul(key.ToString().substr(4,config::kKeyLength).c_str(),NULL,10);
            hashKey=curHashfunc.RSHash((char*)key.ToString().substr(0,config::kKeyLength).c_str(),config::kKeyLength);
          //printf("myKey.size():%d,myKey.data():%s,intKey:%u\n",strlen(myKey.data()),myKey.ToString().substr(0,keyLength).c_str(),intKey);
      }else{
            intKey=strtoul(key.ToString().c_str(),NULL,10);
            hashKey=curHashfunc.RSHash((char*)key.ToString().c_str(),config::kKeyLength-8);
            //fprintf(stderr,"myKey.size():%d,myKey.data():%s,intKey:%d\n",(int)strlen(key.data()),key.data(),intKey);
      }	
      int keyBucket=intKey%config::bucketNum;
      intTo4Byte(hashKey,keyBytes);///
      //	printf( "intKey:%u,hashKey:%u,bucket:%d,%s,keyBytes[2]:%u,keyBytes[3]:%u\n",intKey,hashKey,keyBucket,(char*)myKey.ToString().substr(0,config::kKeyLength).c_str(),(unsigned int )keyBytes[2],(unsigned int )keyBytes[3]);


      int findEmptyBucket=0;
        for(int k=0;k<config::cuckooHashNum;k++){
          keyBucket=curHashfunc.cuckooHash((char*)key.ToString().substr(0,config::kKeyLength).c_str(),k,config::kKeyLength);
          int mytableNum=bytes2ToInt(curHashIndex[keyBucket].TableNum);
          //if(curHashIndex[keyBucket].KeyTag[0]==0 && curHashIndex[keyBucket].KeyTag[1]==0){
          if(mytableNum==0){
            findEmptyBucket=1;
            break;
          }
        }
        if(findEmptyBucket){
          //fprintf(stderr,"add to cuckoo hash !!\n");
          curHashIndex[keyBucket].KeyTag[0]=keyBytes[2];
          curHashIndex[keyBucket].KeyTag[1]=keyBytes[3];     
          curHashIndex[keyBucket].TableNum[0]=tableNumBytes[0];
          curHashIndex[keyBucket].TableNum[1]=tableNumBytes[1];
          curHashIndex[keyBucket].TableNum[2]=tableNumBytes[2];
        }
        else{
          //keyBucket=curHashfunc.cuckooHash((char*)key.ToString().substr(0,config::kKeyLength).c_str(),config::cuckooHashNum-1,config::kKeyLength);
          ///add to head
          ListIndexEntry *lastEntry=&curHashIndex[keyBucket];
          ListIndexEntry *beginNextEntry=lastEntry->nextEntry;    
          ListIndexEntry *addEntry=new ListIndexEntry;      
          addEntry->KeyTag[0]=lastEntry->KeyTag[0];
          addEntry->KeyTag[1]=lastEntry->KeyTag[1];
          addEntry->TableNum[0]=lastEntry->TableNum[0];
          addEntry->TableNum[1]=lastEntry->TableNum[1];
          addEntry->TableNum[2]=lastEntry->TableNum[2];

          lastEntry->KeyTag[0]=keyBytes[2];
          lastEntry->KeyTag[1]=keyBytes[3];
          lastEntry->TableNum[0]=tableNumBytes[0];
          lastEntry->TableNum[1]=tableNumBytes[1];
          lastEntry->TableNum[2]=tableNumBytes[2];
          lastEntry->nextEntry=addEntry;
          addEntry->nextEntry=beginNextEntry;
          delete []keyBytes;   
        }
    delete []tableNumBytes;
    //delete iter;
  }
}








}//namespace rocksdb