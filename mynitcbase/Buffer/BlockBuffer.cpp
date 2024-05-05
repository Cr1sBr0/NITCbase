#include "BlockBuffer.h"
#include <iostream>

#include <cstdlib>
#include <cstring>
#include "string"
// the declarations for these functions can be found in "BlockBuffer.h"




// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

RecBuffer::RecBuffer() : BlockBuffer('R'){}
// call parent non-default constructor with 'R' denoting record block.

BlockBuffer::BlockBuffer(int blockNum) {
  // initialise this.blockNum with the argument
  this->blockNum=blockNum;
}

BlockBuffer::BlockBuffer(char blockType){
    // allocate a block on the disk and a buffer in memory to hold the new block of
    // given type using getFreeBlock function and get the return error codes if any.
    int k=UNUSED_BLK;
    if(blockType=='R')
    k=REC;
    if(blockType=='I')
    k=1;
    if(blockType=='L')
    k=2;
    int freeBlk=getFreeBlock(k);
    
    // set the blockNum field of the object to that of the allocated block
    // number if the method returned a valid block number,
    // otherwise set the error code returned as the block number.
    this->blockNum=freeBlk;
    // (The caller must check if the constructor allocatted block successfully
    // by checking the value of block number field.)
}
int BlockBuffer::getBlockNum(){

    //return corresponding block number.
    return this->blockNum;
}
void BlockBuffer::releaseBlock(){

    // if blockNum is INVALID_BLOCK (-1), or it is invalidated already, do nothing
     if (this->blockNum == INVALID_BLOCKNUM||StaticBuffer::blockAllocMap[blockNum]==UNUSED_BLK)
     return;
     int bufferNum=StaticBuffer::getBufferNum(this->blockNum);
     if(bufferNum==E_BLOCKNOTINBUFFER)
     return ;
    // else
        /* get the buffer number of the buffer assigned to the block
           using StaticBuffer::getBufferNum().
           (this function return E_BLOCKNOTINBUFFER if the block is not
           currently loaded in the buffer)
            */
     StaticBuffer::metainfo[bufferNum].free=true;
        // if the block is present in the buffer, free the buffer
        // by setting the free flag of its StaticBuffer::tableMetaInfo entry
        // to true.

        // free the block in disk by setting the data type of the entry
        // corresponding to the block number in StaticBuffer::blockAllocMap
        // to UNUSED_BLK.
     StaticBuffer::blockAllocMap[this->blockNum]=UNUSED_BLK;
        // set the object's blockNum to INVALID_BLOCK (-1)
        this->blockNum=INVALID_BLOCKNUM;
}

 int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
    
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    if(loadBlockAndGetBufferPtr(&bufferPtr)!=SUCCESS)
    return loadBlockAndGetBufferPtr(&bufferPtr);
    
    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
    bufferHeader->blockType=head->blockType;
    bufferHeader->numSlots=head->numSlots;
    bufferHeader->numEntries=head->numEntries;
    bufferHeader->rblock=head->rblock;
    bufferHeader->lblock=head->lblock;
    bufferHeader->numAttrs=head->numAttrs;
    bufferHeader->pblock=head->pblock;
    
    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed, return the error code
    if(StaticBuffer::setDirtyBit(this->blockNum)!=SUCCESS)
    return StaticBuffer::setDirtyBit(this->blockNum);
    
    // return SUCCESS;
    return SUCCESS;
}
int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    if(loadBlockAndGetBufferPtr(&bufferPtr)!=SUCCESS)
    return loadBlockAndGetBufferPtr(&bufferPtr);
    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
    // *((int32_t *)bufferPtr) = blockType;
    int32_t *blockTypePtr = (int32_t*) bufferPtr;
	*blockTypePtr = blockType;
    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
    StaticBuffer::blockAllocMap[(this->blockNum)]=blockType;
    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
        // return the returned value from the call
    if(StaticBuffer::setDirtyBit(this->blockNum)!=SUCCESS)
    return StaticBuffer::setDirtyBit(this->blockNum);
    // return SUCCESS
    return SUCCESS;
}


int BlockBuffer::getFreeBlock(int blockType){

    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.
    int i,freeBlk;
    for(i=0;i<DISK_BLOCKS;i++){
     if(StaticBuffer::blockAllocMap[i]==UNUSED_BLK){
        freeBlk=i;
        break;
     }
    }
    if(i==DISK_BLOCKS)
    return E_DISKFULL;
    // if no block is free, return E_DISKFULL.

    // set the object's blockNum to the block number of the free block.
    this->blockNum=freeBlk;
    // find a free buffer using StaticBuffer::getFreeBuffer() .
    int bufferIndex=StaticBuffer::getFreeBuffer(this->blockNum);                                     
    if (bufferIndex < 0 && bufferIndex >= BUFFER_CAPACITY) {
		  printf ("Error: Buffer is full\n");
		  return bufferIndex;
	  }
    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
    // to the setHeader() function.
    struct HeadInfo head;
    head.pblock=-1;
    head.lblock=-1;
    head.rblock=-1;
    head.numEntries=0;
    head.numAttrs=0;
    head.numSlots=0;
    setHeader(&head);
    // update the block type of the block to the input block type using setBlockType().
    setBlockType(blockType);
    // return block number of the free block.
    return freeBlk;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {

    double diff;
    if (attrType == STRING)
        diff = strcmp(attr1.sVal, attr2.sVal);

    else
       diff = attr1.nVal - attr2.nVal;

    
    if (diff>0)
    return 1;
    if (diff==0)
    return 0;
    return -1;
   
}
int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */
    int haha=loadBlockAndGetBufferPtr(&bufferPtr);
    if(haha!=SUCCESS)
    return haha;
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    // get the header of the block using the getHeader() function
    struct HeadInfo head;
    getHeader(&head);
    int numSlots = /* the number of slots in the block */head.numSlots;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
    unsigned char *slotPointer = bufferPtr + HEADER_SIZE;
    memcpy(slotPointer, slotMap, numSlots);
    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call
    int hehe=StaticBuffer::setDirtyBit(this->blockNum);
    if(hehe!=SUCCESS)
    return hehe;
    // return SUCCESS
    return SUCCESS;
}
  
  
/* NOTE: This function will NOT check if the block has been initialised as a
   record or an index block. It will copy whatever content is there in that
   disk block to the buffer.
   Also ensure that all the methods accessing and updating the block's data
   should call the loadBlockAndGetBufferPtr() function before the access or
   update is done. This is because the block might not be present in the
   buffer due to LRU buffer replacement. So, it will need to be bought back
   to the buffer before any operations can be done.
 */
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char ** buffPtr) {
    /* check whether the block is already present in the buffer
       using StaticBuffer.getBufferNum() */
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    // if present (!=E_BLOCKNOTINBUFFER),
        // set the timestamp of the corresponding buffer to 0 and increment the
        // timestamps of all other occupied buffers in BufferMetaInfo.
    if(bufferNum!=E_BLOCKNOTINBUFFER){
       for(int i=0;i<32;i++){
          if(StaticBuffer::metainfo[i].free==false){
             StaticBuffer::metainfo[i].timeStamp++;
          }
       }
       StaticBuffer::metainfo[bufferNum].timeStamp=0;
       
    
    }
    else{
       bufferNum=StaticBuffer::getFreeBuffer(this->blockNum);
       if(bufferNum==E_OUTOFBOUND)
          return E_OUTOFBOUND;
       Disk::readBlock(StaticBuffer::blocks[bufferNum],this->blockNum);   
    }
    *buffPtr=StaticBuffer::blocks[bufferNum];
    // else
        // get a free buffer using StaticBuffer.getFreeBuffer()

        // if the call returns E_OUTOFBOUND, return E_OUTOFBOUND here as
        // the blockNum is invalid

        // Read the block into the free buffer using readBlock()

    // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr

    // return SUCCESS;
    return SUCCESS;
}  


int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
    //unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    unsigned char *bufferPtr;
    if(loadBlockAndGetBufferPtr(&bufferPtr)!=SUCCESS)
    return loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    /* get the header of the block using the getHeader() function */
    struct HeadInfo head;
    this->getHeader(&head);
    // get number of attributes in the block.

    // get the number of slots in the block.
    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;
    // if input slotNum is not in the permitted range return E_OUTOFBOUND.
    if(slotCount<slotNum)
    return E_OUTOFBOUND;
    /* offset bufferPtr to point to the beginning of the record at required
       slot. the block contains the header, the slotmap, followed by all
       the records. so, for example,
       record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
       copy the record from `rec` to buffer using memcpy
       (hint: a record will be of size ATTR_SIZE * numAttrs)
    */
    int recordSize = attrCount * ATTR_SIZE;
    unsigned char *slotPointer = /* calculate buffer + offset */bufferPtr+HEADER_SIZE+slotCount+(slotNum*recordSize);
  

    // update dirty bit using setDirtyBit()
    StaticBuffer::setDirtyBit(this->blockNum);
    memcpy(slotPointer,rec,recordSize);
    /* (the above function call should not fail since the block is already
       in buffer and the blockNum is valid. If the call does fail, there
       exists some other issue in the code) */

    // return SUCCESS
   
    return SUCCESS;
}




/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  // get the header of the block using getHeader() function
  this->getHeader(&head);
  int slotCount = /* number of slots in block from header */head.numSlots;

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  memcpy(slotMap,slotMapInBuffer,slotCount);
  return SUCCESS;
}









// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  //unsigned char buffer[BLOCK_SIZE];

  // read the block at this.blockNum into the buffer
   unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;   // return any errors that might have occured in the process
  }
  // populate the numEntries, numAttrs and numSlots fields in *head
  memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->numEntries, /* fill this */bufferPtr+16, 4);
  memcpy(&head->numAttrs, /* fill this */bufferPtr+20, 4);
  memcpy(&head->rblock, /* fill this */bufferPtr+12, 4);
  memcpy(&head->lblock, /* fill this */bufferPtr+8, 4);
  memcpy(&head->pblock,bufferPtr+ 4,4);

  return SUCCESS;
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;

  // get the header using this.getHeader() function
  this->getHeader(&head);
  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  //printf("helon\n");
  fflush(stdout);
  // read the block at this.blockNum into a buffer
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
  
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = /* calculate buffer + offset */bufferPtr+HEADER_SIZE+slotCount+(slotNum*recordSize);

  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}


int IndInternal::setEntry(void *ptr, int indexNum) {
    // if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
    //     return E_OUTOFBOUND.

    if(indexNum<0 || indexNum>=MAX_KEYS_INTERNAL){
      return E_OUTOFBOUND;
    }

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret!=SUCCESS){
      return ret;
    }
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.

    // typecast the void pointer to an internal entry pointer
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    /*
    - copy the entries from *internalEntry to the indexNum`th entry
    - make sure that each field is copied individually as in the following code
    - the lChild and rChild fields of InternalEntry are of type int32_t
    - int32_t is a type of int that is guaranteed to be 4 bytes across every
      C++ implementation. sizeof(int32_t) = 4
    */

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
       from bufferPtr */

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(entryPtr, &(internalEntry->lChild), 4);
    memcpy(entryPtr + 4, &(internalEntry->attrVal), ATTR_SIZE);
    memcpy(entryPtr + 20, &(internalEntry->rChild), 4);


    // update dirty bit using setDirtyBit()
    // if setDirtyBit failed, return the value returned by the call
    if(StaticBuffer::setDirtyBit(this->blockNum)!=SUCCESS)
      return StaticBuffer::setDirtyBit(this->blockNum);

    return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum) {

    // if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
    //     return E_OUTOFBOUND.
    if(indexNum<0 || indexNum>=MAX_KEYS_LEAF){
      return E_OUTOFBOUND;
    }

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret!=SUCCESS){
      return ret;
    }
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.

    // copy the Index at ptr to indexNum'th entry in the buffer using memcpy

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy(entryPtr, (struct Index *)ptr, LEAF_ENTRY_SIZE);
    
    // update dirty bit using setDirtyBit()
    // if setDirtyBit failed, return the value returned by the call
    if(StaticBuffer::setDirtyBit(this->blockNum)!=SUCCESS)
      return StaticBuffer::setDirtyBit(this->blockNum);


    return SUCCESS;
}


//* calls the corresponding parent constructor
IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType){}

//* calls the corresponding parent constructor
IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum){}

//* calls the corresponding parent constructor
//* 'I' used to denote IndInternal.
IndInternal::IndInternal() : IndBuffer('I'){}

//* calls the corresponding parent constructor
IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum){}

//* this is the way to call parent non-default constructor.
//* 'L' used to denote IndLeaf.
IndLeaf::IndLeaf() : IndBuffer('L'){} 

//* this is the way to call parent non-default constructor.
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum){}

int IndInternal::getEntry(void *ptr, int indexNum) {
    // if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
    //     return E_OUTOFBOUND.
    if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
      return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
if(ret!=SUCCESS)
  return ret;

    // typecast the void pointer to an internal entry pointer
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    /*
    - copy the entries from the indexNum`th entry to *internalEntry
    - make sure that each field is copied individually as in the following code
    - the lChild and rChild fields of InternalEntry are of type int32_t
    - int32_t is a type of int that is guaranteed to be 4 bytes across every
      C++ implementation. sizeof(int32_t) = 4
    */

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
       from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr + 20, 4);

    return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum) 
{
    // if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
    //     return E_OUTOFBOUND.
	if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
    return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
	if (ret != SUCCESS) 
    return ret;

    // copy the indexNum'th Index entry in buffer to memory ptr using memcpy

    // the indexNum'th entry will begin at an offset of
    // HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr
    
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}
