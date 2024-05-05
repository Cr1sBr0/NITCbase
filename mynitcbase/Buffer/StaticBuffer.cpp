#include "StaticBuffer.h"
#include "string"
// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
StaticBuffer::StaticBuffer() {


// copy blockAllocMap blocks from disk to buffer (using readblock() of disk)
  // blocks 0 to 3
  for (int i = 0, blockAllocMapSlot = 0; i < 4; i++) {
		unsigned char buffer [BLOCK_SIZE];
		Disk::readBlock(buffer, i);

		for (int slot = 0; slot < BLOCK_SIZE; slot++, blockAllocMapSlot++)
			StaticBuffer::blockAllocMap[blockAllocMapSlot] = buffer[slot];
	}
  // declare the blockAllocMap array


  // initialise all blocks as free
  for (int i=0;i<BUFFER_CAPACITY;i++) {
    metainfo[i].free = true;
    metainfo[i].dirty = false;
    metainfo[i].timeStamp = -1;
    metainfo[i].blockNum = -1;
  }
}

/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
  StaticBuffer::~StaticBuffer() {
  // copy blockAllocMap blocks from buffer to disk(using writeblock() of disk)
  
  for (int i = 0, blockAllocMapSlot = 0; i < 4; i++) {
		unsigned char buffer [BLOCK_SIZE];

		for (int slot = 0; slot < BLOCK_SIZE; slot++, blockAllocMapSlot++) 
			buffer[slot] = blockAllocMap[blockAllocMapSlot];

		Disk::writeBlock(buffer, i);
	}

  /*iterate through all the buffer blocks,
    write back blocks with metainfo as free=false,dirty=true
    using Disk::writeBlock()
    */
    for (int i=0;i<BUFFER_CAPACITY;i++) {
       if(metainfo[i].free==false){
          if(metainfo[i].dirty==true){
             Disk::writeBlock(StaticBuffer::blocks[i],metainfo[i].blockNum);
          }
       }
  }
    
}
/*
int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer;

  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block
  for(int i=0;i<BUFFER_CAPACITY;i++){
  if(metainfo[i].free==true){
  allocatedBuffer=i;
  break;
  }
  }
  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].blockNum = blockNum;

  return allocatedBuffer;
}
*/
int StaticBuffer::getFreeBuffer(int blockNum){
    // Check if blockNum is valid (non zero and less than DISK_BLOCKS)
    // and return E_OUTOFBOUND if not valid.
    if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
    // increase the timeStamp in metaInfo of all occupied buffers.
    for(int i=0;i<BUFFER_CAPACITY;i++){
     if(metainfo[i].free==false)
     metainfo[i].timeStamp++;
    }
    // let bufferNum be used to store the buffer number of the free/freed buffer.
    int bufferNum;
    int large=-1;
    // iterate through metainfo and check if there is any buffer free
    
    // if a free buffer is available, set bufferNum = index of that free buffer.
   
    // if a free buffer is not available,
    //     find the buffer with the largest timestamp
    //     IF IT IS DIRTY, write back to the disk using Disk::writeBlock()
    //     set bufferNum = index of this buffer
    for(int i=0;i<BUFFER_CAPACITY;i++){
     if(metainfo[i].free==false){
        if(metainfo[i].timeStamp>large){
           large=metainfo[i].timeStamp;
           bufferNum=i;
         }
     }
     else{ 
        metainfo[i].free=false;
        metainfo[i].dirty=false;
        metainfo[i].blockNum=blockNum;
        metainfo[i].timeStamp=0;
        return i;
     }
    }
    if(metainfo[bufferNum].dirty)
    Disk::writeBlock(blocks[bufferNum],metainfo[bufferNum].blockNum);
    
    metainfo[bufferNum].free=false;
    metainfo[bufferNum].dirty=false;
    metainfo[bufferNum].blockNum=blockNum;
    metainfo[bufferNum].timeStamp=0;
    return bufferNum;
    // update the metaInfo entry corresponding to bufferNum with
    // free:false, dirty:false, blockNum:the input block number, timeStamp:0.

    // return the bufferNum.
}


int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
   
    int index=getBufferNum(blockNum);
   
    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if(index==E_BLOCKNOTINBUFFER||index==E_OUTOFBOUND)
       return index;
    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND

    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo
    metainfo[index].dirty=true;
    // return SUCCESS
 
    return SUCCESS;
}



/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/

int StaticBuffer::getBufferNum(int blockNum) {
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  
  
  for(int i=0;i<BUFFER_CAPACITY;i++){
  if(metainfo[i].free==false){
  if(metainfo[i].blockNum==blockNum)
  return i;
  }
  }
  // find and return the bufferIndex which corresponds to blockNum (check metainfo)

  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}


int StaticBuffer::getStaticBlockType(int blockNum){
    // Check if blockNum is valid (non zero and less than number of disk blocks)
    // and return E_OUTOFBOUND if not valid.

    // Access the entry in block allocation map corresponding to the blockNum argument
    // and return the block type after type casting to integer.
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
     return E_OUTOFBOUND;
    return (int)blockAllocMap[blockNum];
}