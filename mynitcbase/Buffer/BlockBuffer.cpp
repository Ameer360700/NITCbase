#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>

// the declarations for these functions can be found in "BlockBuffer.h"
BlockBuffer::BlockBuffer(int blockNum) 
{
  // initialise this.blockNum with the argument
  this->blockNum = blockNum;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}
/*
Used to get the header of the block into the location pointed to by `head`
NOTE: this function expects the caller to allocate memory for `head`
*/
// load the block header into the argument pointer
int BlockBuffer::getHeader(HeadInfo *head) 
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr); 
  // Returns a pointer to the first byte of the buffer storing
  // the block. This function will load the block to the buffer
  // if it is not already present.
  if (ret != SUCCESS)
    return ret;
  // read the block at this.blockNum into the buffer
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  // populate the numEntries, numAttrs and numSlots fields in *head
  memcpy(&head->pblock, buffer + 4, 4);
  memcpy(&head->lblock, buffer + 8, 4);
  memcpy(&head->rblock, buffer + 12, 4);
  memcpy(&head->numEntries, buffer + 16, 4);
  memcpy(&head->numAttrs, buffer + 20, 4);
  memcpy(&head->numSlots, bufferPtr + 24, 4);

  return SUCCESS;
}

/*
Used to get the record at slot `slotNum` into the array `rec`
NOTE: this function expects the caller to allocate memory for `rec`
*/
// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *record, int slotNum) 
{
  // get the header using this.getHeader() function
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) 
  {
    return ret;
  }

  HeadInfo head;
  BlockBuffer::getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  // read the block at this.blockNum into a buffer
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize
     * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer =buffer + (32 + slotCount + (recordSize * slotNum)); // calculate buffer + offset

  // load the record into the rec data structure
  memcpy(record, slotPointer, recordSize);

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
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char ** buffPtr) 
{
  /* check whether the block is already present in the buffer
     using StaticBuffer.getBufferNum() */
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  // if present (!=E_BLOCKNOTINBUFFER),
      // set the timestamp of the corresponding buffer to 0 and increment the
      // timestamps of all other occupied buffers in BufferMetaInfo.
  if(bufferNum!=E_BLOCKNOTINBUFFER)
  {
     for(int bufferindex=0; bufferindex<BUFFER_CAPACITY; bufferindex++)
     {
        if(bufferindex==bufferNum)
        {
           StaticBuffer::metainfo[bufferindex].timeStamp=0;
        }
        else
        {
          StaticBuffer::metainfo[bufferindex].timeStamp++;
        }
     }
  }
  // else
      // get a free buffer using StaticBuffer.getFreeBuffer()

      // if the call returns E_OUTOFBOUND, return E_OUTOFBOUND here as
      // the blockNum is invalid

      // Read the block into the free buffer using readBlock()

  // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  else
  {
     bufferNum=StaticBuffer::getFreeBuffer(this->blockNum);
     if(bufferNum==E_OUTOFBOUND)
     {
       return E_OUTOFBOUND;
     }
     Disk::readBlock(StaticBuffer::blocks[bufferNum],this->blockNum);
  }
  // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  *buffPtr=StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) 
{
  unsigned char *bufferPtr;
  /* get the starting address of the buffer containing the block
     using loadBlockAndGetBufferPtr(&bufferPtr). */
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
      // return the value returned by the call.
  if(ret!=SUCCESS)
  {
     return ret;
  }
  /* get the header of the block using the getHeader() function */
   HeadInfo head;
   this->getHeader(&head);
  // get number of attributes in the block.
  int numAttrs=head.numAttrs;
  // get the number of slots in the block.
  int numSlots=head.numSlots;
  // if input slotNum is not in the permitted range return E_OUTOFBOUND.
  if(slotNum < 0 || slotNum >= numSlots)
  {
      return E_OUTOFBOUND;
  }
  /* offset bufferPtr to point to the beginning of the record at required
     slot. the block contains the header, the slotmap, followed by all
     the records. so, for example,
     record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
     copy the record from `rec` to buffer using memcpy
     (hint: a record will be of size ATTR_SIZE * numAttrs)
  */
  int recordsize=ATTR_SIZE*numAttrs;
  int offset=HEADER_SIZE+numSlots+(recordsize*slotNum);
  // update dirty bit using setDirtyBit()
  memcpy(bufferPtr+offset,rec,recordsize);
  StaticBuffer::setDirtyBit(this->blockNum);
  /* (the above function call should not fail since the block is already
     in buffer and the blockNum is valid. If the call does fail, there
     exists some other issue in the code) */
  return SUCCESS;
}

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) 
{
  unsigned char *bufferPtr;
  // get the starting address of the buffer containing the block using
  // loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) 
  {
    return ret;
  }
  struct HeadInfo head;
  BlockBuffer::getHeader(&head);
  // get the header of the b
  int slotCount = head.numSlots; /* number of slots in block from header */
  // get a pointer to the beginning of the slotmap in memory by offsetting
  // HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  memcpy(slotMap, slotMapInBuffer, slotCount);

  return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType)
{
    double diff;
    if (attrType == STRING)
    	diff = strcmp(attr1.sVal, attr2.sVal);

    else
        diff = attr1.nVal - attr2.nVal;

    if (diff > 0)
    	return 1;
    if (diff < 0)
    	return -1;
    return 0;

}
