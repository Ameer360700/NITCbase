#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() 
{

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) 
  {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    OpenRelTable::tableMetaInfo[i].free=true;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  
  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // Allocate on the heap to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  /**** Setting up Attribute Catalog relation in the Relation Cache Table ****/
  Attribute attrCatRelRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(attrCatRelRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

  RelCacheEntry attrCatRelCacheEntry;
  RelCacheTable::recordToRelCatEntry(attrCatRelRecord, &attrCatRelCacheEntry.relCatEntry);
  attrCatRelCacheEntry.recId.block = RELCAT_BLOCK;
  attrCatRelCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  RelCacheTable::relCache[ATTRCAT_RELID] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = attrCatRelCacheEntry;
  
  /************ Setting up Attribute Cache entries ************/

  /**** Setting up Relation Catalog relation in the Attribute Cache Table ****/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  AttrCacheEntry* relCatAttrHead = nullptr;
  AttrCacheEntry* prevAttrEntry = nullptr;

  for (int i = 0; i < RELCAT_NO_ATTRS; i++) 
  {
    attrCatBlock.getRecord(attrCatRecord, i);

    AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    if (prevAttrEntry) 
    {
      prevAttrEntry->next = attrCacheEntry;
    } 
    else 
    {
      relCatAttrHead = attrCacheEntry;
    }
    prevAttrEntry = attrCacheEntry;
  }

  AttrCacheTable::attrCache[RELCAT_RELID] = relCatAttrHead;
  
  AttrCacheEntry* attrCatAttrHead = nullptr;
  prevAttrEntry = nullptr;
  for (int i = 6; i < 12; i++) 
  {   
    attrCatBlock.getRecord(attrCatRecord, i);

    AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    if (prevAttrEntry) 
    {
      prevAttrEntry->next = attrCacheEntry;
    } 
    else 
    {
      attrCatAttrHead = attrCacheEntry;
    }

    prevAttrEntry = attrCacheEntry;
  }

  AttrCacheTable::attrCache[ATTRCAT_RELID] = attrCatAttrHead;
  
  RecBuffer relCatBlock2(RELCAT_BLOCK);
  Attribute relCatRecord2[RELCAT_NO_ATTRS];
  relCatBlock2.getRecord(relCatRecord2, 2);

  RelCacheEntry relCacheEntry2;
  RelCacheTable::recordToRelCatEntry(relCatRecord2, &relCacheEntry2.relCatEntry);
  relCacheEntry2.recId.block = RELCAT_BLOCK;
  relCacheEntry2.recId.slot = 2;

  // Allocate on the heap to persist outside this function
  RelCacheTable::relCache[2] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[2]) = relCacheEntry2;
  
  RecBuffer attrCatBlock2(ATTRCAT_BLOCK);
  Attribute attrCatRecord2[ATTRCAT_NO_ATTRS];

  AttrCacheEntry* relCatAttrHead2 = nullptr;
  AttrCacheEntry* prevAttrEntry2 = nullptr;

  for (int i = 12; i < 16 ; i++) 
  {
    attrCatBlock2.getRecord(attrCatRecord2, i);

    AttrCacheEntry* attrCacheEntry2 = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord2, &attrCacheEntry2->attrCatEntry);
    attrCacheEntry2->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry2->recId.slot = i;
    attrCacheEntry2->next = nullptr;

    if (prevAttrEntry2) 
    {
      prevAttrEntry2->next = attrCacheEntry2;
    } 
    else 
    {
      relCatAttrHead2 = attrCacheEntry2;
    }

    prevAttrEntry2 = attrCacheEntry2;
  }

  AttrCacheTable::attrCache[2] = relCatAttrHead2;
  
  OpenRelTable::tableMetaInfo[RELCAT_RELID].free=false;
  OpenRelTable::tableMetaInfo[ATTRCAT_RELID].free=false;
  strcpy(OpenRelTable::tableMetaInfo[RELCAT_RELID].relName,RELCAT_RELNAME);
  strcpy(OpenRelTable::tableMetaInfo[ATTRCAT_RELID].relName,ATTRCAT_RELNAME);

}

OpenRelTable::~OpenRelTable() 
{
  for (int i = 2; i < MAX_OPEN; ++i) 
  {
    if (!tableMetaInfo[i].free) 
    {
      OpenRelTable::closeRel(i);
    }
  }
  for(int i=0; i<MAX_OPEN; ++i)
  {
    if(RelCacheTable::relCache[i])
    {
      free(RelCacheTable::relCache[i]);
      RelCacheTable::relCache[i]=nullptr;
    }
  }
  for (int i = 0; i < MAX_OPEN; ++i) 
  {
    AttrCacheEntry* current = AttrCacheTable::attrCache[i];
    while (current!=nullptr) 
    {
      AttrCacheEntry* freep = current;
      current = current->next;
      free(freep);
    }
    AttrCacheTable::attrCache[i] = nullptr;
  }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) 
{
  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/

  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
  for(int i=0; i< MAX_OPEN; ++i)
  {
    if(strcmp(tableMetaInfo[i].relName,relName)==0)
    {
       return i;
    }
  }
  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() 
{

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/

  // if found return the relation id, else return E_CACHEFULL.
  for(int i=2; i<MAX_OPEN; ++i)
  {
    if(tableMetaInfo[i].free)
    {
      return i;
    }
  }
  return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) 
{

  if(OpenRelTable::getRelId(relName)!=E_RELNOTOPEN)
  {
    // (checked using OpenRelTable::getRelId())
    return OpenRelTable::getRelId(relName);
    // return that relation id;
  }

  /* find a free slot in the Open Relation Table
  using OpenRelTable::getFreeOpenRelTableEntry(). */
  int ret=OpenRelTable::getFreeOpenRelTableEntry(); 
  
  if (ret==E_CACHEFULL)
  {
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.
  int relId=ret;

  /****** Setting up Relation Cache entry for the relation ******/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  RecId relcatRecId;
  Attribute attrVal;
  strcpy(attrVal.sVal,relName);
  relcatRecId=BlockAccess::linearSearch(RELCAT_RELID,(char*)RELCAT_ATTR_RELNAME,attrVal,EQ);
  if(relcatRecId.block==-1 && relcatRecId.slot==-1)
  {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }

  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */
  Attribute rec[RELCAT_NO_ATTRS];
  RecBuffer relcatblock(relcatRecId.block);
  relcatblock.getRecord(rec,relcatRecId.slot);
  RelCacheEntry* relcacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(rec,&relcacheEntry->relCatEntry);
  relcacheEntry->recId.block=relcatRecId.block;
  relcacheEntry->recId.slot=relcatRecId.slot;
  RelCacheTable::relCache[relId]=relcacheEntry;
  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry* listHead=nullptr;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  for(int attr=0; attr<RelCacheTable::relCache[relId]->relCatEntry.numAttrs;attr++)
  {
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
      RecId attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,(char*)ATTRCAT_ATTR_RELNAME, attrVal, EQ);
      AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
      Attribute record[ATTRCAT_NO_ATTRS];
      RecBuffer attrcatblock(attrcatRecId.block);
      attrcatblock.getRecord(record,attrcatRecId.slot);
      AttrCacheTable::recordToAttrCatEntry(record,&attrCacheEntry->attrCatEntry);
      attrCacheEntry->recId.block = attrcatRecId.block;
      attrCacheEntry->recId.slot = attrcatRecId.slot;
      attrCacheEntry->next = listHead;
      listHead = attrCacheEntry;
  }
  AttrCacheTable::attrCache[relId]=listHead;
  tableMetaInfo[relId].free=false;
  strcpy(tableMetaInfo[relId].relName,relName);

  // set the relIdth entry of the AttrCacheTable to listHead.

  /****** Setting up metadata in the Open Relation Table for the relation******/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.

  return relId;
}


int OpenRelTable::closeRel(int relId) 
{
  if (relId==RELCAT_RELID || relId==ATTRCAT_RELID) 
  {
    return E_NOTPERMITTED;
  }

  if (relId < 0 && relId >= MAX_OPEN) 
  {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free) 
  {
    return E_RELNOTOPEN;
  }

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function

  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
  free(RelCacheTable::relCache[relId]);
  RelCacheTable::relCache[relId]=nullptr;

  AttrCacheEntry *attrNode= AttrCacheTable::attrCache[relId];
  while(attrNode!=nullptr)
  {
    attrNode=attrNode->next;
    free(AttrCacheTable::attrCache[relId]);
    AttrCacheTable::attrCache[relId]=attrNode;
  }
  tableMetaInfo[relId].free=true;
  return SUCCESS;
}
