#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
void clearList(AttrCacheEntry* head) 
{
    for (AttrCacheEntry* it = head, *next; it != nullptr; it = next) 
    {
        next = it->next;
        free(it);
    }
}
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

  OpenRelTable::tableMetaInfo[RELCAT_RELID].free=false;
  OpenRelTable::tableMetaInfo[ATTRCAT_RELID].free=false;
  strcpy(OpenRelTable::tableMetaInfo[RELCAT_RELID].relName,RELCAT_RELNAME);
  strcpy(OpenRelTable::tableMetaInfo[ATTRCAT_RELID].relName,ATTRCAT_RELNAME);

}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) 
{

  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/
  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.

  for(int i=0; i<MAX_OPEN; i++)
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
  for(int i=0; i<MAX_OPEN; i++)
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

  int alreadyexistrelId=OpenRelTable::getRelId(relName);
  if(alreadyexistrelId >= 0 && alreadyexistrelId < MAX_OPEN)
  {
    // (checked using OpenRelTable::getRelId())
    // return that relation id;
    return alreadyexistrelId;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
  int freeslot=OpenRelTable::getFreeOpenRelTableEntry();
  if (freeslot==E_CACHEFULL)
  {
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.
  int relId=freeslot;
  
  /****** Setting up Relation Cache entry for the relation ******/
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute relcatrelname;
  strcpy(relcatrelname.sVal,relName);
  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/

  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  RecId relcatRecId=BlockAccess::linearSearch(RELCAT_RELID,(char*)RELCAT_ATTR_RELNAME,relcatrelname,EQ);

  if (relcatRecId.block==-1 && relcatRecId.slot==-1) 
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
   RecBuffer relcatblock(relcatRecId.block);
   Attribute relcatrecord[RELCAT_NO_ATTRS];
   relcatblock.getRecord(relcatrecord,relcatRecId.slot);
   RelCacheEntry* relcacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
   RelCacheTable::recordToRelCatEntry(relcatrecord,&relcacheEntry->relCatEntry);
   relcacheEntry->recId.block=relcatRecId.block;
   relcacheEntry->recId.block=relcatRecId.block;
   RelCacheTable::relCache[relId]=relcacheEntry;

  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry* listHead=nullptr;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  for(int attr=0; attr<RelCacheTable::relCache[relId]->relCatEntry.numAttrs; attr++)
  {
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
      RecId attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,(char*)ATTRCAT_ATTR_RELNAME,relcatrelname,EQ);
      AttrCacheEntry*attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
      Attribute attrcatrecord[ATTRCAT_NO_ATTRS];
      RecBuffer attrcatblock(attrcatRecId.block);
      attrcatblock.getRecord(attrcatrecord,attrcatRecId.slot);
      AttrCacheTable::recordToAttrCatEntry(attrcatrecord,&attrCacheEntry->attrCatEntry); 
      attrCacheEntry->recId.block=attrcatRecId.block;
      attrCacheEntry->recId.slot=attrcatRecId.slot;
      attrCacheEntry->next=listHead;
      listHead=attrCacheEntry;
      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
  }
  AttrCacheTable::attrCache[relId]=listHead;

  // set the relIdth entry of the AttrCacheTable to listHead.

  /****** Setting up metadata in the Open Relation Table for the relation******/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  tableMetaInfo[relId].free=false;
  strcpy(tableMetaInfo[relId].relName,relName);
  return relId;
}

int OpenRelTable::closeRel(int relId) 
{
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (OpenRelTable::tableMetaInfo[relId].free)
        return E_RELNOTOPEN;

    OpenRelTable::tableMetaInfo[relId].free = true;
    free(RelCacheTable::relCache[relId]);
    clearList(AttrCacheTable::attrCache[relId]);

    RelCacheTable::relCache[relId] = nullptr;
    AttrCacheTable::attrCache[relId] = nullptr;

    return SUCCESS;
}

OpenRelTable::~OpenRelTable() 
{
  // close all open relations (from rel-id = 2 onwards. Why?)
  for (int i = 2; i < MAX_OPEN; ++i) 
  {
    if (!tableMetaInfo[i].free) 
    {
      OpenRelTable::closeRel(i); // we will implement this function later
    }
  }
  for(int i=0; i< MAX_OPEN; i++)
  {
     if(RelCacheTable::relCache[i])
     {
       free(RelCacheTable::relCache[i]);
       RelCacheTable::relCache[i]=nullptr;
     }
  }
  for(int i=0; i<MAX_OPEN; i++)
  {
     AttrCacheEntry*current=AttrCacheTable::attrCache[i];
     while(current!=NULL)
     {
        AttrCacheEntry*freep=current;
        current=current->next;
        free(freep);
     }
     AttrCacheTable::attrCache[i]=nullptr;
  }
  // free the memory allocated for rel-id 0 and 1 in the caches
}