#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) 
  {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
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
  
  /**** Setting up Students relation in the Attribute Cache Table ****/
  /*AttrCacheEntry* studentAttrHead = nullptr;
  prevAttrEntry = nullptr;

  for (int i = 13; i < 16; i++) {
    attrCatBlock.getRecord(attrCatRecord, i);

    AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;
    attrCacheEntry->next = nullptr;

    if (prevAttrEntry) {
      prevAttrEntry->next = attrCacheEntry;
    } else {
      studentAttrHead = attrCacheEntry;
    }

    prevAttrEntry = attrCacheEntry;
  }

  AttrCacheTable::attrCache[2] = studentAttrHead;*/

}

OpenRelTable::~OpenRelTable() {
for (int i = 0; i < MAX_OPEN; ++i) {
    if (RelCacheTable::relCache[i]) {
      free(RelCacheTable::relCache[i]);
      RelCacheTable::relCache[i] = nullptr;
    }
  }
for (int i = 0; i < MAX_OPEN; ++i) {
    AttrCacheEntry* current = AttrCacheTable::attrCache[i];
    while (current!=nullptr) {
      AttrCacheEntry* freep = current;
      current = current->next;
      free(freep);
    }
    AttrCacheTable::attrCache[i] = nullptr;
  }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  // if relname is RELCAT_RELNAME, return RELCAT_RELID
  if(strcmp(relName,RELCAT_RELNAME)==0)
  	return RELCAT_RELID;
  if(strcmp(relName,ATTRCAT_RELNAME)==0)
  	return ATTRCAT_RELID;
  // if relname is ATTRCAT_RELNAME, return ATTRCAT_RELID
  if (strcmp(relName, "Students") == 0)
    return 2;

  return E_RELNOTOPEN;
}