#include "RelCacheTable.h"

#include <cstring>
#include "iostream"
RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];




/* will return the searchIndex for the relation corresponding to `relId
NOTE: this function expects the caller to allocate memory for `*searchIndex`
*/
int RelCacheTable::getSearchIndex(int relId, RecId* searchIndex) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if(relId<0||relId>=MAX_OPEN)
  return E_OUTOFBOUND;
  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
  if(relCache[relId]==nullptr)
  return E_RELNOTOPEN;
  // copy the searchIndex field of the Relation Cache entry corresponding
  //   to input relId to the searchIndex variable.
  *searchIndex = relCache[relId]->searchIndex;
  return SUCCESS;
}

// sets the searchIndex for the relation corresponding to relId
int RelCacheTable::setSearchIndex(int relId, RecId* searchIndex) {

  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if(relId<0||relId>=MAX_OPEN)
  return E_OUTOFBOUND;
  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
  if(relCache[relId]==nullptr)
  return E_RELNOTOPEN;
  // update the searchIndex value in the relCache for the relId to the searchIndex argument
  relCache[relId]->searchIndex=*searchIndex;
  return SUCCESS;
}

int RelCacheTable::resetSearchIndex(int relId) {
  // use setSearchIndex to set the search index to {-1, -1}
  relCache[relId]->searchIndex={-1,-1};
  return SUCCESS;
}








/*
Get the relation catalog entry for the relation with rel-id `relId` from the cache
NOTE: this function expects the caller to allocate memory for `*relCatBuf`
*/
int RelCacheTable::getRelCatEntry(int relId, RelCatEntry* relCatBuf) {


  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // if there's no entry at the rel-id
  if (relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // copy the value to the relCatBuf argument
  *relCatBuf = relCache[relId]->relCatEntry;
 
  return SUCCESS;
}
int RelCacheTable::setRelCatEntry(int relId, RelCatEntry *relCatBuf) {

  if(/*relId is outside the range [0, MAX_OPEN-1]*/relId<0||relId>(MAX_OPEN-1)) {
    return E_OUTOFBOUND;
  }

  if(/*entry corresponding to the relId in the Relation Cache Table is free*/relCache[relId]==nullptr) {
    return E_RELNOTOPEN;
  }

  // copy the relCatBuf to the corresponding Relation Catalog entry in
  // the Relation Cache Table.
  memcpy(&(RelCacheTable::relCache[relId]->relCatEntry), relCatBuf, sizeof(RelCatEntry));
  // set the dirty flag of the corresponding Relation Cache entry in
  // the Relation Cache Table.
  RelCacheTable::relCache[relId]->dirty = true;
  
  return SUCCESS;
}
/* Converts a relation catalog record to RelCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct RelCatEntry type.
NOTE: this function expects the caller to allocate memory for `*relCatEntry`
*/
void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],
                                        RelCatEntry* relCatEntry) {
  strcpy(relCatEntry->relName, record[0].sVal);
  // printf("hii %s\n ",record[0].sVal);
  relCatEntry->numAttrs = (int)record[1].nVal;

  /* fill the rest of the relCatEntry struct with the values at
      RELCAT_NO_RECORDS_INDEX,
      */
      relCatEntry->numRecs=(int)record[2].nVal;
      relCatEntry->firstBlk=(int)record[3].nVal;
      relCatEntry->lastBlk=(int)record[4].nVal;
      relCatEntry->numSlotsPerBlk=(int)record[5].nVal;
      /*
      RELCAT_FIRST_BLOCK_INDEX,
      RELCAT_LAST_BLOCK_INDEX,
      RELCAT_NO_SLOTS_PER_BLOCK_INDEX
  */
}

void RelCacheTable::relCatEntryToRecord(RelCatEntry* relCatEntry,union Attribute record[RELCAT_NO_ATTRS]){
  strcpy( record[0].sVal,relCatEntry->relName);
  // printf("hii %s\n ",record[0].sVal);
  record[RELCAT_NO_ATTRIBUTES_INDEX].nVal=(int)relCatEntry->numAttrs;

  /* fill the rest of the relCatEntry struct with the values at
      RELCAT_NO_RECORDS_INDEX,
      */
      //relCatEntry->numRecs=(int)record[2].nVal;
      record[RELCAT_NO_RECORDS_INDEX].nVal=(int)relCatEntry->numRecs;
     // relCatEntry->firstBlk=(int)record[3].nVal;
      record[RELCAT_FIRST_BLOCK_INDEX].nVal=(int)relCatEntry->firstBlk;
   //   relCatEntry->lastBlk=(int)record[4].nVal;
      record[RELCAT_LAST_BLOCK_INDEX].nVal=(int)relCatEntry->lastBlk;
   //   relCatEntry->numSlotsPerBlk=(int)record[5].nVal;
      record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal=(int)relCatEntry->numSlotsPerBlk;
      /*
      RELCAT_FIRST_BLOCK_INDEX,
      RELCAT_LAST_BLOCK_INDEX,
      RELCAT_NO_SLOTS_PER_BLOCK_INDEX
  */
}
