#include "AttrCacheTable.h"

#include <cstring>
#include "string"
#include "iostream"

AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

/* returns the attribute with name `attrName` for the relation corresponding to relId
NOTE: this function expects the caller to allocate memory for `*attrCatBuf`
*/
int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf) {

  // check that relId is valid and corresponds to an open relation
  if(relId<0||relId>=MAX_OPEN)
  return E_OUTOFBOUND;
  if(attrCache[relId]==nullptr)
  return E_RELNOTOPEN;
  // iterate over the entries in the attribute cache and set attrCatBuf to the entry that
  //    matches attrName
  std::string a,b;
  a=attrName;
  for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
    b=entry->attrCatEntry.attrName;
    if (a == b) {
	*attrCatBuf=entry->attrCatEntry;
	return SUCCESS;
    }
  }
  // no attribute with name attrName for the relation
  return E_ATTRNOTEXIST;
}

/* returns the attrOffset-th attribute for the relation corresponding to relId
NOTE: this function expects the caller to allocate memory for `*attrCatBuf`
*/
int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  //printf("thomas%d ",attrOffset);
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }
  // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
if (attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }
  // traverse the linked list of attribute cache entries
  for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
    if (entry->attrCatEntry.offset == attrOffset) {
	*attrCatBuf=entry->attrCatEntry;
	//printf("crissty%d ",attrOffset);
	return SUCCESS;
      // copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
    }
  }

  // there is no attribute at this offset
  return E_ATTRNOTEXIST;
}

/* Converts a attribute catalog record to AttrCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct AttrCatEntry type.
*/
void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],
                                          AttrCatEntry* attrCatEntry) {
  strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
  strcpy(attrCatEntry->attrName, record[1].sVal);
  attrCatEntry->attrType=(int)record[2].nVal;
  attrCatEntry->primaryFlag=(bool)record[3].nVal;
  attrCatEntry->rootBlock=(int)record[4].nVal;
  attrCatEntry->offset=(int)record[5].nVal;
  // copy the rest of the fields in the record to the attrCacheEntry struct
}


int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {

  if(relId<0 || relId>=MAX_OPEN /*relId is outside the range [0, MAX_OPEN-1]*/) {
    return E_OUTOFBOUND;
  }

  // if(attrCache[relId]/*entry corresponding to the relId in the Attribute Cache Table is free*/) {
  //   return E_RELNOTOPEN;
  // }

  // for(/* each attribute corresponding to relation with relId */)
  // {
  //   if (/* attrName/offset field of the AttrCatEntry
  //       is equal to the input attrName/attrOffset */)
  //   {
  //     //copy the searchIndex field of the corresponding Attribute Cache entry
  //     //in the Attribute Cache Table to input searchIndex variable.

  //     return SUCCESS;
  //   }
  // }

  // return E_ATTRNOTEXIST;

  AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];
    while (curr) {
        if (strcmp(curr->attrCatEntry.attrName, attrName) == 0)
        {
            *searchIndex = curr->searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}


int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN) 
      return E_OUTOFBOUND;

    AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];
    int index = 0;

    while (curr) {
        if (index == attrOffset)
        {
            *searchIndex = curr->searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
        index++;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN) 
      return E_OUTOFBOUND;

    AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];

    while (curr) {
        if (strcmp(curr->attrCatEntry.attrName, attrName) == 0)
        {
            curr->searchIndex = *searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN) 
      return E_OUTOFBOUND;

    AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];
    int index = 0;

    while (curr) {
        if (index == attrOffset)
        {
            curr->searchIndex = *searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
        index++;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE])
{
    // curr->searchIndex = RecId{-1, -1};
    IndexId indexId = {-1, -1};
    return AttrCacheTable::setSearchIndex(relId, attrName, &indexId);
}

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset)
{
    // curr->searchIndex = RecId{-1, -1};
    IndexId indexId = {-1, -1};
    return AttrCacheTable::setSearchIndex(relId, attrOffset, &indexId);
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {

  if(relId<0 || relId > 11/*relId is outside the range [0, MAX_OPEN-1]*/) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr /*entry corresponding to the relId in the Attribute Cache Table is free*/) {
    return E_RELNOTOPEN;
  }

  for( AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next /* each attribute corresponding to relation with relId */)
  {
    if(strcmp(entry->attrCatEntry.attrName,attrName)==0)
    {
      // copy the attrCatBuf to the corresponding Attribute Catalog entry in
      // the Attribute Cache Table.

      // set the dirty flag of the corresponding Attribute Cache entry in the
      // Attribute Cache Table.
      strcpy(entry->attrCatEntry.relName, attrCatBuf->relName);
      strcpy(entry->attrCatEntry.attrName, attrCatBuf->attrName);

      entry->attrCatEntry.attrType = attrCatBuf->attrType;
      entry->attrCatEntry.primaryFlag = attrCatBuf->primaryFlag;
      entry->attrCatEntry.rootBlock = attrCatBuf->rootBlock;
      entry->attrCatEntry.offset = attrCatBuf->offset;

      entry->dirty = true;

      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}
int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf) {

  if(relId<0 || relId > 11/*relId is outside the range [0, MAX_OPEN-1]*/) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr /*entry corresponding to the relId in the Attribute Cache Table is free*/) {
    return E_RELNOTOPEN;
  }

  for( AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next /* each attribute corresponding to relation with relId */)
  {
    if(entry->attrCatEntry.offset==attrOffset)
    {
      // copy the attrCatBuf to the corresponding Attribute Catalog entry in
      // the Attribute Cache Table.

      // set the dirty flag of the corresponding Attribute Cache entry in the
      // Attribute Cache Table.
      strcpy(entry->attrCatEntry.relName, attrCatBuf->relName);
      strcpy(entry->attrCatEntry.attrName, attrCatBuf->attrName);

      entry->attrCatEntry.attrType = attrCatBuf->attrType;
      entry->attrCatEntry.primaryFlag = attrCatBuf->primaryFlag;
      entry->attrCatEntry.rootBlock = attrCatBuf->rootBlock;
      entry->attrCatEntry.offset = attrCatBuf->offset;

      entry->dirty = true;

      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

void AttrCacheTable :: attrCatEntryToRecord(AttrCatEntry * attrCatEntry,Attribute record[ATTRCAT_NO_ATTRS]){
  strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);

    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;
}
