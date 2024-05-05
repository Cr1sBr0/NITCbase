#include "OpenRelTable.h"
#include "stdlib.h"
#include <cstring>
#include "iostream"
#include "string"
//RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];
//AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];
  
/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

int OpenRelTable::openRel( char relName[ATTR_SIZE]) {
  int relId;

  if(/* the relation `relName` already has an entry in the Open Relation Table */(relId=OpenRelTable::getRelId(relName))!=E_RELNOTOPEN){
    // (checked using OpenRelTable::getRelId())

    // return that relation id;
    return relId;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
  relId=OpenRelTable::getFreeOpenRelTableEntry();
  if (/* free slot not available */relId==E_CACHEFULL){
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.
  

  /****** Setting up Relation Cache entry for the relation ******/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  Attribute a;
  strcpy(a.sVal,relName);
  char  cris[16];
  strcpy(cris,"RelName");
  RecId relcatRecId=BlockAccess::linearSearch(RELCAT_RELID,cris,a,EQ);

  if (relcatRecId.block==-1&&relcatRecId.slot==-1 ) {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }

  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */
  Attribute record[6];
  RecBuffer buffer(relcatRecId.block);
  buffer.getRecord(record,relcatRecId.slot);
  RelCacheEntry* relCacheEntry=(RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(record,&((*relCacheEntry).relCatEntry));
  (*relCacheEntry).recId=relcatRecId;
  RelCacheTable::relCache[relId]=relCacheEntry;
  
  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry* listHead;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  RecId attrcatRecId;
  //Attribute a;
  //strcpy(a.sVal,relName);
  attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,cris,a,EQ);
  
  RecBuffer buffer2(attrcatRecId.block);
  buffer2.getRecord(record,attrcatRecId.slot);
  AttrCacheEntry* attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));;
  AttrCacheTable::recordToAttrCatEntry(record,&((*attrCacheEntry).attrCatEntry));
  (*attrCacheEntry).recId=attrcatRecId;
  AttrCacheTable::attrCache[relId]=attrCacheEntry;
  listHead=(attrCacheEntry);
  attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,cris,a,EQ);
  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  while(attrcatRecId.block!=-1&&attrcatRecId.slot!=-1)
  {
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
      RecBuffer buffer2(attrcatRecId.block);
      buffer2.getRecord(record,attrcatRecId.slot);
      AttrCacheEntry* attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
      AttrCacheTable::recordToAttrCatEntry(record,&((*attrCacheEntry).attrCatEntry));
      (*attrCacheEntry).recId=attrcatRecId;
      listHead->next=attrCacheEntry;
      listHead=listHead->next;
      attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,cris,a,EQ);
      //AttrCacheTable::attrCache[relId]=attrCacheEntry;

      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
  }
  listHead->next=NULL;
  // set the relIdth entry of the AttrCacheTable to listHead.
 
  /****** Setting up metadata in the Open Relation Table for the relation******/
   OpenRelTable::tableMetaInfo[relId].free=false;
   strcpy(OpenRelTable::tableMetaInfo[relId].relName,relName);
  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.

  return relId;
}





int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
  for(int i=0;i<MAX_OPEN;i++){
     if(OpenRelTable::tableMetaInfo[i].free)
     return i;
  }  
  return E_CACHEFULL;
  // if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
std::string a,b;
   a=relName;
   for(int i=0;i<12;i++){
   if(RelCacheTable::relCache[i]!=nullptr){
   b=RelCacheTable::relCache[i]->relCatEntry.relName;
   if(strcmp(relName,RelCacheTable::relCache[i]->relCatEntry.relName)==0){
   return i;
   }
   }
   }
  // if relname is RELCAT_RELNAME, return RELCAT_RELID
  // if relname is ATTRCAT_RELNAME, return ATTRCAT_RELID

  return E_RELNOTOPEN;
}

OpenRelTable::OpenRelTable() {
  
  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }
// initialise all values in relCache and attrCache to be nullptr and all entries
  // in tableMetaInfo to be free
  for(int i=0;i<MAX_OPEN;i++){
     OpenRelTable::tableMetaInfo[i].free=true;
  }
  
    
  
  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

   RecBuffer relCatBlock(RELCAT_BLOCK);
   Attribute relCatRecord[RELCAT_NO_ATTRS];
   struct RelCacheEntry relCacheEntry;
   for(int i=0;i<2;i++){
      relCatBlock.getRecord(relCatRecord, i);
      RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
      relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = i;
  RelCacheTable::relCache[i] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[i]) = relCacheEntry;
   }









  /**** setting up Relation Catalog relation in the Relation Cache Table****/
 

  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  struct AttrCacheEntry *a;
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  struct AttrCacheEntry *head;
  //relCatBlock.getRecord(relCatRecord, 1);
  int p=0,k=0;
  for(int i=0;i<2;i++){
     relCatBlock.getRecord(relCatRecord, i);
     a=(struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
     attrCatBlock.getRecord(attrCatRecord,p);
     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(a->attrCatEntry));
     a->recId.block = ATTRCAT_BLOCK;
     a->recId.slot = p;
     AttrCacheTable::attrCache[i] = a;
     head=a;
     p++;
     k=1;
     //printf("%d %d %d\n",p,i,(int)relCatRecord[1].nVal);
     while(k<relCatRecord[1].nVal){
      a=(struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
      attrCatBlock.getRecord(attrCatRecord,p);
      AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(a->attrCatEntry));
      a->recId.block = ATTRCAT_BLOCK;
      a->recId.slot = p;
      head->next=a;
      head=head->next;
      p++;
      k++;
     }
     head->next=NULL;
     
     
     
     /************ Setting up tableMetaInfo entries ************/

  // in the tableMetaInfo array
  //   set free = false for RELCAT_RELID and ATTRCAT_RELID
  OpenRelTable::tableMetaInfo[0].free=false;
  strcpy(OpenRelTable::tableMetaInfo[0].relName,"RELATIONCAT");
  OpenRelTable::tableMetaInfo[1].free=false;
  strcpy(OpenRelTable::tableMetaInfo[1].relName,"ATTRIBUTECAT");
  //   set relname for RELCAT_RELID and ATTRCAT_RELID
  }






  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  //RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  // iterate through all the attributes of the relation catalog and create a linked

}





int OpenRelTable::closeRel(int relId) {

    if (relId == RELCAT_RELID || relId== ATTRCAT_RELID)
    {
        return E_NOTPERMITTED;
    }

    if (0 > relId || relId >= MAX_OPEN) 
      return E_OUTOFBOUND;

  	if (tableMetaInfo[relId].free) 
      return E_RELNOTOPEN;

    /****** Releasing the Relation Cache entry of the relation ******/

    if (RelCacheTable::relCache[relId]->dirty == true/* RelCatEntry of the relIdth Relation Cache entry has been modified */)
    {
        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        Attribute relCatBuffer [RELCAT_NO_ATTRS];
		    RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), relCatBuffer);
        RecId recId=RelCacheTable::relCache[relId]->recId;
        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(relCatBuffer,recId.slot);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    }

    // free the memory dynamically alloted to this Relation Cache entry
    // and assign nullptr to that entry
    free (RelCacheTable::relCache[relId]);
    RelCacheTable::relCache[relId]=nullptr;
    /****** Releasing the Attribute Cache entry of the relation ******/

    // for all the entries in the linked list of the relIdth Attribute Cache entry.
    AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
	AttrCacheEntry *next = head->next;
    while(true){
        if(head->dirty)
        {
            /* Get the Attribute Catalog entry from attrCache
             Then convert it to a record using AttrCacheTable::attrCatEntryToRecord().
             Write back that entry by instantiating RecBuffer class. Use recId
             member field and recBuffer.setRecord() */
            Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
			      AttrCacheTable::attrCatEntryToRecord(&(head->attrCatEntry), attrCatRecord);

			      RecBuffer attrCatBlockBuffer (head->recId.block);
			      attrCatBlockBuffer.setRecord(attrCatRecord, head->recId.slot);
        }

        // free the memory dynamically alloted to this entry in Attribute
        // Cache linked list and assign nullptr to that entry
        free (head);
		    head = next;

		    if (head == NULL) 
          break;
		    next = next->next;

    }
    AttrCacheTable::attrCache[relId]=nullptr;

    /****** Updating metadata in the Open Relation Table of the relation  ******/

    //free the relIdth entry of the tableMetaInfo.
    tableMetaInfo[relId].free = true;


    return SUCCESS;
}




OpenRelTable::~OpenRelTable() {

    for(int i=2;i<12;i++)
    {
        if (!tableMetaInfo[i].free)
        {
            // close the relation using openRelTable::closeRel().
            OpenRelTable::closeRel(i); 
        }
    }

    /**** Closing the catalog relations in the relation cache ****/

    //releasing the relation cache entry of the attribute catalog

    if (/* RelCatEntry of the ATTRCAT_RELID-th RelCacheEntry has been modified */RelCacheTable::relCache[1]->dirty==true) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        Attribute record[6];
        RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[1]->relCatEntry),record);
        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(RelCacheTable::relCache[1]->recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
        relCatBlock.setRecord(record,RelCacheTable::relCache[1]->recId.slot);
    }
    // free the memory dynamically allocated to this RelCacheEntry
    free(RelCacheTable::relCache[1]);

    //releasing the relation cache entry of the relation catalog

    if(/* RelCatEntry of the RELCAT_RELID-th RelCacheEntry has been modified */RelCacheTable::relCache[0]->dirty==true) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        Attribute record[6];
        RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[0]->relCatEntry),record);
        
        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(RelCacheTable::relCache[0]->recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
        relCatBlock.setRecord(record,RelCacheTable::relCache[0]->recId.slot);
    }
    // free the memory dynamically allocated for this RelCacheEntry
    free(RelCacheTable::relCache[0]);

    // free the memory allocated for the attribute cache entries of the
    // relation catalog and the attribute catalog
    struct AttrCacheEntry* temp;
   struct AttrCacheEntry* head=AttrCacheTable::attrCache[RELCAT_RELID];
    while (head != nullptr) {
        temp = head;
        head = head->next;
        delete temp;
    }
    head=AttrCacheTable::attrCache[ATTRCAT_RELID];
    while (head != nullptr) {
        temp = head;
        head = head->next;
        delete temp;
    }
}

