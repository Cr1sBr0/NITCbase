#include "BlockAccess.h"

#include <cstring>
#include "iostream"
#include "string"



int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
    if(strcmp(relName,RELCAT_RELNAME)==0||strcmp(relName,ATTRCAT_RELNAME)==0)
       return E_NOTPERMITTED;
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(0);
    Attribute relNameAttr; // (stores relName as type union Attribute)
    // assign relNameAttr.sVal = relName
    strcpy((char*)relNameAttr.sVal,(const char*)relName);
    //  linearSearch on the relation catalog for RelName = relNameAttr
    char relname[16];
    strcpy(relname,RELCAT_ATTR_RELNAME);
    RecId recId=linearSearch(0,relname,relNameAttr,EQ);
    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST
    if(recId.block==-1)
    return E_RELNOTEXIST;
    
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
    RecBuffer buffer(recId.block);
    buffer.getRecord(relCatEntryRecord,recId.slot);
    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    int firstBlock=(int)relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int nextBlock;
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */
    int nAttrs=(int)relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    /*
     Delete all the record blocks of the relation
    */
    // for each record block of the relation:
    struct HeadInfo head;
    while(true){
    
        if(firstBlock==-1)
        break;
        RecBuffer block(firstBlock);
    //     get block header using BlockBuffer.getHeader
        
        block.getHeader(&head);
    //     get the next block from the header (rblock)
        nextBlock=head.rblock;
    //     release the block using BlockBuffer.releaseBlock
        block.releaseBlock();
    //
        
        firstBlock=nextBlock;  
    //     Hint: to know if we reached the end, check if nextBlock = -1
    } 
    
    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/

    // reset the searchIndex of the attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numberOfAttributesDeleted = 0;
    Attribute record[6];
    while(true) {
        RecId attrCatRecId;
        // attrCatRecId = linearSearch on attribute catalog for RelName = relNameAttr
        char rellname[16];
    strcpy(rellname,RELCAT_ATTR_RELNAME);
        attrCatRecId=linearSearch(1,rellname,relNameAttr,EQ);
        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
        //     break;
        if(attrCatRecId.block==-1)
        break;
        numberOfAttributesDeleted++;

        // create a RecBuffer for attrCatRecId.block
        RecBuffer block(attrCatRecId.block);
        // get the header of the block
        block.getHeader(&head);
        // get the record corresponding to attrCatRecId.slot
        block.getRecord(record,attrCatRecId.slot);
        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.
        int rootBlock = /*get root block from the record */(int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        // (This will be used later to delete any indexes if it exists)

        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
        unsigned char slotMap[head.numSlots];
        block.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot]=SLOT_UNOCCUPIED;
        block.setSlotMap(slotMap);
        head.numEntries--;
        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */
        block.setHeader(&head);
        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */
        if (/*   header.numEntries == 0  */head.numEntries==0) {
            /* Standard Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */
            RecBuffer lblock(head.lblock);
            struct HeadInfo lblockhead;
            lblock.getHeader(&lblockhead);
            lblockhead.rblock=head.rblock;
            lblock.setHeader(&lblockhead);
            // create a RecBuffer for lblock and call appropriate methods

            if (/* header.rblock != -1 */head.rblock!=-1) {
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods
                RecBuffer rblock(head.rblock);
            struct HeadInfo rblockhead;
            rblock.getHeader(&rblockhead);
            rblockhead.lblock=head.lblock;
            rblock.setHeader(&rblockhead);

            } else {
                // (the block being released is the "Last Block" of the relation.)
                /* update the Relation Catalog entry's LastBlock field for this
                   relation with the block number of the previous block. */
                   RelCatEntry relCatEntry;
	           RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
                   relCatEntry.lastBlk = head.lblock;
                   RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);
            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)

            // call releaseBlock()
            block.releaseBlock();
        }
        if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
            BPlusTree::bPlusDestroy(rootBlock);
        }

        // (the following part is only relevant once indexing has been implemented)
        // if index exists for the attribute (rootBlock != -1), call bplus destroy
        //if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
            //BPlusTree::bPlusDestroy();
          //  ;
        //}
    }

    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block
    RecBuffer block(4);
    block.getHeader(&head);
    head.numEntries--;
    block.setHeader(&head);
    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */

    /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */
    unsigned char slotMap[(int)head.numSlots];
    block.getSlotMap(slotMap);
    slotMap[recId.slot]=SLOT_UNOCCUPIED;
    block.setSlotMap(slotMap);
    /*** Updating the Relation Cache Table ***/
    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/
    // Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCatBuf);
    relCatBuf.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID,&relCatBuf);
    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted

    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    RelCacheTable::getRelCatEntry(1,&relCatBuf);
    relCatBuf.numRecs-=numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(1,&relCatBuf);
    
    return SUCCESS;
}



int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* get the attribute catalog entry from the attribute cache corresponding
    to the relation with Id=relid and with attribute_name=attrName  */
    AttrCatEntry attrcat;
    int ret = AttrCacheTable::getAttrCatEntry(relId,attrName,&attrcat);

    if(ret != SUCCESS)
        return ret;


    // if this call returns an error, return the appropriate error code
    int rootblck=attrcat.rootBlock;
    // get rootBlock from the attribute catalog entry
    /* if Index does not exist for the attribute (check rootBlock == -1) */ 
    if(rootblck==-1){

        /* search for the record id (recid) corresponding to the attribute with
           attribute name attrName, with value attrval and satisfying the
           condition op using linearSearch()
        */
       recId=linearSearch(relId,attrName,attrVal,op);

    }

    /* else */ 
    else{
        // (index exists for the attribute)

        /* search for the record id (recid) correspoding to the attribute with
        attribute name attrName and with value attrval and satisfying the
        condition op using BPlusTree::bPlusSearch() */
        recId=BPlusTree::bPlusSearch(relId,attrName,attrVal,op);
    }
    if(recId.block==-1 && recId.slot==-1 ){
            return E_NOTFOUND;

    }


    // if there's no record satisfying the given condition (recId = {-1, -1})
    //     return E_NOTFOUND;

    /* Copy the record with record id (recId) to the record buffer (record).
       For this, instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
   RecBuffer buff(recId.block);
   buff.getRecord(record,recId.slot);

    return SUCCESS;
}



int BlockAccess::insert(int relId, Attribute *record) {
    // get the relation catalog entry from relation cache
    // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;
   
    RelCacheTable::getRelCatEntry(relId,&relCatEntry);

    int blockNum =  relCatEntry.firstBlk ;/* first record block of the relation (from the rel-cat entry)*/

    // rec_id will be used to store where the new record will be inserted
    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk/* number of slots per record block */;
    int numOfAttributes = relCatEntry.numAttrs/* number of attributes of the relation */;

    int prevBlockNum = -1/* block number of the last element in the linked list = -1 */;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */
    while (blockNum != -1) {
        RecBuffer buffer(blockNum);
        // get header of block(blockNum) using RecBuffer::getHeader() function
        HeadInfo head;
        buffer.getHeader(&head);
        unsigned char slotmap[head.numSlots];
        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
        buffer.getSlotMap(slotmap);
        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */
        for(int i=0;i<numOfSlots;i++){
        if(slotmap[i]==SLOT_UNOCCUPIED){
           rec_id.block=blockNum;
           rec_id.slot=i;
           break;
        }
       
        }
        if(rec_id.block!=-1)
        break;
        prevBlockNum=blockNum;
        blockNum=head.rblock;

        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */

        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */
    }

    //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
    if(rec_id.block==-1)
    {
        // if relation is RELCAT, do not allocate any more blocks
        //     return E_MAXRELATIONS;
        if(relId==0){
            return E_MAXRELATIONS;
        }

        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        // get the block number of the newly allocated block
        // (use BlockBuffer::getBlockNum() function)
        // let ret be the return value of getBlockNum() function call
        RecBuffer newBlock;
        int ret=newBlock.getBlockNum();
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
        rec_id.block=ret;
        rec_id.slot=0;

        /*
            set the header of the new record block such that it links with
            existing record blocks of the relation
            set the block's header as follows:
            blockType: REC, pblock: -1
            lblock
                  = -1 (if linked list of existing record blocks was empty
                         i.e this is the first insertion into the relation)
                  = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numOfSlots, numAttrs: numOfAttributes
            (use BlockBuffer::setHeader() function)
        */
        HeadInfo head;
        head.blockType=REC;
        head.pblock=-1;
        head.lblock=prevBlockNum;
        head.rblock=-1;
        head.numSlots=numOfSlots;
        head.numAttrs=numOfAttributes;
        head.numEntries = 0;
        newBlock.setHeader(&head);
       // newBlock.setBlockType(REC);
        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */
        unsigned char slotmap[numOfSlots];
        for(int i=0;i<numOfSlots;i++){
           slotmap[i]=SLOT_UNOCCUPIED;
        }
        newBlock.setSlotMap(slotmap);

        // if prevBlockNum != -1
        if(prevBlockNum!=-1)
        {
            // create a RecBuffer object for prevBlockNum
            // get the header of the block prevBlockNum and
            // update the rblock field of the header to the new block
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)
            RecBuffer block(prevBlockNum);
            block.getHeader(&head);
            head.rblock=rec_id.block;
            block.setHeader(&head);
        }
        // else
        else
        {
            // update first block field in the relation catalog entry to the
            // new block (using RelCacheTable::setRelCatEntry() function)
            relCatEntry.firstBlk=ret;
            RelCacheTable::setRelCatEntry(relId,&relCatEntry);
        }

        // update last block field in the relation catalog entry to the
        // new block (using RelCacheTable::setRelCatEntry() function)
        relCatEntry.lastBlk=ret;
        RelCacheTable::setRelCatEntry(relId,&relCatEntry);
    }

    // create a RecBuffer object for rec_id.block
    // insert the record into rec_id'th slot using RecBuffer.setRecord())

    RecBuffer block(rec_id.block);
    block.setRecord(record,rec_id.slot);
    
    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */

       unsigned char slotmap[numOfSlots];
       block.getSlotMap(slotmap);
       slotmap[rec_id.slot]=SLOT_OCCUPIED;
       block.setSlotMap(slotmap);

    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
    struct HeadInfo head;
    block.getHeader(&head);
    head.numEntries++;
    block.setHeader(&head);

    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId,&relCatEntry);

    /* B+ Tree Insertions */
    // (the following section is only relevant once indexing has been implemented)

    int flag = SUCCESS;
    // Iterate over all the attributes of the relation
    // (let attrOffset be iterator ranging from 0 to numOfAttributes-1)
    for(int attrOffset=0;attrOffset<relCatEntry.numAttrs;attrOffset++)
    {
        // get the attribute catalog entry for the attribute from the attribute cache
        // (use AttrCacheTable::getAttrCatEntry() with args relId and attrOffset)
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId,attrOffset,&attrCatEntry);
        int rootblk=attrCatEntry.rootBlock;

        // get the root block field from the attribute catalog entry

        // if index exists for the attribute(i.e. rootBlock != -1)
        if(rootblk!=-1)
        {
            /* insert the new record into the attribute's bplus tree using
             BPlusTree::bPlusInsert()*/
            int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName,
                                                record[attrOffset], rec_id);

            if (retVal == E_DISKFULL) {
                //(index for this attribute has been destroyed)
                flag = E_INDEX_BLOCKS_RELEASED;
                
            }
        }
    }

    return flag;
}



int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(0);
    Attribute relNameAttr;    // set relNameAttr to relName
    strcpy(relNameAttr.sVal, relName);
    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    RecId searchIndex;
     char  cris[16];
     strcpy(cris,"RelName");
     searchIndex=linearSearch(0, cris,  relNameAttr, EQ);
     if(searchIndex.block==-1&&searchIndex.slot==-1)
        return E_RELNOTEXIST;
    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(1);
    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId;
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];
    
    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr
        searchIndex=linearSearch(1, cris,  relNameAttr, EQ);
        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;
      
        if(searchIndex.block==-1&&searchIndex.slot==-1)
           break;

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        RecBuffer buffer(searchIndex.block);
        buffer.getRecord(attrCatEntryRecord,searchIndex.slot);
        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record
       
     
        if(strcmp(attrCatEntryRecord[1].sVal,oldName)==0){
           attrToRenameRecId.block=searchIndex.block;
           attrToRenameRecId.slot=searchIndex.slot;
           
        }
        
        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
        if(strcmp(attrCatEntryRecord[1].sVal,newName)==0)
        return E_ATTREXIST;
    }
   
    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;
    if(attrToRenameRecId.block==-1&&attrToRenameRecId.slot==-1)
        return E_ATTRNOTEXIST;

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    RecBuffer buffer(attrToRenameRecId.block);
    
    buffer.getRecord(attrCatEntryRecord,attrToRenameRecId.slot);   
   
    //   update the AttrName of the record with newName
    strcpy(attrCatEntryRecord[1].sVal,newName);
    //   set back the record with RecBuffer.setRecord
    buffer.setRecord(attrCatEntryRecord,attrToRenameRecId.slot);
    return SUCCESS;
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(0);
    Attribute newRelationName;    // set newRelationName with newName
    strcpy(newRelationName.sVal, newName);
    // search the relation catalog for an entry with "RelName" = newRelationName
     RecId searchIndex;
     char  cris[16];
     strcpy(cris,"RelName");
     
     searchIndex=linearSearch(0, cris,  newRelationName, EQ);
    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;
     if(searchIndex.block!=-1&&searchIndex.slot!=-1)
        return E_RELEXIST;
 
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(0);
    Attribute oldRelationName;    // set oldRelationName with oldName
    strcpy(oldRelationName.sVal,oldName);
    // search the relation catalog for an entry with "RelName" = oldRelationName
    searchIndex=linearSearch(0, cris,  oldRelationName, EQ);
    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;
    if(searchIndex.block==-1&&searchIndex.slot==-1)
        return E_RELNOTEXIST;
    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    RecBuffer buffer(RELCAT_BLOCK);
    Attribute record[6];
    buffer.getRecord(record,searchIndex.slot);
    int haha=(int)record[1].nVal;

    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
       
    strcpy(record[0].sVal,newName);
    // set back the record value using RecBuffer.setRecord
    buffer.setRecord(record,searchIndex.slot);
    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */
    
    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(1);
    
    //for i = 0 to numberOfAttributes :
    //    linearSearch on the attribute catalog for relName = oldRelationName
    //    get the record using RecBuffer.getRecord
    //
    //    update the relName field in the record to newName
    //    set back the record using RecBuffer.setRecord
    //printf("\nno of records is %d\n",record[1].nVal);
    for(int i=0;i<haha;i++){
       searchIndex=linearSearch(1, cris,  oldRelationName, EQ);
       RecBuffer buffer(searchIndex.block);
       buffer.getRecord(record,searchIndex.slot);
       strcpy(record[0].sVal,newName);
       
       buffer.setRecord(record,searchIndex.slot);
       
    }
   
    return SUCCESS;
}














RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    // get the previous search index of the relation relId from the relation cache
   
    RecId searchIndex;
    RecId prevRecId;
    int block,slot,numSlots,offset,attrType;
    struct HeadInfo head;
    AttrCatEntry attrCatBuf;
    // (use RelCacheTable::getSearchIndex() function)
    
    RelCacheTable::getSearchIndex(relId,&prevRecId);
    
    RelCatEntry relCatBuf;
    
    RelCacheTable::getRelCatEntry(relId,&relCatBuf);
  
    Attribute record[relCatBuf.numAttrs];
    // let block and slot denote the record id of the record being currently checked

    // if the current search index record is invalid(i.e. both block and slot = -1)
    
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (no hits from previous search; search should start from the
        // first record itself)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        
        // block = first record block of the relation
        block=relCatBuf.firstBlk;
        // slot = 0
        slot=0;
        
    }
    else
    {

        // (there is a hit from previous search; search should start from
        // the record next to the search index record)

        // block = search index's block
        block=prevRecId.block;
        // slot = search index's slot + 1
        slot=prevRecId.slot+1;
    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
         RecBuffer recordBlock(block);
        // get the record with id (block, slot) using RecBuffer::getRecord()
        // get header of the block using RecBuffer::getHeader() function
        recordBlock.getHeader(&head);
        // get slot map of the block using RecBuffer::getSlotMap() function
        unsigned char slotMap[head.numSlots];
        numSlots=head.numSlots;
        recordBlock.getSlotMap(slotMap);
        // If slot >= the number of slots per block(i.e. no more slots in this block)
        if(slot>=numSlots)
        {
            // update block = right block of block
            block=head.rblock;
            //RecBuffer recordBlock(block);
            //recordBlock(block);
            //recordBlock.getHeader(&head);
            // update slot = 0
            slot=0;
            continue;  // continue to the beginning of this while loop
        }

        // if slot is free skip the loop
        // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)

        if(slotMap[slot]==SLOT_UNOCCUPIED)
        {
            // increment slot and continue to the next record slot
            slot++;
            continue;
        }

        // compare record's attribute value to the the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using
            AttrCacheTable::getAttrCatEntry()
        */
        AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatBuf);
        offset=attrCatBuf.offset;
        /* use the attribute offset to get the value of the attribute from
           current record */
        // get the record with id (block, slot) using RecBuffer::getRecord()
        recordBlock.getRecord(record, slot);
        int cmpVal;  // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        attrType=attrCatBuf.attrType;
        cmpVal=compareAttrs(record[offset],attrVal,attrType);
        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            /*
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            
            searchIndex.block=block;
            searchIndex.slot=slot;
            RelCacheTable::setSearchIndex(relId, &searchIndex);
            return searchIndex;
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1, -1};
}


/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);

    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block, slot;


    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)

        // block = first record block of the relation
        // slot = 0
        RelCatEntry  relcat;
        RelCacheTable::getRelCatEntry(relId,&relcat);
        block=relcat.firstBlk;
        slot=0;
    }
    else
    {
        // (a project/search operation is already in progress)

        // block = previous search index's block
        // slot = previous search index's slot + 1
        block=prevRecId.block;
        slot=prevRecId.slot+1;
    }


    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)

        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function

        RecBuffer recbuff(block);
        HeadInfo head;
        recbuff.getHeader(&head);
        unsigned char slotmap[head.numSlots];
        recbuff.getSlotMap(slotmap);

        if(slot >=head.numSlots /* slot >= the number of slots per block*/)
        {
            // (no more slots in this block)
            // update block = right block of block
            // update slot = 0
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
            block=head.rblock;
            slot=0;
            continue;
        }
        else if (slotmap[slot]==SLOT_UNOCCUPIED  /* slot is free */)
        { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)

            // increment slot
            slot++;
            continue;
        }
        else {
            // (the next occupied slot / record has been found)
            
            break;
        }
    }

    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};
    nextRecId.block=block;
    nextRecId.slot=slot;
    RelCacheTable::setSearchIndex(relId,&nextRecId);

    // set the search index to nextRecId using RelCacheTable::setSearchIndex

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
   RecBuffer buff(nextRecId.block);
   buff.getRecord(record,nextRecId.slot);

    return SUCCESS;
}
