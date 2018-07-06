#include "AM.h"
#include "bf.h"
#include "my_structs.h"
#include "defn.h"
#include "Functions.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define AM_ID '#'

int AM_errno = AME_OK;
File_type** AM_File;
Scan_type** AM_Scan;

int c=0;

void AM_Init() {
  int i;
  BF_Init(MRU);
  //allocate memory for the arrays
  AM_File = malloc(AM_MAX_OPEN_FILES*sizeof(File_type *));
  AM_Scan = malloc(AM_MAX_OPEN_SCANS*sizeof(Scan_type *));
  //initialize arrays with NULL
  for(i=0;i<AM_MAX_OPEN_FILES;i++)
    AM_File[i] = NULL;
  for(i=0;i<AM_MAX_OPEN_SCANS;i++)
    AM_Scan[i] = NULL;
  return;
}


int AM_CreateIndex(char *fileName,
	               char attrType1,
	               int attrLength1,
	               char attrType2,
	               int attrLength2) {
  //check if given arguments are correct
  if(attrType1 != STRING && attrType1 != INTEGER && attrType1 != FLOAT){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }
  if(attrType2 != STRING && attrType2 != INTEGER && attrType2 != FLOAT){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }
  if(attrType1 == INTEGER && attrLength1 != sizeof(int)){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }
  if(attrType1 == FLOAT && attrLength1 != sizeof(float)){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }
  if(attrType1 == STRING && (attrLength1 < 1 || attrLength1 > 255)){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }
  if(attrType2 == INTEGER && attrLength2 != sizeof(int)){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }
  if(attrType2 == FLOAT && attrLength2 != sizeof(float)){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }
  if(attrType2 == STRING && (attrLength2 < 1 || attrLength2 > 255)){
    AM_errno = AME_INVALID_TYPE;
    return AME_ERROR;
  }

  int fd;
  BF_ErrorCode code;
  BF_Block *block;
  BF_Block_Init(&block);
  char *data;
  char am_id = AM_ID;
  BF_ErrorCode be;
  if(BF_CreateFile(fileName) != BF_OK){
    AM_errno = AME_FILE_ALREADY_EXISTS;
    BF_Close();//if file already exists
    return AME_ERROR;
  }
  //give identification for am file
  if(BF_OpenFile(fileName,&fd)!= BF_OK){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  if(BF_AllocateBlock(fd,block)!= BF_OK){
    AM_errno = AME_FULL_MEMORY_ERROR;
    return AME_ERROR;
  }
  data = BF_Block_GetData(block);
  memcpy(data, &am_id, sizeof(char));
  memcpy(data + sizeof(char), &attrType1, sizeof(char));
  memcpy(data + 2*sizeof(char), &attrLength1, sizeof(int));
  memcpy(data + 2*sizeof(char) + sizeof(int), &attrType2, sizeof(char));
  memcpy(data + 3*sizeof(char) + sizeof(int), &attrLength2, sizeof(int));
  //metadata block now should be like: [id,type1,size1,type2,size2,...]
  BF_Block_SetDirty(block);
  if(BF_UnpinBlock(block)!= BF_OK)
    BF_PrintError(BF_ERROR);
  code = BF_CloseFile(fd);
  if(code != BF_AVAILABLE_PIN_BLOCKS_ERROR && code != BF_OK){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  BF_Block_Destroy(&block);
  return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
  if(access(fileName, F_OK) == -1){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  int i;
  for(i=0; i<AM_MAX_OPEN_FILES; i++){
    if(AM_File[i] != NULL){
      if(!strcmp(AM_File[i]->fileName,fileName)){
        AM_errno = AME_FILE_IN_USE;
        return AME_ERROR;//file is open we can't delete it
      }
    }
  }
  char* command;
  command = malloc(6*sizeof(char) + sizeof(fileName));
  strcpy(command,"rm -f ");
  strcat(command,fileName);
  system(command);
  free(command);
  return AME_OK;
}


int AM_OpenIndex (char *fileName) {
  if(access(fileName, F_OK) == -1){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  BF_Block *block;
  BF_Block_Init(&block);
  char *data;
  int counter;//file block counter
  int fd;//file desc
  char am_id;//put the first char of metadata block
  int flag = 0;//if AM_File is full return error
  int i;
  if(BF_OpenFile(fileName,&fd)!= BF_OK){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  //now let's check if the file is am file...
  if(BF_GetBlockCounter(fd,&counter) != BF_OK){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  if(counter==0){//empty file
    AM_errno = AME_EMPTY_FILE_ERROR;
    return AME_ERROR;
  }
  if(BF_GetBlock(fd,0,block) != BF_OK){
    AM_errno = AME_FULL_MEMORY_ERROR;
    return AME_ERROR;
  }
  data = BF_Block_GetData(block);
  memcpy(&am_id,data,sizeof(char));
  if(am_id!=AM_ID){//if it's not am file exit
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  //find an empty place in AM_File array to put our file's data
  for(i=0; i<AM_MAX_OPEN_FILES; i++){
    if(AM_File[i]==NULL){
      AM_File[i] = malloc(sizeof(File_type));
      AM_File[i]->fileDesc = fd;
      AM_File[i]->fileName = malloc(sizeof(fileName));
      strcpy(AM_File[i]->fileName, fileName);
      memcpy(&(AM_File[i]->type1), data + sizeof(char), sizeof(char));
      memcpy(&(AM_File[i]->size1), data + 2*sizeof(char), sizeof(int));
      memcpy(&(AM_File[i]->type2), data + 2*sizeof(char) + sizeof(int), sizeof(char));
      memcpy(&(AM_File[i]->size2), data + 3*sizeof(char) + sizeof(int), sizeof(int));
      AM_File[i]->max_index_records = (BF_BLOCK_SIZE - 3*sizeof(int) - sizeof(char))/(AM_File[i]->size1 + sizeof(int));
      AM_File[i]->max_data_records = (BF_BLOCK_SIZE - 2*sizeof(int) - sizeof(char))/(AM_File[i]->size1 + AM_File[i]->size2);
      if(counter>1)//for already opened file get root
        memcpy(&(AM_File[i]->root), data + 3*sizeof(char) + 2*sizeof(int),  sizeof(int));
      flag = 1;//empty place found
      break ;
    }
  }

  if(BF_UnpinBlock(block) != BF_OK)//unpin block here to be sure we won't
    BF_PrintError(BF_ERROR);  //exit the function leaving the block pinned
  if(!flag){//if AM_File is full exit;
    AM_errno = AME_OPEN_FILES_LIMIT_ERROR;
    return AME_ERROR;
  }
  BF_Block_Destroy(&block);
  return i;//return AM_File's position which has the file's data
}


int AM_CloseIndex (int fileDesc) {
  int i;
  char* data;
  BF_ErrorCode code;
  if(fileDesc<0 || fileDesc>AM_MAX_OPEN_FILES || AM_File[fileDesc]==NULL){
    AM_errno = AME_INVALID_FILEDESC_ERROR;
    return AME_ERROR;
  }
  //check for open scans and if it has return error
  for(i=0; i<AM_MAX_OPEN_SCANS; i++){
    if(AM_Scan[i] != NULL){
      if(AM_Scan[i]->fileDesc == AM_File[fileDesc]->fileDesc){
        AM_errno = AME_FILE_WITH_OPEN_SCANS;
        return AME_ERROR;//we can't close the file
      }
    }
  }
  //save new root in metadata
  BF_Block *block;
  BF_Block_Init(&block);
  if(BF_GetBlock(fileDesc,0,block) != BF_OK){
    AM_errno = AME_FULL_MEMORY_ERROR;
    return AME_ERROR;
  }
  data = BF_Block_GetData(block);
  memcpy(data + 3*sizeof(char) + 2*sizeof(int), &(AM_File[fileDesc]->root), sizeof(int));
  BF_Block_SetDirty(block);
  if(BF_UnpinBlock(block)!= BF_OK)
    BF_PrintError(BF_ERROR);
  code = BF_CloseFile(AM_File[fileDesc]->fileDesc);
  if(code != BF_AVAILABLE_PIN_BLOCKS_ERROR && code != BF_OK){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  if(AM_File[fileDesc] != NULL){
    free(AM_File[fileDesc]->fileName);
    free(AM_File[fileDesc]);
    AM_File[fileDesc]=NULL;
  }
  BF_Block_Destroy(&block);
  return AME_OK;
}


int AM_InsertEntry(int fd, void *value1, void *value2) {
  //////check fd before accessing in AM_File[fd]
  if(fd<0 || fd>AM_MAX_OPEN_FILES || AM_File[fd]==NULL){
    AM_errno = AME_INVALID_FILEDESC_ERROR;
    return AME_ERROR;
  }
  //////
  BF_Block *block;
  BF_Block_Init(&block);
  char* data;
  int counter;
  int parent;
  int temp_error;
  if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&counter) != BF_OK){
    AM_errno = AME_INVALID_FILE_ERROR;
    return AME_ERROR;
  }
  if(counter == 1){ //here we check if it's the first block of the file
    //if it is then we create our index root
    if(BF_AllocateIndexBlock(AM_File[fd]->fileDesc,block,value1,1,-1) != BF_OK)
      return AME_ERROR;
    //we also have to update our current AM_File
    AM_File[fd]->root = 1;//root is the second allocated block(block num=1)
    char* data = BF_Block_GetData(block);
    counter = 2;
    memcpy(data + sizeof(char) + 2*sizeof(int), &counter, sizeof(int));//left child

    //the right pointer of our new IndexBlock must point to our new DataBlock
    counter = 3;//in our case the block number of first DataBlock is 2
    memcpy(data + sizeof(char) + 3*sizeof(int) + (AM_File[fd]->size1), &counter, sizeof(int));
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block) != BF_OK)
      BF_PrintError(BF_ERROR);

    if(BF_AllocateDataBlock(fd,block,value1,value2,0) != BF_OK)
      return AME_ERROR;
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block) != BF_OK)
      BF_PrintError(BF_ERROR);
    if(BF_AllocateDataBlock(fd,block,value1,value2,1) != BF_OK)
      return AME_ERROR;
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block) != BF_OK)
      BF_PrintError(BF_ERROR);

    if(setNext(fd, 2, 3) == AME_ERROR) return AME_ERROR;//
  }
  else{
    int blockToInsert = findBlockToInsert(AM_File[fd]->fileDesc,value1,&parent,AM_File[fd]->size1,AM_File[fd]->root,AM_File[fd]->type1);
    if(blockToInsert == AME_ERROR) return AME_ERROR;
    if(!(temp_error=isFullDataBlock(fd,blockToInsert))){
      if(temp_error == AME_ERROR) return AME_ERROR;//cause !(-2) = true so we check it here
      if(insertInDataBlock(fd,blockToInsert,value1,value2) == AME_ERROR) return AME_ERROR;
    }
    else{//full data block, we have to allocate a new one
      void* value;
      if(AM_File[fd]->type1==STRING)
        value = malloc(AM_File[fd]->size1*sizeof(char));
      else
        value = malloc(64);
      int newBlock = dataBlockSplit(fd,blockToInsert,value,value1,value2);
      if(newBlock == AME_ERROR) return AME_ERROR;
      if(!(temp_error = isFullIndexBlock(fd,parent))){
        if(temp_error == AME_ERROR) return AME_ERROR;
        if(insertInIndexBlock(fd,parent,value,newBlock) == AME_ERROR) return AME_ERROR;
      }
      else{
        //splits blocks until empty place is found
        if(insertInNotFull(fd,parent,value,newBlock) == AME_ERROR) return AME_ERROR;
      }

      free(value);
    }
  }
  BF_Block_Destroy(&block);
  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  BF_Block *block;
  BF_Block_Init(&block);
  char* data;
  int index;
  int flag = 1;
  for(index=0; index<AM_MAX_OPEN_SCANS; index++){
    if(AM_Scan[index] == NULL){
      AM_Scan[index] = malloc(sizeof(Scan_type));
      flag = 0;
      break;
    }
  }
  if(flag==1){
    AM_errno = AME_OPEN_SCANS_LIMIT_ERROR;
    return AME_ERROR;
  }
  else{//
    int root;
    AM_Scan[index]->fileDesc = fileDesc;
    AM_Scan[index]->op = op;
    if(BF_GetBlock(fileDesc,0,block) != BF_OK){
      AM_errno = AME_INVALID_FILE_ERROR;
      free(AM_Scan[index]);
      return AME_ERROR;
    }
    data = BF_Block_GetData(block);
    memcpy(&(AM_Scan[index]->type1), data + sizeof(char), sizeof(char));
    memcpy(&(AM_Scan[index]->size1), data + 2*sizeof(char), sizeof(int));
    memcpy(&(AM_Scan[index]->type2), data + 2*sizeof(char) + sizeof(int), sizeof(char));
    memcpy(&(AM_Scan[index]->size2), data + 3*sizeof(char) + sizeof(int), sizeof(int));
    memcpy(&root, data + 3*sizeof(char) + 2*sizeof(int), sizeof(int));
    if(BF_UnpinBlock(block) != BF_OK)
      BF_PrintError(BF_ERROR);
    BF_Block_Destroy(&block);
    if(AM_Scan[index]->type1==STRING){
      AM_Scan[index]->value1 = malloc(AM_Scan[index]->size1*sizeof(char));
      AM_Scan[index]->value2 = malloc(AM_Scan[index]->size1*sizeof(char));
      memcpy(AM_Scan[index]->value1,value,AM_Scan[index]->size1);
    }
    else{
      AM_Scan[index]->value1 = malloc(64);
      AM_Scan[index]->value2 = malloc(64);
      memcpy(AM_Scan[index]->value1,value,64);
    }

    if(AM_Scan[index]->type2==STRING)
      AM_Scan[index]->value3 = malloc(AM_Scan[index]->size2*sizeof(char));
    else
      AM_Scan[index]->value3 = malloc(64);
    int temp;
    if(op==EQUAL || op==GREATER_THAN || op==GREATER_THAN_OR_EQUAL){
      AM_Scan[index]->Current_block_num = findBlockToInsert(fileDesc,value,&temp,AM_Scan[index]->size1,root,AM_Scan[index]->type1);
      AM_Scan[index]->Current_record_num = findRecord(index);
    }
    else{
      AM_Scan[index]->Current_block_num = firstDataBlock(fileDesc);
      AM_Scan[index]->Current_record_num = 0;
    }

  }
  return index;

}


void *AM_FindNextEntry(int scanDesc) {
  //////check sd before accessing in AM_File[fd]
  if(scanDesc<0 || scanDesc>AM_MAX_OPEN_FILES || AM_Scan[scanDesc]==NULL){
    AM_errno = AME_INVALID_FILEDESC_ERROR;
    return NULL;
  }
  //////

  AM_errno = AME_OK;

  int op = AM_Scan[scanDesc]->op;
  int *iv1;
  int *iv2;
  float *fv1;
  float *fv2;

  if(op != NOT_EQUAL && op != GREATER_THAN)
    findNextRecord(scanDesc,0);
  else
    findNextRecord(scanDesc,1);

  if(AM_errno == AME_EOF)
    return NULL;

  int flag = 0;
  //flag = 0 -> v1=v2
  //flag = 1 -> v1<v2
  //flag = 2 -> v1>v2
  if(AM_Scan[scanDesc]->type1 == STRING){
    if(strcmp(AM_Scan[scanDesc]->value1,AM_Scan[scanDesc]->value2)<0) flag=1;//v1<v2
    else if(strcmp(AM_Scan[scanDesc]->value1,AM_Scan[scanDesc]->value2)>0) flag=2;//v1>v2
  }
  else if(AM_Scan[scanDesc]->type1 == INTEGER){
    iv1 = (int*)AM_Scan[scanDesc]->value1;
    iv2 = (int*)AM_Scan[scanDesc]->value2;
    if(*iv1 < *iv2) flag = 1;
    else if(*iv1>*iv2) flag = 2;
  }
  else{
    fv1 = (float*)AM_Scan[scanDesc]->value1;
    fv2 = (float*)AM_Scan[scanDesc]->value2;
    if(*fv1<*fv2) flag = 1;
    else if(*fv1>*fv2) flag = 2;
  }

  if(op == EQUAL){
    if(flag==1 || flag==2){AM_errno = AME_EOF;}
  }
//  else if(op == NOT_EQUAL){
//  }
  else if(op == LESS_THAN){
    if(flag==0 || flag==1){AM_errno = AME_EOF;}
  }
  else if(op == GREATER_THAN){
    if(flag==0 || flag==2){AM_errno = AME_EOF;}
  }
  else if(op == LESS_THAN_OR_EQUAL){
    if(flag==1){AM_errno = AME_EOF;}
  }
  else if(op == GREATER_THAN_OR_EQUAL){
    if(flag==2){AM_errno = AME_EOF;}
  }

  if(AM_errno == AME_EOF)
    return NULL;

  return AM_Scan[scanDesc]->value3;
}


int AM_CloseIndexScan(int scanDesc) {
  if(scanDesc<0 || scanDesc>=AM_MAX_OPEN_SCANS){
    AM_errno = AME_INVALID_FILEDESC_ERROR;
    return AME_ERROR;
  }

  if(AM_Scan[scanDesc] != NULL){
    free(AM_Scan[scanDesc]->value1);
    free(AM_Scan[scanDesc]->value2);
    free(AM_Scan[scanDesc]->value3);
    free(AM_Scan[scanDesc]);
    AM_Scan[scanDesc] = NULL;
  }
  return AME_OK;
}

void AM_PrintError(char *errString) {
  printf("%s",errString);
  if(AM_errno == AME_OK)
    printf("{OK!(no errors)}\n");
  else if(AM_errno == AME_EOF)
    printf("{EOF found!}\n");
  else if(AM_errno == AME_ERROR)
    printf("AM error!\n");
  else if(AM_errno == AME_INVALID_FILEDESC_ERROR)
    printf("Found invalid filedesc!\n");
  else if(AM_errno == AME_FULL_MEMORY_ERROR)
    printf("Memory full of active blocks\n");
  else if(AM_errno == AME_WRONG_BLOCK_NUMBER)
    printf("Wrong given block number in function!\n");
  else if(AM_errno == AME_INVALID_TYPE)
    printf("Invalid TYPE or SIZE!\n");
  else if(AM_errno == AME_FILE_ALREADY_EXISTS)
    printf("File already exists!\n");
  else if(AM_errno == AME_INVALID_FILE_ERROR)
    printf("File not exists or is not AM!\n");
  else if(AM_errno == AME_FILE_IN_USE)
    printf("File is open,can not be deleted!\n");
  else if(AM_errno == AME_EMPTY_FILE_ERROR)
    printf("AM file is empty!\n");
  else if(AM_errno == AME_OPEN_FILES_LIMIT_ERROR)
    printf("Max number of open files reached,can not open more files!\n");
  else if(AM_errno == AME_OPEN_SCANS_LIMIT_ERROR)
    printf("Max number of open scans reached,can not scan more files!\n");
  else if(AM_errno == AME_FILE_WITH_OPEN_SCANS)
    printf("File has open scans,can not be closed!\n");
}

void AM_Close() {
  int i;
  for(i=0; i<AM_MAX_OPEN_FILES; i++){
    if(AM_File[i] != NULL){
      free(AM_File[i]->fileName);
      free(AM_File[i]);
      AM_File[i] = NULL;
    }
  }
  free(AM_File);
  for(i=0; i<AM_MAX_OPEN_SCANS; i++){
    if(AM_Scan[i] != NULL){
      free(AM_Scan[i]->value1);
      free(AM_Scan[i]->value2);
      free(AM_Scan[i]->value3);
      free(AM_Scan[i]);
      AM_Scan[i] = NULL;
    }
  }
  free(AM_Scan);
  BF_Close();
}
