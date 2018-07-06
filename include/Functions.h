#ifndef FUNCTIONS_H
#define FUNCTIONS_H

//#include "AM.h"
#include "bf.h"

BF_ErrorCode BF_AllocateIndexBlock(int ,BF_Block * ,void *,int ,int );

BF_ErrorCode BF_AllocateDataBlock(int ,BF_Block *,void *,void *,int );

int setNext(int ,int ,int);

int getNext(int ,int);

int getParent(int ,int);

int isGreaterString(char *,char *);

int isGreaterInteger(int ,int );

int isGreaterFloat(float ,float );

int isGreater(char *,char *,int );

int findBlockToInsert(int ,void* ,int* ,int ,int ,char );

int isFullDataBlock(int,int);

int isFullIndexBlock(int,int);

int insertInDataBlock(int ,int ,void* ,void* );

int dataBlockSplit(int ,int ,void*,void* ,void*);

int insertInIndexBlock(int ,int ,void* ,int );

int insertInNotFull(int ,int ,void* ,int );

int firstDataBlock(int);

int findRecord(int );

void findNextRecord(int,int);

#endif
