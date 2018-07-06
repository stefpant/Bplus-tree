#ifndef MY_STRUCTS_H_
#define MY_STRUCTS_H_

#include "bf.h"


#define AM_MAX_OPEN_FILES 20
#define AM_MAX_OPEN_SCANS 20

typedef struct File_type{
  char* fileName;
  int fileDesc;
  char type1;
  int size1;
  char type2;
  int size2;
  int max_index_records;
  int max_data_records;
  int root;
}File_type;

typedef struct Scan_type{
  int fileDesc;
  int op;
  void* value1;//value to compare
  int Current_block_num;
  int Current_record_num;
  char type1;
  int size1;
  char type2;
  int size2;
  void* value2;
  void* value3;//return value
}Scan_type;

extern File_type** AM_File;
extern Scan_type** AM_Scan;

#endif
