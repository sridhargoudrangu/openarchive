/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __CMDLINE_H__
#define __CMDLINE_H__

struct backup_args
{
    char *src_product;
    char *src_store;
    char *dest_product;
    char *dest_store;
    char *inp_loc;
    char *outp_loc;
};
typedef struct backup_args backup_args_t;
   
struct stub_args
{
    char *src_product;
    char *src_store;
    char *dest_product;
    char *dest_store;
    char *inp_loc;
    char *outp_loc;
};
typedef struct stub_args stub_args_t;

enum scan_type
{
    OPENARCHIVE_FULL_SCAN = 1,
    OPENARCHIVE_INCR_SCAN = 2
};
typedef enum scan_type scan_type_t;
  
struct scan_args
{
    scan_type_t type;
    char *src_product;
    char *src_store;
    char *outp_loc;
};
typedef struct scan_args scan_args_t;

enum openarchive_args_type
{
    OPENARCHIVE_SCAN_ARGS = 1,
    OPENARCHIVE_BACKUP_ARGS = 2,
    OPENARCHIVE_STUB_ARGS = 3,
    OPENARCHIVE_UNDEF_ARGS = 128 
};
typedef enum openarchive_args_type openarchive_args_type_t;

void usage (const char *);
int parse_backup_args (int, char *[ ], backup_args_t *);
int parse_stub_args (int, char *[ ], stub_args_t *);
int parse_scan_args (int, char *[ ], scan_args_t *);
openarchive_args_type_t get_args_type (int, char *[ ]);
void* alloc_args (openarchive_args_type_t);
void free_args (void **); 
int parse_cmdline_params (int, char *[ ], openarchive_args_type_t *, void **);

#endif
