/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <cmdline.h>

void usage (const char *app)
{
    fprintf (stderr, "NAME\n");
    fprintf (stderr, "\t%s - "
                     "command line interface to openarchive\n\n", app);
    fprintf (stderr, "SYNOPSIS\n");
    fprintf (stderr, "\t%s [options]\n\n", app);
    fprintf (stderr, "DESCRIPTION\n");
    fprintf (stderr, "\tOpenArchive provides the capability to integrate data "
                     "storage and data management products. openarchive provides a "
                     "command line interface to OpenArchive. Various data "
                     "management functions can be performed on different "
                     "products using openarchive. Support is provided for the "
                     "following products: \n");
    fprintf (stderr, "\t\tglusterfs\n");
    fprintf (stderr, "\t\t\tstore information is volume name\n\n");
    fprintf (stderr, "\t\tCommvault\n");
    fprintf (stderr, "\t\t\tstore information will be in following format:\n");
    fprintf (stderr, "\t\t\tcc=2:cn=node1:ph=node2:pp=8400:at=29:in=Instance001:"
                     "bs=idm:sc=arch:ji=0:jt=full-backup:ns=16\n");
    fprintf(stderr, "\t\t\tMeaning of each argument is explained below: \n");
    fprintf(stderr, "\t\t\t\t cc - CommCell ID\n");
    fprintf(stderr, "\t\t\t\t cn - Client name\n");
    fprintf(stderr, "\t\t\t\t ph - Proxy host name\n");
    fprintf(stderr, "\t\t\t\t pp - Proxy port number\n");
    fprintf(stderr, "\t\t\t\t at - App type. Valid values are: \n");
    fprintf(stderr, "\t\t\t\t\t 29 - Linux File system \n");
    fprintf(stderr, "\t\t\t\t in - Instance Name \n"); 
    fprintf(stderr, "\t\t\t\t bs - BackupSet Name \n");
    fprintf(stderr, "\t\t\t\t sc - SubClient Name \n");
    fprintf(stderr, "\t\t\t\t ji - Job ID \n");
    fprintf(stderr, "\t\t\t\t jt - Job Type. Valid values are: \n"); 
    fprintf(stderr, "\t\t\t\t\t browse,full-backup,incr-backup,restore \n");
    fprintf(stderr, "\t\t\t\t ns - Number of streams \n");
    fprintf (stderr, "\nOPTIONS\n");

    fprintf (stderr, "\tbackup <source product> <source store> "
                     "<destination product> <destination store> "
                     "<path to input file> <path to failed files>\n");
    fprintf (stderr, "\t\tWill backup list of files using OpenArchive.\n\n");   
    
    fprintf (stderr, "\tscan <full|incr> <source product> <source store> " 
                     "<path to output file>\n");
    fprintf (stderr, "\t\tWill determine the list of files to be backed up "
                     "using OpenArchive.\n\n");   

    fprintf (stderr, "\tstub <source product> <source store> "
                     "<destination product> <destination store> "
                     "<path to input file> <path to failed files>\n");
    fprintf (stderr, "\t\tWill archive list of files using OpenArchive.\n\n");   

    fprintf (stderr, "\n");

    exit (EXIT_FAILURE);
}

int parse_backup_args (int argc, char *argv[ ], backup_args_t * bckarg)
{
    if (8 == argc) {
        bckarg->src_product = argv[2];
        bckarg->src_store = argv[3];
        bckarg->dest_product = argv[4];
        bckarg->dest_store = argv[5];
        bckarg->inp_loc = argv[6];
        bckarg->outp_loc = argv[7];

        return 0;   
    }

    return -1;  
}

int parse_stub_args (int argc, char *argv[ ], stub_args_t * stubarg)
{
    if (8 == argc) {
        stubarg->src_product = argv[2];
        stubarg->src_store = argv[3];
        stubarg->dest_product = argv[4];
        stubarg->dest_store = argv[5];
        stubarg->inp_loc = argv[6];
        stubarg->outp_loc = argv[7];

        return 0;   
    }

    return -1;  
}

int parse_scan_args (int argc, char *argv[ ], scan_args_t *scanarg)
{
    if (6 == argc) {
        if (!strcmp (argv[2], "full")) {
            scanarg->type = OPENARCHIVE_FULL_SCAN;
        } else if (!strcmp (argv[2], "incr")) {
            scanarg->type = OPENARCHIVE_INCR_SCAN;
        } else {
            return -1;  
        }  
        scanarg->src_product = argv[3];
        scanarg->src_store = argv[4];
        scanarg->outp_loc = argv[5];

        return 0;   
    }

    return -1;  
}

openarchive_args_type_t get_args_type (int argc, char *argv[ ])
{
    if (!strcmp (argv[1], "backup")) {
        return OPENARCHIVE_BACKUP_ARGS;
    } else if (!strcmp (argv[1], "scan")) {
        return OPENARCHIVE_SCAN_ARGS;
    } else if (!strcmp (argv[1], "stub")) {
        return OPENARCHIVE_STUB_ARGS;
    }

    return OPENARCHIVE_UNDEF_ARGS;
}

int process_cmdline_params(openarchive_args_type_t type, int argc, char *argv[ ], void **argsptr)
{
    switch (type)
    {
        case OPENARCHIVE_SCAN_ARGS:
            return parse_scan_args (argc, argv, (scan_args_t *) *argsptr);

        case OPENARCHIVE_BACKUP_ARGS:
            return parse_backup_args (argc, argv, (backup_args_t *) *argsptr);

        case OPENARCHIVE_STUB_ARGS:
            return parse_stub_args (argc, argv, (stub_args_t *) *argsptr);
    
        default:
            return -1;
    } 
}

void* alloc_args (openarchive_args_type_t type)
{
    void * ptr = NULL;

    switch (type)
    {
    case OPENARCHIVE_SCAN_ARGS:
        ptr = malloc(sizeof(scan_args_t));
        return ptr;

    case OPENARCHIVE_BACKUP_ARGS:
        ptr = malloc(sizeof(backup_args_t));
        return ptr;

    case OPENARCHIVE_STUB_ARGS:
        ptr = malloc(sizeof(stub_args_t));
        return ptr;
    
    default:
        return NULL;
    } 
}

void free_args (void **ptr)
{
    if (*ptr) {
        free (*ptr);
        *ptr = NULL;  
    }
}

int parse_cmdline_params (int argc, char *argv [ ],
                          openarchive_args_type_t *args_type,
                          void **ptr)      
{
    *ptr = NULL;

    if (argc > 1) {

        *args_type = get_args_type (argc, argv);
        *ptr = alloc_args (*args_type);

        if (NULL==*ptr) {
            return -1;
        }

        return process_cmdline_params(*args_type, argc, argv, ptr);

    }

    return -1;
}
