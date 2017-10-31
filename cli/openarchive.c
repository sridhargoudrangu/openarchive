/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <errno.h>
#include <uuid/uuid.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <pthread.h>
#include <semaphore.h>
#include <archivestore.h>
#include <cmdline.h>

#define LIBARCHIVE_SO "libopenarchive.so"
#define GLUSTERFS_GFID "glusterfs.gfid.string"

static int64_t archret = 0;
static int32_t archerr = 0;

static void wait_for_completion (archstore_desc_t *store, 
                                 app_callback_info_t *cbk_info,
                                 void *cookie, int64_t ret, int errcode)
{
    sem_t *semptr=NULL;

    archret = ret;
    archerr = errcode;

    semptr = (sem_t *)cookie;
    if (semptr) {
        sem_post(semptr);
    } 
    return; 
}

static int32_t do_scan (scan_args_t *scanargs, archstore_desc_t *store_desc,
                        archstore_methods_t *arch_methods)
{
    archstore_info_t storeinfo;
    archstore_scan_type_t scan_type;

    if (OPENARCHIVE_FULL_SCAN==scanargs->type) {
        scan_type = FULL;
    } else {
        scan_type = INCREMENTAL; 
    }

    /*
     * Perform the scan operation.
     */
    storeinfo.id = scanargs->src_store;
    storeinfo.idlen = strlen (scanargs->src_store);
    storeinfo.prod = scanargs->src_product;
    storeinfo.prodlen = strlen (scanargs->src_product);

    if (arch_methods->scan (store_desc, &storeinfo, scan_type, 
                            scanargs->outp_loc, &archerr)) { 

        fprintf(stderr, 
                "Failed to determine the files to be backed up for "
                "product %s store %s\n", storeinfo.prod, storeinfo.id);
        return (-1); 

    }

    return (0);
}

static int32_t do_backup (backup_args_t *bckargs, archstore_desc_t *store_desc,
                          archstore_methods_t *arch_methods)
{
    sem_t completion;
    archstore_info_t src_storeinfo;
    archstore_info_t dest_storeinfo; 
    archstore_fileinfo_t fileinfo;
    archstore_fileinfo_t failedfilesinfo;

    sem_init(&completion, 0, 0);

    src_storeinfo.id = bckargs->src_store;
    src_storeinfo.idlen = strlen (bckargs->src_store);
    src_storeinfo.prod = bckargs->src_product;
    src_storeinfo.prodlen = strlen (bckargs->src_product);

    fileinfo.path = bckargs->inp_loc;
    fileinfo.pathlength = strlen(bckargs->inp_loc);

    failedfilesinfo.path = bckargs->outp_loc;
    failedfilesinfo.pathlength = strlen (bckargs->outp_loc);

    dest_storeinfo.id = bckargs->dest_store;
    dest_storeinfo.idlen = strlen (bckargs->dest_store);
    dest_storeinfo.prod = bckargs->dest_product;
    dest_storeinfo.prodlen = strlen (bckargs->dest_product);

    if (arch_methods->backup (store_desc, &src_storeinfo, &fileinfo, 
                              &dest_storeinfo, &failedfilesinfo, &archerr, 
                              wait_for_completion, &completion)) { 

        fprintf(stderr, "Failed to backup files from  %s \n", bckargs->inp_loc);
        return (-1); 

    }

    sem_wait(&completion); 
    sem_destroy(&completion);

    return (0);
}

static int32_t do_stub (stub_args_t *stubargs, archstore_desc_t *store_desc, 
                        archstore_methods_t *arch_methods)
{
    sem_t completion;
    archstore_info_t src_storeinfo;
    archstore_info_t dest_storeinfo; 
    archstore_fileinfo_t fileinfo;
    archstore_fileinfo_t failedfilesinfo;

    sem_init(&completion, 0, 0);

    src_storeinfo.id = stubargs->src_store;
    src_storeinfo.idlen = strlen (stubargs->src_store);
    src_storeinfo.prod = stubargs->src_product;
    src_storeinfo.prodlen = strlen (stubargs->src_product);

    fileinfo.path = stubargs->inp_loc;
    fileinfo.pathlength = strlen(stubargs->inp_loc);

    failedfilesinfo.path = stubargs->outp_loc;
    failedfilesinfo.pathlength = strlen (stubargs->outp_loc);

    dest_storeinfo.id = stubargs->dest_store;
    dest_storeinfo.idlen = strlen (stubargs->dest_store);
    dest_storeinfo.prod = stubargs->dest_product;
    dest_storeinfo.prodlen = strlen (stubargs->dest_product);

    if (arch_methods->archive (store_desc, &src_storeinfo, &fileinfo, 
                               &dest_storeinfo, &failedfilesinfo, &archerr, 
                               wait_for_completion, &completion)) { 

        fprintf(stderr, "Failed to stub files from  %s \n", stubargs->inp_loc);
        return (-1); 

    }

    sem_wait(&completion); 
    sem_destroy(&completion);

    return (0);
}

static int32_t do_task (void *ptr, openarchive_args_type_t args_type, 
                        archstore_desc_t *store_desc, 
                        archstore_methods_t *arch_methods)
{
    switch (args_type)
    {
        case OPENARCHIVE_SCAN_ARGS:
            return do_scan ((scan_args_t *) ptr, store_desc, arch_methods);

        case OPENARCHIVE_BACKUP_ARGS:
            return do_backup ((backup_args_t *) ptr, store_desc, arch_methods);

        case OPENARCHIVE_STUB_ARGS:
            return do_stub ((stub_args_t *) ptr, store_desc, arch_methods); 
    
        default:
            return -1;
    } 
} 

int main(int argc, char *argv[ ])
{
    int ret;
    char store_up = 0;
    void *handle = NULL;
    archstore_errno_t archerr;
    archstore_desc_t store_desc;
    archstore_methods_t arch_methods;
    get_archstore_methods_t get_archstore_methods;
    char *app = strdupa (argv[0]);
    openarchive_args_type_t args_type;
    void *args_ptr=NULL;
   
    ret = parse_cmdline_params (argc, argv, &args_type, &args_ptr);
    if (ret < 0) {
        usage (argv[0]);
        goto error;
    } 

    handle = dlopen (LIBARCHIVE_SO, RTLD_NOW);
    if (!handle) {
        fprintf(stderr, "failed to open %s errno:%d desc:%s \n",
                LIBARCHIVE_SO, errno, strerror(errno));
        goto error;
    }
        
    dlerror();    /* Clear any existing error */

    get_archstore_methods = (get_archstore_methods_t) dlsym (handle, "get_archstore_methods");
    if (!get_archstore_methods) {
        fprintf(stderr, " Error extracting get_archstore_methods() \n");
        goto error;
    }

    ret = get_archstore_methods (&arch_methods);
    if (ret) {
        fprintf(stderr, "Failed to extract methods in get_archstore_methods\n");
        goto error;
    }

    /*
     * Initialize the archive store.
     */
    if (arch_methods.init(&store_desc, &archerr, basename(app))) {
        fprintf(stderr, "Failed to initialise archive store");
        goto error;
    }

    store_up=1;

    /*
     * Invoke the data mgmt operation that was selected
     */
    ret = do_task(args_ptr, args_type, &store_desc, &arch_methods); 

    if (ret < 0 || archret < 0) { 
        goto error;
    }

    /*
     * Clean up the archive store.
     */
    if (store_up) {
        if (arch_methods.fini(&store_desc, &archerr)) {
            fprintf(stderr, "Failed to cleanup acrhive store");
            goto error;
        }
    } 

    if (handle) {
        dlclose (handle);
    }

    exit (EXIT_SUCCESS);

error:

    if (store_up) {
        if (arch_methods.fini(&store_desc, &archerr)) {
            fprintf(stderr, "Failed to cleanup archive store");
        }
    } 

    if (handle) {
        dlclose(handle);
    }

    exit (EXIT_FAILURE);
}
