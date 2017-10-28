/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __CVLT_FOPS_H__
#define __CVLT_FOPS_H__

#include <dlfcn.h>
#include <linux/types.h>
#include <string>
#include <CVOpenBackup.h>
#include <cfgparams.h>
#include <logger.h>

namespace openarchive
{
    namespace cvlt_fops
    {
        typedef int32_t (*cvob_init_t)             (const CertificateInfo_t*, 
                                                    const char*, int16_t, int32_t,
                                                    int32_t, int32_t, int32_t, 
                                                    int32_t, const char*,
                                                    reportError_f, CVOB_session_t**,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_init2_t)            (const CertificateInfo_t*,
                                                    const char *, int16_t,
                                                    const char *, int32_t,
                                                    reportError_f, 
                                                    ClientInfo_t *,
                                                    CVOB_session_t **,
                                                    const char *, const char *,
                                                    const char *, const char *,
                                                    const char *, int32_t,
                                                    int32_t, const char *,
                                                    CVOB_hError **);   

        typedef int32_t (*cvob_deinit_t)           (CVOB_session_t*, int16_t, 
                                                    CVOB_hError**);

        typedef int32_t (*cvob_startjob_t)         (CVOB_session_t*, int16_t, 
                                                    int32_t, CVOB_JobType, 
                                                    const char*, CVOB_hJob**, 
                                                    uint32_t*,  CVOB_hError**);

        typedef int32_t (*cvob_endjob_t)           (CVOB_hJob*, CVOB_hError**,
                                                    int32_t);

        typedef int32_t (*cvob_startstream_t)      (CVOB_hJob*, CVOB_hStream**,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_endstream_t)        (CVOB_hStream*, CVOB_hError**);

        typedef int32_t (*cvob_senditem_t)         (CVOB_hStream*, void*, 
                                                    const char*, CVOB_ItemType,
                                                    const char*, char*, int64_t,
                                                    CVOB_BackupItemAttributes_t*,
                                                    CVOB_BackupCallbackInfo_t*,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_senditem_begin_t)   (CVOB_hStream*, CVOB_hItem**,
                                                    const char*, CVOB_ItemType,
                                                    const char*, char*, int64_t,
                                                    CVOB_BackupItemAttributes_t*,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_sendmetadata_t)     (CVOB_hStream*, CVOB_hItem*,
                                                    CVOB_MetadataID, const char*,
                                                    size_t, CVOB_hError**);

        typedef int32_t (*cvob_senddata_t)         (CVOB_hStream*, CVOB_hItem*,
                                                    const char*, int64_t,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_sendend_t)          (CVOB_hStream*, CVOB_hItem*,
                                                    CVOB_ItemStatus, const char*,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_commitstream_t)     (CVOB_hStream*, void*,
                                                    backupItemInfo_f,   
                                                    CVOB_hError**);

        typedef int32_t (*cvob_getbackuplist_t)    (CVOB_session_t*, void*,
                                                    CVOB_BackupItemsQuery_t*,
                                                    backupItemInfo_f,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_mark_deleted_t)     (CVOB_session_t*, const char*,
                                                    const char*, CVOB_hError**);

        typedef int32_t (*cvob_send_contentlist_t) (CVOB_session_t*,
                                                    CVOB_hJob*, void*, int16_t,
                                                    contentItem_f,
                                                    backupItemInfo_f,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_restore_object_t)   (CVOB_hJob*, void*, 
                                                    const char*, uint64_t, 
                                                    uint64_t,
                                                    CVOB_RestoreCallbackInfo_t*,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_get_commit_items_t) (CVOB_hStream*, void*,
                                                    backupItemInfo_f,
                                                    CVOB_hError**);

        typedef int32_t (*cvob_get_error_t)        (CVOB_hError*, int32_t*,
                                                    char*, int32_t);

        typedef void    (*cvob_freeerror_t)        (CVOB_hError*);

        typedef int32_t (*cvob_enable_logging_t)   (const char *, const char *,
                                                    int32_t);

        struct libcvob_fops
        {
            cvob_init_t init;
            cvob_init2_t init2; 
            cvob_deinit_t deinit;
            cvob_startjob_t start_job;
            cvob_endjob_t end_job;
            cvob_startstream_t start_stream;
            cvob_endstream_t end_stream;
            cvob_senditem_t send_item;
            cvob_senditem_begin_t send_item_begin;
            cvob_sendmetadata_t send_metadata;
            cvob_senddata_t send_data; 
            cvob_sendend_t send_end;
            cvob_commitstream_t commit_stream;
            cvob_getbackuplist_t get_backuplist;
            cvob_mark_deleted_t mark_deleted;
            cvob_send_contentlist_t send_contentlist;
            cvob_restore_object_t restore_object;
            cvob_get_commit_items_t get_commit_items;
            cvob_get_error_t get_error;
            cvob_freeerror_t free_error;
            cvob_enable_logging_t enable_logging;
        };

        class cvlt_fops
        {
            bool ready;
            void *handle;
            std::string lib;
            struct libcvob_fops fops;
            src::severity_logger<int> log; 
            int32_t log_level;
        
            private:
            void   init_fops (void);
            void * extract_symbol (std::string);   
            void   extract_all_symbols (void);
            void   dump_all_symbols (void);
            bool   validate_fops (void);

            public: 
            cvlt_fops (std::string);
            ~cvlt_fops (void);
            struct libcvob_fops & get_fops (void); 
            bool is_valid (void) { return ready; } 
        }; 
    }
}
  
#endif
