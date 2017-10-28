/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __CVLT_IOPX_H__
#define __CVLT_IOPX_H__

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif
#include <endian.h>
#include <climits>
#include <arch_core.h>
#include <arch_mem.hpp>
#include <arch_iopx.h>
#include <iopx_reqpx.h>
#include <cfgparams.h>
#include <logger.h>
#include <cvlt_fops.h>
#include <cvlt_types.h>

namespace openarchive
{
    namespace cvlt_iopx
    {
        class cvlt_iopx;

        class cvlt_cbk_context
        {
            public: 
            uint64_t seq;
            std::string path;
            std::string cvguid;
            char * buffptr;
            size_t bufflen;
            size_t buff_offset; 
            uuid_t uuid;
            size_t file_offset;
            size_t file_size;
            int64_t ret;
            int32_t err;
            atomic_vol_bool cbk_done;  
            cvlt_iopx * iopx;
            cvlt_stream * stream;
            bool async_io;      
            openarchive::arch_core::semaphore sem;
        };

        const int num_cvlt_ctx_alloc = 32;

        class cvlt_iopx: public openarchive::arch_iopx::arch_iopx
        {
            openarchive::cvlt_fops::cvlt_fops fops;
            struct openarchive::cvlt_fops::libcvob_fops & fptrs;
            src::severity_logger<int> log;
            int32_t log_level;
            std::string args;
            bool ready;
            int32_t comcell_id;
            std::string proxy_name; 
            int32_t proxy_port;
            int32_t app_type;
            std::string client_name;
            std::string instance_name;
            std::string backupset_name;
            std::string subclient_name;
            CVOB_session_t *cvobsession;
            int32_t client_id;
            int32_t app_id;
            uint32_t job_id;
            std::string job_token;
            cvlt_job_type job_type;   
            CVOB_hJob * cvobjob;
            int32_t num_streams; 
            cvlt_stream_manager * cvstreammgr;
            std::atomic<uint64_t> seq;
            openarchive::arch_mem::objpool <cvlt_cbk_context,
                                            num_cvlt_ctx_alloc> ctx_pool;
            openarchive::arch_core::mtmap<uint64_t, req_ptr_t> request_map;
            uint32_t num_worker_threads;

            public:
            cvlt_iopx (std::string, io_service_ptr_t, std::string, uint32_t);
            ~cvlt_iopx (void);
            void run_cbk (cvlt_cbk_context *);

            /*
             * File operations
             */
            virtual std::error_code open              (file_ptr_t, req_ptr_t);

            virtual std::error_code close             (file_ptr_t, req_ptr_t);

            virtual std::error_code close             (file_t &);

            virtual std::error_code pread             (file_ptr_t, req_ptr_t);

            virtual std::error_code pwrite            (file_ptr_t, req_ptr_t);

            virtual std::error_code fstat             (file_ptr_t, req_ptr_t);

            virtual std::error_code stat              (file_ptr_t, req_ptr_t);

            virtual std::error_code ftruncate         (file_ptr_t, req_ptr_t);

            virtual std::error_code truncate          (file_ptr_t, req_ptr_t);

            virtual std::error_code fsetxattr         (file_ptr_t, req_ptr_t);

            virtual std::error_code setxattr          (file_ptr_t, req_ptr_t);

            virtual std::error_code fgetxattr         (file_ptr_t, req_ptr_t);

            virtual std::error_code getxattr          (file_ptr_t, req_ptr_t);

            virtual std::error_code fremovexattr      (file_ptr_t, req_ptr_t);

            virtual std::error_code removexattr       (file_ptr_t, req_ptr_t);

            virtual std::error_code lseek             (file_ptr_t, req_ptr_t);

            virtual std::error_code getuuid           (file_ptr_t, req_ptr_t);

            virtual std::error_code gethosts          (file_ptr_t, req_ptr_t);

            /*
             * File system operations
             */
            virtual std::error_code mkdir             (file_ptr_t, req_ptr_t);

            virtual std::error_code resolve           (file_ptr_t, req_ptr_t);

            virtual std::error_code dup               (file_ptr_t, file_ptr_t);

            virtual std::error_code scan              (file_ptr_t, req_ptr_t);

            virtual void profile (void);

            private:
            void set_cvlt_logging (void);
            std::error_code parse_args (void); 
            std::error_code extract_ids (void);
            std::error_code releasesession (void);
            std::error_code allocsession (void);
            bool validate_params (void);
            std::error_code startjob (void);
            std::error_code stopjob (bool);
            std::error_code release_resources (file_info_t &);
            cvlt_stream * alloc_stream (std::string &);
            void release_stream (cvlt_stream *);
            void cvuuid_to_v4uuid(std::string &, std::string &);
            void v4uuid_to_cvuuid (uuid_t, std::string &);
            cvlt_cbk_context * alloc_ctx (uint64_t, char *, size_t, off_t,
                                          bool, cvlt_stream *);
            void release_ctx (cvlt_cbk_context *); 
            std::error_code receive_data (uint64_t, std::string &, char *,
                                          size_t, off_t, bool, int64_t &);
        };

        ssize_t cvmdserialize (struct cvmd &, char *, size_t);
        ssize_t cvmdunserialize (struct cvmd &, char *, size_t);
        void log_error (CVOB_hError *, src::severity_logger<int>&, 
                        openarchive::cvlt_fops::libcvob_fops &,std::string);
        /*
         * Callback handlers for data management operations.
         */ 

        int32_t cvheadercbk (void *, const char *, const char *, CVOB_ItemType,
                             CVOB_BackupItemAttributes_t *, int64_t); 
        int32_t cvmetadatacbk (void *, CVOB_MetadataID, char *, size_t); 
        int32_t cvdatacbk (void *, char *, size_t); 
        int32_t cveof (void *, int32_t);  
        void cvcbk (cvlt_cbk_context *);
    }

    typedef openarchive::cvlt_iopx::cvlt_stream * cvlt_stream_ptr_t;
    typedef struct openarchive::cvlt_fops::libcvob_fops libcvob_fops_t; 
    typedef openarchive::cvlt_iopx::cvlt_iopx cvlt_iopx_t;


}
 
#endif
