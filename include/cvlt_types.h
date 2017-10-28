/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __CVLT_TYPES_H__
#define __CVLT_TYPES_H__

#include <cvlt_fops.h>
#include <logger.h>

namespace openarchive
{
    namespace cvlt_iopx
    {
        enum cvlt_job_type
        {
            CVLT_BROWSE = 1,
            CVLT_FULL_BACKUP = 2, 
            CVLT_INCR_BACKUP = 3,
            CVLT_RESTORE = 4,
            CVLT_UNKNOWN_JOB = 127 
        };   

        struct cvmd
        {
            uuid_t uuid;
            uint64_t file_len;
        };


        class cvlt_stream
        {
            CVOB_hJob * pjob;
            CVOB_hStream * pstream;
            CVOB_hItem * item;
            std::string cvguid;  
            struct openarchive::cvlt_fops::libcvob_fops & fptrs;
            src::severity_logger<int> log;
            int32_t log_level;

            /*
             * Busy is used only in cases where pointer to a stream is saved
             * in TLS. Once the pointer is assigned in TLS busy would be set
             * to true.
             */ 
            bool busy;

            /* 
             * Active would be set to true if the stream is currently 
             * allocated for a file. At any given point of time a stream
             * can be allocated to a single file.
             */
            bool active; 

            private:
            void release_stream (void);

            public:
            cvlt_stream (CVOB_hJob *,
                         struct openarchive::cvlt_fops::libcvob_fops & ); 
            ~cvlt_stream (void);
            std::error_code alloc_item (std::string &, std::string &, size_t);
            std::error_code release_item (void);
            std::error_code send_metadata (uint32_t, char *, size_t);
            std::error_code send_data (char *, size_t);
            std::error_code receive_data (uint64_t, std::string &, char *,
                                          size_t, off_t, ssize_t &);
            struct cvlt_cbk_context * get_ctx (uint64_t, char *, size_t, off_t);

            CVOB_hItem * get_item (void)  { return item;    }
            CVOB_hStream * get (void)     { return pstream; }
            void set_busy (bool val)      { busy = val;     }
            bool get_busy (void)          { return busy;    } 
            void set_active (bool val)    { active = val;   }
            bool get_active (void)        { return active;  }   
            std::string & get_guid (void) { return cvguid;  }
        };

        class cvlt_stream_manager
        {
            CVOB_hJob * pjob;
            uint32_t num_streams;
            bool enable_reserve;
            bool valid;
            openarchive::arch_core::semaphore * sem; 
            openarchive::arch_core::mtqueue<cvlt_stream *> queue_streams;
            struct openarchive::cvlt_fops::libcvob_fops & fptrs;
            src::severity_logger<int> log;
            
            private: 
            cvlt_stream * get_free_stream (void);

            public:
            cvlt_stream_manager (CVOB_hJob *, uint32_t, 
                                 struct openarchive::cvlt_fops::libcvob_fops &,
                                 uint32_t);
            ~cvlt_stream_manager (void);
            cvlt_stream * alloc_stream (void);
            void release_stream (cvlt_stream *);
            void enable_stream_reservation (void) { enable_reserve = true; }
        }; 

        typedef void (*cvlt_app_cbk) (void *, int64_t, uint32_t);

    }

    typedef openarchive::cvlt_iopx::cvlt_stream cvlt_stream_t; 
}

#endif
