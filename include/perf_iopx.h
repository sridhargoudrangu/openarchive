/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __PERF_IOPX_H__
#define __PERF_IOPX_H__

#include <chrono>
#include <boost/format.hpp> 
#include <boost/make_shared.hpp>
#include <arch_iopx.h>
#include <iopx_reqpx.h>
#include <cfgparams.h>
#include <logger.h>

namespace openarchive
{
    namespace perf_iopx
    {

        struct req_info
        {
            std::chrono::high_resolution_clock::time_point start;
        };

        class perf_iopx: public openarchive::arch_iopx::arch_iopx
        {
            bool ready;
            src::severity_logger<int> log;
            int32_t log_level;
            std::atomic<uint64_t> seq;
            openarchive::arch_core::mtmap<uint64_t, req_info> request_map;

            /*
             * Number of times the FOP has been invoked.
             */  
            std::atomic <uint64_t> open_count;
            std::atomic <uint64_t> close_count;
            std::atomic <uint64_t> pread_count;
            std::atomic <uint64_t> pwrite_count; 
            std::atomic <uint64_t> fstat_count; 
            std::atomic <uint64_t> stat_count; 
            std::atomic <uint64_t> ftruncate_count; 
            std::atomic <uint64_t> truncate_count; 
            std::atomic <uint64_t> fsetxattr_count; 
            std::atomic <uint64_t> setxattr_count; 
            std::atomic <uint64_t> fgetxattr_count; 
            std::atomic <uint64_t> getxattr_count; 
            std::atomic <uint64_t> fremovexattr_count; 
            std::atomic <uint64_t> removexattr_count; 
            std::atomic <uint64_t> lseek_count; 
            std::atomic <uint64_t> getuuid_count; 
            std::atomic <uint64_t> gethosts_count; 
            std::atomic <uint64_t> mkdir_count; 
            std::atomic <uint64_t> resolve_count; 
            std::atomic <uint64_t> dup_count; 
            std::atomic <uint64_t> bytes_read;
            std::atomic <uint64_t> bytes_written;

            /*
             * Time taken for the FOP.
             */  
            std::atomic <uint64_t> open_time;
            std::atomic <uint64_t> close_time;
            std::atomic <uint64_t> pread_time;
            std::atomic <uint64_t> pwrite_time; 
            std::atomic <uint64_t> fstat_time; 
            std::atomic <uint64_t> stat_time; 
            std::atomic <uint64_t> ftruncate_time; 
            std::atomic <uint64_t> truncate_time; 
            std::atomic <uint64_t> fsetxattr_time; 
            std::atomic <uint64_t> setxattr_time; 
            std::atomic <uint64_t> fgetxattr_time; 
            std::atomic <uint64_t> getxattr_time; 
            std::atomic <uint64_t> fremovexattr_time; 
            std::atomic <uint64_t> removexattr_time; 
            std::atomic <uint64_t> lseek_time; 
            std::atomic <uint64_t> getuuid_time; 
            std::atomic <uint64_t> gethosts_time; 
            std::atomic <uint64_t> mkdir_time; 
            std::atomic <uint64_t> resolve_time; 
            std::atomic <uint64_t> dup_time; 

            private:
            void log_avg_time (std::string, std::atomic <uint64_t> &, 
                               std::atomic <uint64_t> &);
            void log_throughput (std::string, std::atomic <uint64_t> &,
                                 std::atomic <uint64_t> &);
            void work_done_cbk (file_ptr_t, req_ptr_t, std::error_code);

            public:
            perf_iopx (std::string, io_service_ptr_t);
            ~perf_iopx (void);

            /*
             * File operations
             */
            virtual std::error_code open              (file_ptr_t, req_ptr_t);

            virtual std::error_code close             (file_ptr_t, req_ptr_t);

            virtual std::error_code close             (file_t &);

            virtual std::error_code pread             (file_ptr_t, req_ptr_t);

            virtual std::error_code pread_async       (file_ptr_t, req_ptr_t);

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

            virtual std::error_code pread_cbk         (file_ptr_t, req_ptr_t,
                                                       std::error_code);
            /*
             * File system operations
             */
            virtual std::error_code mkdir             (file_ptr_t, req_ptr_t);

            virtual std::error_code resolve           (file_ptr_t, req_ptr_t);

            virtual std::error_code dup               (file_ptr_t, file_ptr_t);

            virtual void profile (void);

        };
    }

    typedef openarchive::perf_iopx::perf_iopx perf_iopx_t;
    typedef boost::shared_ptr <perf_iopx_t> perf_iopx_ptr_t;

}
 
#endif
