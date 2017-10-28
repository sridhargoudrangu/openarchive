/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_IOPX_H__
#define __ARCH_IOPX_H__

#include <thread>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <arch_file.h>
#include <arch_core.h>
#include <iopx_reqpx.h>

namespace openarchive
{
    namespace arch_iopx
    {
        class arch_iopx
        {
            std::string name; 
            io_service_ptr_t iosvc;
            boost::shared_ptr<arch_iopx> parent;
            std::list<boost::shared_ptr<arch_iopx>> children;
            atomic_vol_uint64_t refcount; 

            protected:
            std::error_code fop_default (file_ptr_t, req_ptr_t);
            std::error_code close_default (file_t &);
            std::error_code dup_default (file_ptr_t, file_ptr_t);
            std::error_code fop_cbk_default (file_ptr_t, req_ptr_t, 
                                             std::error_code);
            std::error_code run_fop (file_ptr_t, req_ptr_t);
            bool schedule_fop (req_ptr_t);
            std::error_code parent_cbk (file_ptr_t, req_ptr_t, std::error_code);
            boost::shared_ptr<arch_iopx> get_first_child (void)  
            { 
                return children.front ();
            }

            public:
            arch_iopx (std::string, io_service_ptr_t);
            virtual ~arch_iopx (void);
            void put (void);
            void get (void);
            uint64_t get_refcount (void);
            void reset_links (void);

            std::string get_name (void) { return name; }

            void add_child (boost::shared_ptr <arch_iopx> child) 
            {
                children.push_back (child);
            }
  
            void set_parent (boost::shared_ptr <arch_iopx> par)
            {
                parent = par;  
            }   

            boost::shared_ptr<arch_iopx> get_parent (void)
            { 
                return parent;
            }

            /* 
             * For each supported file operation fop the schdule_fop method will
             * determine whether the method needs to be executed as the next 
             * function in the stack or should be schduled to run in the context
             * of a different thread. These will be low priority threads. 
             * This can be set to true by iopx which are computation intensive
             * like compress/dedupe so that these activities can be performed 
             * in a different thread context while returning back immediately 
             * to the parent iopx.
             */
            virtual bool schedule_open                (void) { return false; }
            virtual bool schedule_close               (void) { return false; }
            virtual bool schedule_pread               (void) { return false; }
            virtual bool schedule_pwrite              (void) { return false; }
            virtual bool schedule_fstat               (void) { return false; }
            virtual bool schedule_stat                (void) { return false; }
            virtual bool schedule_ftruncate           (void) { return false; }
            virtual bool schedule_truncate            (void) { return false; }
            virtual bool schedule_fsetxattr           (void) { return false; }
            virtual bool schedule_setxattr            (void) { return false; }
            virtual bool schedule_fgetxattr           (void) { return false; }
            virtual bool schedule_getxattr            (void) { return false; }
            virtual bool schedule_fremovexattr        (void) { return false; }
            virtual bool schedule_removexattr         (void) { return false; }
            virtual bool schedule_lseek               (void) { return false; }
            virtual bool schedule_mkdir               (void) { return false; }
            virtual bool schedule_getuuid             (void) { return false; }
            virtual bool schedule_resolve             (void) { return false; }  
            virtual bool schedule_gethosts            (void) { return false; } 
            virtual bool schedule_scan                (void) { return false; } 

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

            /*
             * Profiling operations
             */ 

            virtual void profile (void);

            /*
             * File operation callbacks
             */

            virtual std::error_code open_cbk          (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code close_cbk         (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code pread_cbk         (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code pwrite_cbk        (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code fstat_cbk         (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code stat_cbk          (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code ftruncate_cbk     (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code truncate_cbk      (file_ptr_t, req_ptr_t,
                                                       std::error_code); 

            virtual std::error_code fsetxattr_cbk     (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code setxattr_cbk      (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code fgetxattr_cbk     (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code getxattr_cbk      (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code fremovexattr_cbk  (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code removexattr_cbk   (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code lseek_cbk         (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code getuuid_cbk       (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code gethosts_cbk      (file_ptr_t, req_ptr_t,
                                                       std::error_code);
            /*
             * File system operation callbacks
             */

            virtual std::error_code mkdir_cbk         (file_ptr_t, req_ptr_t,
                                                       std::error_code);

            virtual std::error_code resolve_cbk       (file_ptr_t, req_ptr_t,
                                                       std::error_code); 

            virtual std::error_code scan_cbk          (file_ptr_t, req_ptr_t,
                                                       std::error_code);

        }; 
    } /* End of namespace arch_iopx */

    typedef boost::shared_ptr<openarchive::arch_iopx::arch_iopx>   iopx_ptr_t;
} /* End of namespace arch_iopx */
#endif /* End of __ARCH_IOPX_H__ */
