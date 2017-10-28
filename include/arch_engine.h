/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_ENGINE_H__
#define __ARCH_ENGINE_H__

#include <pthread.h>
#include <arch_iopx.h>
#include <logger.h>

namespace openarchive
{
    namespace arch_engine
    {
        struct iopx_tree_cfg
        {
            std::string product;
            std::string store;
            std::string desc;
            bool enable_fast_iosvc;
            bool enable_meta_cache;
            uint32_t meta_cache_ttl;
            bool enable_fd_cache;
            uint32_t fd_cache_size;
        };
 
        class arch_engine
        {
            src::severity_logger<int> log; 
            bool ready;
            bool enable_fast;
            bool enable_slow;
            int32_t log_level;

            io_service_ptr_t fast_iosvc;  /* High priority ioservice          */
            work_ptr_t fast_worker;       /* High priority worker             */
            boost::thread_group fast_threads; /* High prioty threads          */
            uint32_t nfastthreads;        /* Number of high priority threads  */

            io_service_ptr_t slow_iosvc;  /* Low priority ioservice           */
            work_ptr_t slow_worker;       /* Low priority worker              */
            boost::thread_group slow_threads; /* Low prioty threads           */
            uint32_t nslowthreads;        /* Number of low priority threads   */

            private:
            std::error_code alloc_engine_resources (void);
            std::error_code release_engine_resources(void);
            io_service_ptr_t alloc_ioservice (std::string);
            work_ptr_t alloc_worker (io_service_ptr_t, std::string);
            uint32_t create_threads (io_service_ptr_t, boost::thread_group &,
                                     uint32_t);
            iopx_ptr_t mkgltree (struct iopx_tree_cfg &);
            iopx_ptr_t mkcvlttree (struct iopx_tree_cfg &);
            void map_cvlt_store_id (std::string &, std::string &);

            public:
            arch_engine (bool, bool);
            void stop (void);
            ~arch_engine (void);

            /* 
             * Create tree of iopx objects one tree has to be created for
             * each source and sink.
             */
            iopx_ptr_t mktree (struct iopx_tree_cfg &);

            io_service_ptr_t get_ioservice (bool fast) 
            { 
                return (fast? fast_iosvc:slow_iosvc);
            }

            uint32_t get_num_fast_threads (void) { return nfastthreads; }
            uint32_t get_num_slow_threads (void) { return nslowthreads; }
            void map_store_id (std::string &, std::string &, std::string &);
           

        };

        void worker_thread(io_service_ptr_t, uint32_t);
        boost::shared_ptr<arch_engine> alloc_engine (void); 

    } /* namespace arch_engine */

    typedef openarchive::arch_engine::arch_engine           arch_engine_t;  
    typedef boost::shared_ptr<arch_engine_t>                arch_engine_ptr_t;
    typedef struct openarchive::arch_engine::iopx_tree_cfg  iopx_tree_cfg_t;

} /*  namespace openarchive */    
#endif
