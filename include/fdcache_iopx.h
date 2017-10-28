/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __FDCACHE_IOPX_H__
#define __FDCACHE_IOPX_H__

#include <map>
#include <vector>
#include <mutex>
#include <arch_core.h>
#include <arch_iopx.h>
#include <arch_engine.h>
#include <arch_mem.hpp>
#include <data_mgmt.h>
#include <cfgparams.h>

namespace openarchive
{
    namespace fdcache_iopx
    {
        const uint32_t num_file_alloc = 32;
        const uint32_t num_req_alloc  = 32;
        const uint32_t num_plain_buff = 32;
        const uint32_t ra_buff_size   = 0x400000; /* Buffer size of 4MB     */
        const uint64_t ra_bit_mask    = ~(0x3FFFFFUL);
        const uint32_t ra_bit_width   = 22;       /* 4MB == 22bits width    */

        struct map_entry
        {
            bool valid;
            bool busy;
            uint32_t index; 
        };

        struct ra_buf
        {
            bool valid;
            bool busy;
            bool pending;  
            bool rd_in_progress;
            uint32_t offset;
            uint32_t bytes;
            boost::shared_ptr <std::mutex> rd_mtx; 
            plbuff_ptr_t plbuff;
        };

        struct vec_entry
        {
            bool valid;
            bool busy;
            bool pending;  
            boost::shared_ptr <std::mutex> op_mtx; 
            file_ptr_t fp;
            struct ra_buf rabuff;
        };

        struct rqmap_entry
        {
            req_ptr_t gen_req;
            uint32_t slot;
            std::queue <req_ptr_t> parent_req;
            plbuff_ptr_t plbuff; 
        };

        class fdcache_iopx: public openarchive::arch_iopx::arch_iopx
        {

           /*
            * In the current implementation we will create a map of 
            * file descriptors which are read only. Support for writes
            * will be messy to handle since flush/close/truncate cannot be 
            * handled this way.
            */     

            std::map <std::string, map_entry> uuid_map;
            std::vector <vec_entry> fd_queue;
            src::severity_logger<int> log; 
            int32_t log_level;
            
            /*
             * read-write lock for safegaurading access to uuid_map and fd_queue
             */
            rwlock_t fdlock;

            std::map <std::string, rqmap_entry> request_map;

            /*
             * Spinlock for safegaurding access to request map
             */
            openarchive::arch_core::spinlock rqlock;

            /*
             * Total number of slots
             */
            uint32_t capacity; 

            /* 
             * Slot where the next element will be added   
             */
            uint32_t front; 

            /* 
             * Slot from which the next element will be removed.
             * This will come in to play once all the slots are 
             * filled in the circular buffer.
             */ 
            uint32_t rear; 
            
            openarchive::arch_mem::objpool <file_t, num_file_alloc> file_pool; 
            openarchive::arch_mem::objpool <req_t,  num_req_alloc>  req_pool; 
            openarchive::arch_mem::plbpool <ra_buff_size, 
                                            num_plain_buff> buff_pool;

            private:
            std::error_code get_fd (file_ptr_t);
            std::error_code search_fd (file_ptr_t);
            std::error_code alloc_slot (file_ptr_t, bool, uint32_t &, 
                                        uint32_t &, bool &);
            std::error_code init_slot (file_ptr_t, req_ptr_t, uint32_t,
                                       uint32_t,bool);
            plbuff_ptr_t getbuff (file_ptr_t, req_ptr_t, int32_t &);
            plbuff_ptr_t checkbuff (file_ptr_t, req_ptr_t, bool&);
            std::error_code readdata_async (std::string &, file_ptr_t, 
                                            req_ptr_t, req_ptr_t, int32_t, 
                                            bool &);
            std::error_code readdata_sync (std::string &, file_ptr_t, 
                                           req_ptr_t, req_ptr_t, int32_t,
                                           bool &);
            std::error_code readdata (file_ptr_t, req_ptr_t, int32_t, bool&);
            std::error_code processbuff (plbuff_ptr_t, file_ptr_t,
                                         req_ptr_t);

            inline void init_ra_buf (struct ra_buf &);
            inline void init_vec_entry (struct vec_entry &);
            inline void reserve_vec_entry (struct vec_entry &);
            inline void mark_vec_entry_ready (struct vec_entry &);
            inline void reserve_ra_buf (struct ra_buf &);
            inline void mark_ra_buf_ready (struct ra_buf &);
            inline bool is_validslot (req_ptr_t, uint32_t);
            std::error_code add_gen_req (std::string &, uint32_t slot,req_ptr_t,
                                         plbuff_ptr_t, req_ptr_t); 
            std::error_code add_parent_req (std::string &, req_ptr_t);
            std::error_code del_req (std::string &);

            public:
            fdcache_iopx (std::string, io_service_ptr_t, uint32_t);
            ~fdcache_iopx (void);
            virtual std::error_code open (file_ptr_t, req_ptr_t);
            virtual std::error_code pread (file_ptr_t, req_ptr_t);
            virtual std::error_code pread_cbk (file_ptr_t, req_ptr_t,
                                               std::error_code);
            virtual void profile (void);
            
        };
    }

    typedef openarchive::fdcache_iopx::fdcache_iopx fdcache_iopx_t;
    typedef boost::shared_ptr <fdcache_iopx_t> fdcache_iopx_ptr_t;
    typedef boost::shared_ptr <std::mutex> mutex_ptr_t;
    typedef openarchive::arch_core::lock_guard <mutex_ptr_t>  mutex_guard_t;
 
}
   
#endif
