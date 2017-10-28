/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __MEM_CACHE_H__
#define __MEM_CACHE_H__

#include <sys/uio.h>
#include <dlfcn.h>
#include <string>
#include <libmemcached/memcached.h>
#include <logger.h>
#include <arch_core.h>
#include <cfgparams.h>

typedef memcached_st * 
(*memcached_t) (const char *, size_t);

typedef void 
(*memcached_free_t) (memcached_st *);

typedef void 
(*memcached_result_free_t) (memcached_result_st *);

typedef memcached_result_st * 
(*memcached_result_create_t) (const memcached_st *, memcached_result_st *);

typedef const char * 
(*memcached_result_key_value_t) (const memcached_result_st *);

typedef size_t 
(*memcached_result_key_length_t) (const memcached_result_st *);

typedef const char *
(*memcached_result_value_t) (const memcached_result_st *);

typedef size_t 
(*memcached_result_length_t) (const memcached_result_st *);

typedef uint32_t 
(*memcached_result_flags_t) (const memcached_result_st *);

typedef memcached_return_t 
(*memcached_mget_t) (memcached_st *, const char * const *, const size_t *, 
                     size_t);

typedef memcached_result_st * 
(*memcached_fetch_result_t) (memcached_st *, memcached_result_st *,
                             memcached_return_t *);

typedef const char *
(*memcached_last_error_message_t) (memcached_st *);

typedef void
(*memcached_quit_t) (memcached_st *);

typedef memcached_return_t
(*memcached_set_t) (memcached_st *, const char *, size_t, const char *, size_t,
                    time_t, uint32_t); 

typedef memcached_return_t
(*memcached_delete_t) (memcached_st *, const char *, size_t, time_t);

typedef memcached_return_t
(*memcached_flush_t) (memcached_st *, time_t);
 
typedef memcached_return_t
(*memcached_flush_buffers_t) (memcached_st *);

namespace openarchive
{
    namespace mem_cache
    {
        struct kvpair
        {
            std::string key;
            struct iovec value;
            uint32_t ttl;
        };

        struct memcached_fops
        {
            memcached_t                     memcached;
            memcached_free_t                memcached_free;
            memcached_result_free_t         memcached_result_free;
            memcached_result_create_t       memcached_result_create;
            memcached_result_key_value_t    memcached_result_key_value;
            memcached_result_key_length_t   memcached_result_key_length;
            memcached_result_value_t        memcached_result_value;
            memcached_result_length_t       memcached_result_length;
            memcached_result_flags_t        memcached_result_flags;
            memcached_mget_t                memcached_mget;
            memcached_fetch_result_t        memcached_fetch_result;
            memcached_last_error_message_t  memcached_last_error_message;
            memcached_quit_t                memcached_quit;   
            memcached_set_t                 memcached_set;
            memcached_delete_t              memcached_delete; 
            memcached_flush_t               memcached_flush;
            memcached_flush_buffers_t       memcached_flush_buffers; 
        }; 
        typedef struct memcached_fops memcached_fops_t; 

        class lib_mcache_intfx
        {
            bool ready;
            void *handle;
            memcached_fops_t fops;
            src::severity_logger<int> log; 
            int32_t log_level;

            private:
            void init_fops (void);
            void *extract_symbol (std::string);   
            void extract_all_symbols (void);
            void dump_all_symbols (void);

            public:
            lib_mcache_intfx (void);
            ~lib_mcache_intfx (void);
            bool validate_all_symbols (void);
            memcached_fops_t * get_fops (void) { return &fops; }
        };
 
        boost::shared_ptr <lib_mcache_intfx> alloc_mcache_intfx (void);

        class mem_cache
        {
            bool ready;
            memcached_st *mcache;
            memcached_result_st mresult;
            src::severity_logger<int> log; 
            int32_t log_level;
            boost::shared_ptr <lib_mcache_intfx> lmcintfx; 
            memcached_fops_t *fops; 
 
            public:
            mem_cache (std::list<std::string> &);
            ~mem_cache (void);
            std::error_code get (struct kvpair &);
            std::error_code get (std::map<std::string, struct iovec> &);
            std::error_code set (struct kvpair &);
            std::error_code set (std::map<std::string, struct iovec> &, 
                                 uint32_t);
            std::error_code drop (struct kvpair &);
            std::error_code drop (std::list<std::string> &);
            std::error_code flush (void);   
            std::error_code sync (void);
            
        };
    }

    typedef struct openarchive::mem_cache::kvpair kvpair_t;
    typedef openarchive::mem_cache::mem_cache mem_cache_t;
    typedef boost::shared_ptr<mem_cache_t> mem_cache_ptr_t; 
}

#endif
