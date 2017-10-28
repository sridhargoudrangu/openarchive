/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <mem_cache.h>

namespace openarchive
{
    namespace mem_cache
    {
        static std::string LIBMEMCACHEDB("libmemcached.so");
        static openarchive::arch_core::spinlock memcache_lock;
        static std::string einternal("internal error");  

        lib_mcache_intfx::lib_mcache_intfx(void): ready(false),handle(NULL)
        {
            log_level = openarchive::cfgparams::get_log_level();
            init_fops(); 

            /*
             * Open libmemcachedb dll
             */
            dlerror ();
            handle = dlopen (LIBMEMCACHEDB.c_str(), RTLD_NOW);
            if (!handle) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open  " << LIBMEMCACHEDB 
                               << " error : " << dlerror (); 
                return;
            }

            /*
             * Extract the required symbols from dll
             */
            extract_all_symbols();
            dump_all_symbols();

            if (!validate_all_symbols ()) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcachedb entrypoints validation failed";
                return;
            } 

            ready = true;
            return; 
            
        }

        lib_mcache_intfx::~lib_mcache_intfx (void)
        {
            if (handle) {
                dlclose (handle);
                handle = NULL; 
            }

            return; 
        } 

        void lib_mcache_intfx::init_fops (void)
        {
            fops.memcached = NULL;
            fops.memcached_free = NULL;
            fops.memcached_result_free = NULL;
            fops.memcached_result_create = NULL;
            fops.memcached_result_key_value = NULL;
            fops.memcached_result_key_length = NULL;
            fops.memcached_result_value = NULL;
            fops.memcached_result_length = NULL;
            fops.memcached_result_flags = NULL;
            fops.memcached_mget = NULL;
            fops.memcached_fetch_result = NULL;
            fops.memcached_last_error_message = NULL;
            fops.memcached_quit = NULL;
            fops.memcached_set = NULL;
            fops.memcached_delete = NULL;
            fops.memcached_flush = NULL; 
            fops.memcached_flush_buffers = NULL; 

            return;
        }  

        void* lib_mcache_intfx::extract_symbol (std::string name)
        {
            dlerror(); /* Reset errors */
            
            void *fptr = dlsym (handle, name.c_str());
            const char *dlsymerr = dlerror();
            if (dlsymerr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find " << name << " in " 
                               << LIBMEMCACHEDB << " error desc: " << dlsymerr;
                return NULL;
            }    

            return fptr; 

        }

        void lib_mcache_intfx::dump_all_symbols (void)
        {
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Dumping all the symbols from " 
                               <<LIBMEMCACHEDB ;
            } 
                
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached:                     0x" 
                               << std::hex
                               << (uint64_t) fops.memcached; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_free:                0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_free; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_result_free:         0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_result_free; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_result_create:       0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_result_create; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_result_key_value:    0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_result_key_value; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_result_key_length:   0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_result_key_length; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_result_value:        0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_result_value; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_result_length:       0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_result_length; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_last_error_message:  0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_last_error_message;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_quit:                0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_quit;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_set:                 0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_set;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_delete:              0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_delete;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_flush:               0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_flush;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached_flush_buffers:       0x" 
                               << std::hex
                               << (uint64_t) fops.memcached_flush_buffers;
            } 

            return;
        }

        void lib_mcache_intfx::extract_all_symbols (void)
        {
            void *fptr;

            fops.memcached_fetch_result = NULL;

            fptr = extract_symbol ("memcached");
            fops.memcached = (memcached_t) fptr;

            fptr = extract_symbol ("memcached_free");
            fops.memcached_free = (memcached_free_t) fptr;

            fptr = extract_symbol ("memcached_result_free");
            fops.memcached_result_free = (memcached_result_free_t) fptr;

            fptr = extract_symbol ("memcached_result_create");
            fops.memcached_result_create = (memcached_result_create_t) fptr;

            fptr = extract_symbol ("memcached_result_key_value");
            fops.memcached_result_key_value = (memcached_result_key_value_t) fptr; 

            fptr = extract_symbol ("memcached_result_key_length");
            fops.memcached_result_key_length = (memcached_result_key_length_t) fptr; 

            fptr = extract_symbol ("memcached_result_value");
            fops.memcached_result_value = (memcached_result_value_t) fptr; 

            fptr = extract_symbol ("memcached_result_length");
            fops.memcached_result_length = (memcached_result_length_t) fptr; 

            fptr = extract_symbol ("memcached_result_flags");
            fops.memcached_result_flags = (memcached_result_flags_t) fptr; 

            fptr = extract_symbol ("memcached_mget");
            fops.memcached_mget = (memcached_mget_t) fptr; 

            fptr = extract_symbol ("memcached_fetch_result");
            fops.memcached_fetch_result = (memcached_fetch_result_t) fptr;

            fptr = extract_symbol ("memcached_last_error_message");
            fops.memcached_last_error_message = (memcached_last_error_message_t) fptr;

            fptr = extract_symbol ("memcached_quit");
            fops.memcached_quit = (memcached_quit_t) fptr;

            fptr = extract_symbol ("memcached_set");
            fops.memcached_set = (memcached_set_t) fptr; 

            fptr = extract_symbol ("memcached_delete");
            fops.memcached_delete = (memcached_delete_t) fptr; 

            fptr = extract_symbol ("memcached_flush");
            fops.memcached_flush = (memcached_flush_t) fptr; 

            fptr = extract_symbol ("memcached_flush_buffers");
            fops.memcached_flush_buffers = (memcached_flush_buffers_t) fptr; 

            return; 
        }

        bool lib_mcache_intfx::validate_all_symbols (void)
        {
            if (fops.memcached                    && 
                fops.memcached_free               && 
                fops.memcached_result_free        && 
                fops.memcached_result_create      &&
                fops.memcached_result_key_value   && 
                fops.memcached_result_key_length  &&
                fops.memcached_result_value       &&
                fops.memcached_result_length      &&
                fops.memcached_result_flags       &&
                fops.memcached_mget               &&
                fops.memcached_fetch_result       &&
                fops.memcached_last_error_message && 
                fops.memcached_quit               &&
                fops.memcached_set                &&
                fops.memcached_delete             &&
                fops.memcached_flush              &&
                fops.memcached_flush_buffers) {
                return true;
            } else {
                return false;
            }  
        } 

        boost::shared_ptr <lib_mcache_intfx> alloc_mcache_intfx (void)
        {
            static bool init=false;
            static boost::shared_ptr <lib_mcache_intfx> mcache_intfx_ptr;

            openarchive::arch_core::spinlock_handle handle(memcache_lock);

            if (!init) {
                mcache_intfx_ptr = boost::make_shared <lib_mcache_intfx> ();
                init=true;
            }

            return mcache_intfx_ptr; 
        } 

        mem_cache::mem_cache (std::list<std::string> &servers): ready(false), 
                                                                mcache(NULL)
        {
            log_level = openarchive::cfgparams::get_log_level();
            lmcintfx = alloc_mcache_intfx ();
            if (!lmcintfx) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate intfx for " 
                               << LIBMEMCACHEDB ;
                return;
            }

            if (!lmcintfx->validate_all_symbols ()) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to validate all symbols from " 
                               << LIBMEMCACHEDB ;
                return;
            }

            fops = lmcintfx->get_fops ();
          
            /*
             * Form the config string using the list of supplied servers.
             */
            std::string cfg("");
            std::list<std::string>::iterator iter;

            for(iter = servers.begin(); iter != servers.end(); iter++) {
                if (cfg.length ()) {
                    cfg.append (" ");
                }
                cfg.append("--SERVER=");
                cfg.append(*iter);
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcachedb will be initialized with config "
                               << cfg;
            }

            mcache = fops->memcached (cfg.c_str(), cfg.length());    
            if (!mcache) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to initialize memcachedb";
                return; 
            }

            /*
             * Initialize the result structure
             */  
            if (!fops->memcached_result_create (mcache, &mresult)) { 
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to create memcached_result"
                               << fops->memcached_last_error_message (mcache);
                return; 
            }

            ready = true; 
            return;
            
        }

        mem_cache::~mem_cache (void)
        {

            fops->memcached_result_free (&mresult);

            if (mcache) {
                fops->memcached_free (mcache);
                mcache = NULL;
            }
            return;
        } 

        std::error_code mem_cache::get (struct kvpair &kv)
        {
            const char * key = kv.key.c_str();
            size_t key_len = kv.key.length();
            const char * keys[2] = { key, NULL};
            size_t key_lens[2] = {key_len, 0};
            memcached_return_t rc;
  
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }
  
            rc = fops->memcached_mget (mcache, keys, key_lens, 1);  

            if (rc != MEMCACHED_SUCCESS) {

                char *err = NULL;
                if (fops->memcached_last_error_message) {
                    err = (char *) fops->memcached_last_error_message (mcache);
                } else {
                    err = (char *) einternal.c_str();
                }

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to fetch " << kv.key 
                               << " error: " << err;
                return (std::error_code (ENOENT, std::generic_category()));
            }

            /*
             * Found the key-value pair
             */

            if (!fops->memcached_fetch_result (mcache, &mresult, &rc) || 
                rc != MEMCACHED_SUCCESS) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to fetch result for "
                               << kv.key << " error: "
                               << fops->memcached_last_error_message (mcache);
                return (std::error_code (ENOENT, std::generic_category()));
            }

            kv.value.iov_base = (void *) fops->memcached_result_value (&mresult);
            kv.value.iov_len = fops->memcached_result_length (&mresult);

            return openarchive::success;
            
        }

        std::error_code mem_cache::get (std::map<std::string, struct iovec> & kv)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }

            memcached_return_t rc;
            uint32_t count = kv.size ();
            char * keys[count];
            size_t key_lens[count];
            std::map<std::string, struct iovec>::iterator iter;

            uint32_t index=0;
            for(iter = kv.begin(); iter != kv.end(); iter++) {
                keys[index] = (char *)iter->first.c_str();
                key_lens[index++] = iter->first.length();  
            } 
  
            rc = fops->memcached_mget (mcache, keys, key_lens, count);  

            if (rc != MEMCACHED_SUCCESS) {

                char *err = NULL;
                if (fops->memcached_last_error_message) {
                    err = (char *) fops->memcached_last_error_message (mcache);
                } else {
                    err = (char *) einternal.c_str();
                }

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to fetch key/value pairs" 
                               << " error: " << err;
                return (std::error_code (ENOENT, std::generic_category()));
            }

            /*
             * Start fetching all the values from result object
             */ 
            uint32_t found = 0;
            while(fops->memcached_fetch_result (mcache, &mresult, &rc)) {
                if (rc != MEMCACHED_SUCCESS) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to fetch result"
                                   << fops->memcached_last_error_message (mcache);
                    fops->memcached_quit (mcache);
                    return (std::error_code (ENOENT, std::generic_category()));
                } 

                char * k = (char *)fops->memcached_result_key_value (&mresult);
                size_t l = fops->memcached_result_key_length (&mresult); 
      
                std::string rskey(k,l);
                iter = kv.find (rskey);
                if (iter != kv.end()) {
                    iter->second.iov_base = (void *) fops->memcached_result_value (&mresult);
                    iter->second.iov_len = fops->memcached_result_length (&mresult);
                    found++; 
                } 
            }

            if (found != count) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to fetch all key/value pairs" 
                               << " found: " << found << " expected: " <<count;
                return (std::error_code (ENOENT, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code mem_cache::set (struct kvpair & kv)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }

            memcached_return_t rc;
            rc = fops->memcached_set (mcache, kv.key.c_str(), kv.key.length (),
                                      (char *) kv.value.iov_base, 
                                      kv.value.iov_len, (time_t) kv.ttl, 
                                      (uint32_t) 0);

            if (rc != MEMCACHED_SUCCESS) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to insert key: " <<kv.key
                               << fops->memcached_last_error_message (mcache);
                return (std::error_code (EPERM, std::generic_category()));
            }

            return openarchive::success;

        }
            
        std::error_code mem_cache::set (std::map<std::string, 
                                        struct iovec> &kvmap, uint32_t ttl)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }

            std::map <std::string, struct iovec>::iterator iter;

            for(iter=kvmap.begin(); iter != kvmap.end(); iter++) {
                memcached_return_t rc;
                rc = fops->memcached_set (mcache, iter->first.c_str (), 
                                          iter->first.length (),
                                          (char *) iter->second.iov_base, 
                                          iter->second.iov_len, (time_t) ttl, 
                                          (uint32_t) 0);

                if (rc != MEMCACHED_SUCCESS) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to insert key: " <<iter->first
                                   << fops->memcached_last_error_message (mcache);
                    return (std::error_code (EPERM, std::generic_category()));
                }
            }

            return openarchive::success;
        }

        std::error_code mem_cache::drop (struct kvpair & kv)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }

            memcached_return_t rc;
            rc = fops->memcached_delete (mcache, kv.key.c_str(), 
                                         kv.key.length (), (time_t)0);

            if (rc != MEMCACHED_SUCCESS) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to delete key: " <<kv.key
                               << fops->memcached_last_error_message (mcache);
                return (std::error_code (EPERM, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code mem_cache::drop (std::list<std::string> & kvlist)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }

            std::list<std::string>::iterator it;

            for(it = kvlist.begin(); it != kvlist.end(); it++) {
                memcached_return_t rc;
                rc = fops->memcached_delete (mcache, (*it).c_str (),
                                             (*it).length (), (time_t)0);

                if (rc != MEMCACHED_SUCCESS) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to delete key: " <<*it
                                   << fops->memcached_last_error_message (mcache);
                    return (std::error_code (EPERM, std::generic_category()));
                }
            } 

            return openarchive::success;
        }

        std::error_code mem_cache::flush (void)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }

            memcached_return_t rc;
            rc = fops->memcached_flush (mcache, (time_t)0);

            if (rc != MEMCACHED_SUCCESS) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to flush"
                               << fops->memcached_last_error_message (mcache);
                return (std::error_code (EPERM, std::generic_category()));
            }

            return openarchive::success;
        }
   
        std::error_code mem_cache::sync(void)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " memcached interface not ready";
                return (std::error_code (EPERM, std::generic_category()));
            }

            memcached_return_t rc;
            rc = fops->memcached_flush_buffers (mcache);

            if (rc != MEMCACHED_SUCCESS) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to flush buffers"
                               << fops->memcached_last_error_message (mcache);
                return (std::error_code (EPERM, std::generic_category()));
            }

            return openarchive::success;
        }

    }
}
