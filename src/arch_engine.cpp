/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <arch_engine.h>
#include <gfapi_iopx.h>
#include <meta_iopx.h>
#include <fdcache_iopx.h>
#include <perf_iopx.h>

typedef boost::tokenizer<boost::char_separator<char>> tokenizer_t;
typedef std::map <std::string, std::string> params_map_t; 

namespace openarchive
{
 
    static openarchive::arch_core::spinlock big_lock;

    namespace arch_engine
    {
        /*
         * It is assumed that there will be only one arch_engine per 
         * application.
         */

        arch_engine::arch_engine(bool bfast, bool bslow): enable_fast (bfast),
                                                          enable_slow (bslow),
                                                          nfastthreads (0),
                                                          nslowthreads (0) 
        {

            log_level = openarchive::cfgparams::get_log_level();

            std::error_code ec = alloc_engine_resources();
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV(log, openarchive::logger::level_error)
                              << " failed to allocate resources for engine "
                              << " error code: " << ec.value() 
                              << " error desc: " << ec.message();
                return;
            }
            
            ready = true;
        }

        arch_engine::~arch_engine (void)
        {
            stop ();
        }

        void arch_engine::stop (void)
        {
            ready=false;
            release_engine_resources(); 
        }

        io_service_ptr_t arch_engine::alloc_ioservice (std::string srvtype)
        {
            io_service_ptr_t iosvc = boost::make_shared<
                                            boost::asio::io_service>();
            if (!iosvc) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV(log, openarchive::logger::level_error)
                              << " failed to allocate " <<srvtype 
                              << " ioservice ";
            }

            return iosvc;
        }

        work_ptr_t arch_engine::alloc_worker (io_service_ptr_t srv,
                                              std::string wtype)
        {
            work_ptr_t worker;
            worker = boost::make_shared<boost::asio::io_service::work> (*srv);

            if (!worker) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV(log, openarchive::logger::level_error)
                              << " failed to allocate " <<wtype 
                              << " worker ";
            }

            return worker;
        }

        uint32_t arch_engine::create_threads (io_service_ptr_t iosvc,
                                              boost::thread_group &tg,
                                              uint32_t nthreads)
        {
            uint32_t ret = 0;
  
            for(uint32_t count=0; count<nthreads; count++) {
                boost::thread *th = tg.create_thread(boost::bind(&worker_thread,
                                                                 iosvc, count));
                if (th) {
     
                    ret++;

                    boost::thread::native_handle_type hnd = th->native_handle();

                    int policy;
                    struct sched_param param;
                    if (!pthread_getschedparam(hnd, &policy, &param)) {

                        if (log_level >= openarchive::logger::level_debug_2) {
                            BOOST_LOG_FUNCTION ();
                            BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                           << " Thread " << count 
                                           << " created with policy: " <<policy
                                           << " priority: " << param.sched_priority;
                        }

                    }
                } else {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)
                                  << " Failed to create threadi " << count;
                }
            }

            return ret;
        } 
                                                        
  
        std::error_code arch_engine::alloc_engine_resources(void)
        {
            if (enable_fast) {

                nfastthreads = boost::thread::hardware_concurrency();

                fast_iosvc = alloc_ioservice ("fast");
                if (!fast_iosvc) {
                    return (std::error_code(ENOMEM, std::generic_category()));
                }

                fast_worker = alloc_worker (fast_iosvc, "fast");
                if (!fast_worker) {
                    return (std::error_code (ENOMEM, std::generic_category()));
                }

                nfastthreads = create_threads (fast_iosvc, fast_threads, 
                                               nfastthreads);  
            }

            if (enable_slow) {

                nslowthreads = boost::thread::hardware_concurrency();

                slow_iosvc = alloc_ioservice ("slow");
                if (!slow_iosvc) {
                    return (std::error_code(ENOMEM, std::generic_category()));
                }

                slow_worker = alloc_worker (slow_iosvc, "slow");
                if (!fast_worker) {
                    return (std::error_code (ENOMEM, std::generic_category()));
                }

                nslowthreads = create_threads (slow_iosvc, slow_threads, 
                                               nslowthreads);  
            }

            if (log_level >= openarchive::logger::level_error) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Number of worker threads that were created "
                               << " Fast: " << nfastthreads 
                               << " Slow: " << nslowthreads;
            }

            return openarchive::success;

        }

        std::error_code arch_engine::release_engine_resources(void)
        {
            if (enable_fast) {
                fast_iosvc->stop();
                fast_threads.interrupt_all();
                fast_threads.join_all();
                enable_fast = false;
            }

            if (enable_slow) {
                slow_iosvc->stop();
                slow_threads.interrupt_all();
                slow_threads.join_all();
                enable_slow = false;
            }

            return openarchive::success;
        }

        iopx_ptr_t arch_engine::mkgltree (iopx_tree_cfg_t & tree_cfg)
        {
            /*
             * We will create a iopx tree which would access data from a 
             * glusterfs volume through libgfapi interface.
             */
            io_service_ptr_t ptr = (tree_cfg.enable_fast_iosvc? fast_iosvc : 
                                                                slow_iosvc);
            iopx_ptr_t iopx, parent;
            uint32_t ttl = tree_cfg.meta_cache_ttl;
            uint32_t cache_size = tree_cfg.fd_cache_size;   
            std::string store = tree_cfg.store;

            iopx = boost::make_shared <perf_iopx_t> ("perf", ptr);
            parent = iopx;
            
            if (tree_cfg.enable_meta_cache) {
                iopx_ptr_t ch = boost::make_shared <meta_iopx_t> ("meta", ptr, 
                                                                  ttl);
                parent->add_child (ch);
                ch->set_parent (parent); 
                parent = ch;
            }

            if (tree_cfg.enable_fd_cache) {
                iopx_ptr_t ch = boost::make_shared <fdcache_iopx_t> ("fdcache", 
                                                                     ptr,
                                                                     cache_size);
                parent->add_child (ch);
                ch->set_parent (parent); 
                parent = ch;
            }
            
            iopx_ptr_t ch = boost::make_shared <gfapi_iopx_t> ("gfapi", ptr,
                                                               store);
            parent->add_child (ch);  
            ch->set_parent (parent); 

            return iopx;

        }

        iopx_ptr_t arch_engine::mkcvlttree (struct iopx_tree_cfg & tree_cfg)
        {
            /*
             * We will create a iopx tree which would access data from a 
             * commvault content store through openbackupapi interface.
             */
            io_service_ptr_t ptr = (tree_cfg.enable_fast_iosvc? fast_iosvc : 
                                                                slow_iosvc);
            iopx_ptr_t iopx, parent;
            uint32_t cache_size = tree_cfg.fd_cache_size;   
            std::string store = tree_cfg.store;

            iopx = boost::make_shared <perf_iopx_t> ("perf", ptr);
            parent = iopx;
            
            if (tree_cfg.enable_fd_cache) {
                iopx_ptr_t ch = boost::make_shared <fdcache_iopx_t> ("fdcache", 
                                                                     ptr,
                                                                     cache_size);
                parent->add_child (ch);
                ch->set_parent (parent); 
                parent = ch;
            }
           
            uint32_t nthreads = (tree_cfg.enable_fast_iosvc? nfastthreads :
                                                             nslowthreads);
   
            iopx_ptr_t ch = boost::make_shared <cvlt_iopx_t> ("cvlt", ptr,
                                                              store, nthreads);
            parent->add_child (ch);  
            ch->set_parent (parent); 

            return iopx;
        }

        iopx_ptr_t arch_engine::mktree (iopx_tree_cfg_t & tree_cfg)
        {
            /*
             * Depending on the product and engine desc the tree of iopx will 
             * be created.
             */

            if (tree_cfg.product == "glusterfs") {
                return mkgltree (tree_cfg);
            } else if (tree_cfg.product == "commvault") {
                return mkcvlttree (tree_cfg);
            }

            iopx_ptr_t dummy;
            return dummy;
        }

        void arch_engine::map_cvlt_store_id (std::string &inp_store,
                                             std::string &outp_store)
        {
            /*
             * The store id provided during backup may not be applicable
             * during read. We will convert the store id provided during
             * backup to one which can be applied during read.
             * The store id is expected to be in the following format.
             * "cc=2:cn=node1:ph=node2:pp=8400:at=29:in=Instance001:bs=idm:"
             * "sc=arch:ji=124:jt=full-backup:ns=16"
             * The meaning of each argument label is explained below:
             * cc - CommCell ID
             * cn - Commvault Host Name
             * ph - Proxy host name
             * pp - Proxy port number
             * at - App type. The valid values for app type are
             *      29 - Linux File system APP type
             * in - Instance Name 
             * bs - BackupSet Name
             * sc - SubClient Name
             * ji - Job ID
             * jk - Job token
             * jt - Job Type. Valid values are 
             *      browse, full-backup, incr-backup, restore
             * ns - Number of streams
             */ 
            outp_store.clear ();
            openarchive::arch_core::arg_parser parser(inp_store);

            /*
             * All the arguments have been extracted. Parse the arguments 
             * and extract parameters.
             */
            std::list<std::string> args;
            args.push_back ("cc");
            args.push_back ("cn");
            args.push_back ("ph");
            args.push_back ("pp");
            args.push_back ("at");
            args.push_back ("in");
            args.push_back ("bs");
            args.push_back ("sc");

            std::list<std::string>::iterator iter;
            for (iter = args.begin(); iter != args.end(); iter++) { 
                std::string value;
                std::error_code ec = parser.extract_param (*iter, value);
                if (ec == ok) {
                    if (outp_store.length () > 0) {
                        outp_store.append (":");
                    }
                    outp_store.append (*iter);
                    outp_store.append ("=");
                    outp_store.append (value);  
                }
            }

            if (outp_store.length () > 0) {
                outp_store.append (":");
            }
            outp_store.append ("jt=restore:ns=1");
               
            return;
        }
 
        void arch_engine::map_store_id (std::string &product,
                                        std::string &inp_store, 
                                        std::string &outp_store)
        {
            if (product == "commvault") {
                map_cvlt_store_id (inp_store, outp_store);
            } else {
                outp_store = inp_store;
            }

            return;
        }

        void worker_thread(io_service_ptr_t ptr, uint32_t threadid)
        {
            ptr->run();
        }


        arch_engine_ptr_t alloc_engine (void)
        {
            openarchive::arch_core::spinlock_handle handle(big_lock);

            static bool init=false;
            static arch_engine_ptr_t engine_ptr;

            if (!init) {

                bool bfast = openarchive::cfgparams::create_fast_threads ();
                bool bslow = openarchive::cfgparams::create_slow_threads ();

                engine_ptr = boost::make_shared <arch_engine_t> (bfast, bslow);
                init=true;
            }

            return engine_ptr; 
        } 

    } /* namespace arch_engine */
} /*  namespace openarchive */    
