/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <arch_mem.hpp>

namespace openarchive
{
    static openarchive::arch_core::spinlock mem_lock;
    std::string libmalloc = "libjemalloc.so"; 

    namespace arch_mem
    {
        malloc_fops::malloc_fops (std::string name): ready(false), lib(name)
        {
            log_level = openarchive::cfgparams::get_log_level();
            init_fops(); 

            /*
             * Open libjemalloc dll
             */
            dlerror ();  
            handle = dlopen (lib.c_str(), RTLD_NOW);
            if (!handle) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open  " << lib
                               << " error : "  << dlerror ();
                return;
            }

            /*
             * Extract the required symbols from dll
             */
            extract_all_symbols();
            dump_all_symbols();

            ready = true; 
            
        }

        malloc_fops::~malloc_fops (void)
        {
            if (handle) {
                dlclose (handle);
                handle = NULL; 
            }
        } 

        void malloc_fops::init_fops (void)
        {
            fops.malloc                   =  NULL ;
            fops.calloc                   =  NULL;
            fops.posix_memalign           =  NULL;
            fops.aligned_alloc            =  NULL;
            fops.realloc                  =  NULL;
            fops.free                     =  NULL;
            fops.malloc_stats_print       =  NULL;

            return;
        }  

        void* malloc_fops::extract_symbol (std::string name)
        {
            dlerror(); /* Reset errors */
            
            void *fptr = dlsym (handle, name.c_str());
            const char *dlsymerr = dlerror();
            if (dlsymerr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find " << name << " in " << lib
                               << " error desc: "    << dlsymerr;
                return NULL;
            }    

            return fptr; 
        }
       
        void malloc_fops::dump_all_symbols (void)
        {
                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " Dumping all the symbols from " << lib;
                }
                
                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " malloc:                    0x" 
                                   << std::hex
                                   << (uint64_t) fops.malloc; 
                }

                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " calloc:                    0x" 
                                   << std::hex
                                   << (uint64_t) fops.calloc;
                }

                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " posix_memalign:            0x" 
                                   << std::hex
                                   << (uint64_t) fops.posix_memalign; 
                }

                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " aligned_alloc:             0x" 
                                   << std::hex
                                   << (uint64_t) fops.aligned_alloc; 
                }

                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " realloc:                   0x"
                                   << std::hex
                                   << (uint64_t) fops.realloc;
                }

                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " free:                      0x"
                                   << std::hex
                                   << (uint64_t) fops.free;
                }

                {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " malloc_stats_print:        0x"
                                   << std::hex
                                   << (uint64_t) fops.malloc_stats_print;
                }

        }
 
        void  malloc_fops::extract_all_symbols (void)
        {
            void *fptr;

            fptr = extract_symbol ("malloc");
            fops.malloc = (libmalloc_malloc_t) fptr;

            fptr = extract_symbol ("calloc");
            fops.calloc = (libmalloc_calloc_t) fptr;

            fptr = extract_symbol ("posix_memalign");
            fops.posix_memalign = (libmalloc_posix_memalign_t) fptr;

            fptr = extract_symbol ("aligned_alloc");
            fops.aligned_alloc = (libmalloc_aligned_alloc_t) fptr;

            fptr = extract_symbol ("realloc");
            fops.realloc = (libmalloc_realloc_t) fptr;

            fptr = extract_symbol ("free");
            fops.free = (libmalloc_free_t) fptr;

            fptr = extract_symbol ("malloc_stats_print");
            fops.malloc_stats_print = (libmalloc_malloc_stats_print_t) fptr;

            return;

        }

        struct libmalloc_fops & malloc_fops::get_fops (void)
        {
            return fops;
        }

        buff::buff (void *p, int s, int a, malloc_intfx *m): ptr(p), size(s), 
                                                             align(a), mm(m)
        {
        }  

        buff::~buff (void)
        {
            if (ptr) {

                mm->free (this);
                ptr = NULL;
                  
            }
        }

        /*
         * glibc provided memory management functions like malloc have the 
         * following problems:
         * 1) Fragmentation
         * 2) Lock contention
         * Some of the modern memory managers like tcmalloc and jemalloc seem
         * to be good at handling the above two problems. We will check for the
         * presence of tcmalloc or jemalloc provided memory managment functions
         * on the server and will use them if any of these libraries is present.
         * If none of these are present then we will revert to the good old 
         * malloc/free provided by glibc.
         */   
        malloc_intfx::malloc_intfx(std::string lib):fops(lib),
                                                    fptrs(fops.get_fops ()) 
        {
            alloced.store (0);
            released.store (0);
        }

        void malloc_intfx::getstats (std::string &stat)
        {

            stat = std::string (" memory allocation summary: ") +
                   std::string (" allocated memory: ") +       
                   boost::lexical_cast <std::string> (alloced.load()) + 
                   std::string (" released memory: ") +       
                   boost::lexical_cast <std::string> (released.load());

            return;
                            
        }

        boost::shared_ptr<buff> malloc_intfx::malloc (size_t size)
        {
            void * ptr = NULL;

            if (fptrs.malloc) {

                ptr = fptrs.malloc (size);

            } else {

                ptr = ::malloc (size);

            }

            alloced.fetch_add (size);

            return boost::shared_ptr<buff> (new buff (ptr, size, 0, this),
                                            boost::bind (&malloc_intfx::free,
                                                         this, _1));
        }

        void malloc_intfx::free (buff * ptr)
        {
            if (fptrs.free) {

                fptrs.free (ptr->get_base ());

            } else {

                ::free (ptr->get_base ());

            }
            
            ptr->clear ();

            released.fetch_add (ptr->get_size ());

            return;
        }

        boost::shared_ptr<buff> malloc_intfx::calloc (size_t count, size_t size)
        {
            void * ptr = NULL;

            if (fptrs.calloc) {

                ptr = fptrs.calloc (count, size);

            } else {

                ptr = ::calloc (count, size);

            }

            alloced.fetch_add (size);

            return boost::shared_ptr<buff> (new buff (ptr, size, 0, this),
                                            boost::bind (&malloc_intfx::free,
                                                         this, _1));
        }

        boost::shared_ptr<buff> malloc_intfx::posix_memalign (size_t align, 
                                                              size_t size)
        {
            void * ptr = NULL;

            if (fptrs.posix_memalign) {

                fptrs.posix_memalign (&ptr, align, size);

            } else {

                ::posix_memalign (&ptr, align, size);

            }

            alloced.fetch_add (size);

            return boost::shared_ptr<buff> (new buff (ptr, size, align, this),
                                            boost::bind (&malloc_intfx::free,
                                                         this, _1));
        } 

        boost::shared_ptr<buff> malloc_intfx::aligned_alloc(size_t align, 
                                                            size_t size)
        {
            void * ptr = NULL;

            if (fptrs.aligned_alloc) {

                ptr = fptrs.aligned_alloc (align, size);

            } else {

                ptr = ::aligned_alloc (align, size);

            }

            alloced.fetch_add (size);

            return boost::shared_ptr<buff> (new buff (ptr, size, align, this),
                                            boost::bind (&malloc_intfx::free,
                                                         this, _1));
        } 
            
        malloc_intfx_ptr_t get_malloc_intfx (void)
        {
            openarchive::arch_core::spinlock_handle handle(mem_lock);

            static bool init=false;
            static malloc_intfx_ptr_t malloc_ptr;

            if (!init) {

                malloc_ptr = boost::make_shared <malloc_intfx_t> (libmalloc);
                init=true;
            }

            return malloc_ptr; 
        }

    } /*namespace arch_mem */

} /*namespace openarchive */
