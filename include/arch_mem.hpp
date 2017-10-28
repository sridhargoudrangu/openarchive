/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_MEM_H__
#define __ARCH_MEM_H__

#include <dlfcn.h>
#include <jemalloc/jemalloc.h>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/pool/object_pool.hpp>
#include <logger.h>
#include <arch_core.h>
#include <cfgparams.h>

namespace openarchive
{
    namespace arch_mem
    {
        /*
         * mempool is assumed to allocated once per thread in the thread 
         * local storage. Hence we do not use any kind of synchronization
         * primitives in the mempool operations.
         */  

        template <class T> class mempool
        {
            boost::object_pool<T> objpool;
            uint64_t alloced;
            uint64_t freed;

            public:
            mempool(void): alloced(0), freed(0)
            {
            }

            boost::shared_ptr<T> make_shared (void)
            {
                alloced++;
                return boost::shared_ptr<T> (objpool.construct(),
                       boost::bind (&openarchive::arch_mem::mempool<T>::destroy,
                                    this, _1));
            } 

            void destroy (T *ptr)
            {
                freed++;
                objpool.destroy (ptr);
            }

            uint64_t get_alloced       (void) { return alloced;                }
            uint64_t get_freed         (void) { return freed;                  }
            uint64_t get_next_req_size (void) { return objpool.get_next_size();}
           
        };

        typedef void   (*write_cb_t)                     (void *, const char *);
        typedef void * (*libmalloc_malloc_t)             (size_t);
        typedef void * (*libmalloc_calloc_t)             (size_t, size_t);
        typedef int    (*libmalloc_posix_memalign_t)     (void **, size_t, 
                                                          size_t);
        typedef void * (*libmalloc_aligned_alloc_t)      (size_t, size_t);
        typedef void * (*libmalloc_realloc_t)            (void *, size_t);
        typedef void   (*libmalloc_free_t)               (void *);
        typedef void   (*libmalloc_malloc_stats_print_t) (write_cb_t, 
                                                          void *, const char *);
        struct libmalloc_fops
        {
            libmalloc_malloc_t              malloc;
            libmalloc_calloc_t              calloc;
            libmalloc_posix_memalign_t      posix_memalign;
            libmalloc_aligned_alloc_t       aligned_alloc;
            libmalloc_realloc_t             realloc;
            libmalloc_free_t                free;
            libmalloc_malloc_stats_print_t  malloc_stats_print;
        };

        class malloc_fops
        {
            bool ready;
            void *handle;
            std::string lib;
            struct libmalloc_fops fops;
            src::severity_logger<int> log; 
            int32_t log_level;
        
            private:
            void   init_fops (void);
            void * extract_symbol (std::string);   
            void   extract_all_symbols (void);
            void   dump_all_symbols (void);

            public: 
            malloc_fops (std::string);
            ~malloc_fops (void);
            struct libmalloc_fops & get_fops (void); 
        };

        class buff;
        class malloc_intfx
        {
            malloc_fops fops; 
            struct libmalloc_fops & fptrs;
            src::severity_logger<int> log; 
            atomic_uint64_t alloced;
            atomic_uint64_t released;

            public:
            malloc_intfx  (std::string);
            void getstats (std::string &);
            malloc_fops & get_fops (void)        { return fops;     } 

            boost::shared_ptr<buff> malloc             (size_t);
            boost::shared_ptr<buff> calloc             (size_t, size_t);
            boost::shared_ptr<buff> posix_memalign     (size_t, size_t);
            boost::shared_ptr<buff> aligned_alloc      (size_t, size_t);
            void  free (buff *);
            void  malloc_stats_print (void);

            uint64_t get_page_size      (void)
            {
                uint64_t sz = sysconf (_SC_PAGESIZE);
                return sz;  
            }

            uint64_t get_alloced       (void) { return alloced.load ();   }
            uint64_t get_released      (void) { return released.load ();  }
        };

        boost::shared_ptr<malloc_intfx> get_malloc_intfx (void);
 
        class buff
        {
            void         * ptr;
            int            size;
            int            align;
            malloc_intfx * mm;

            public:

            buff  (void *, int, int, malloc_intfx *);
            ~buff (void);
            void   clear (void)    { ptr = NULL;    }
            void * get_base (void) { return ptr;    }
            int    get_size (void) { return size;   }
        };

        template <class T, uint32_t COUNT> class objpool
        {
            std::string name;             /* Name of the object pool          */
            uint32_t next_alloc_size;     /* Number of buffers to be allocated*/
                                          /* in the next new request          */
            std::atomic<uint32_t> active; /* Number of current active objects */
            std::atomic<uint32_t> total;  /* Total objects allocated          */
            boost::shared_ptr<malloc_intfx> mem_intfx;
            openarchive::arch_core::mtqueue<T*> free_pool;
            struct libmalloc_fops & fops;  

            private:
            uint32_t expand (void)
            {
                int new_slots = 0;

                for(uint32_t count = 0; count < next_alloc_size; count++) {

                    T * obj = (T *) fops.malloc (sizeof (T));

                    if (obj) {
                        if (free_pool.push (obj)) {
                            new_slots++;
                        }
                    }

                }

                total.fetch_add (new_slots);

                /* 
                 * Double the number of buffers that will be allocated in the
                 * next malloc request.
                 */  
                next_alloc_size = next_alloc_size<<1;

                return (new_slots);
            }

            public:
            objpool(std::string sn): name(sn),
                                     next_alloc_size (COUNT),
                                     mem_intfx (openarchive::arch_mem::get_malloc_intfx ()),
                                     fops (mem_intfx->get_fops ().get_fops ()) 
            {
                active.store(0);
                total.store(0);

                /*
                 * Get an instance of the malloc_intfx object
                 */ 
                mem_intfx = openarchive::arch_mem::get_malloc_intfx ();

                /*
                 * Allocate queue of buffers.
                 */
                expand ();
            } 

            ~objpool (void)
            {
                T * obj;
              
                while(free_pool.pop (obj)) {
                    
                    fops.free (obj);
                }
  
            }

            T * alloc_obj (void)
            {
                T * obj = NULL;

                if (!free_pool.pop (obj)) {

                    expand();
                    free_pool.pop (obj);
                    
                }

                if (obj) {
                  
                   new (obj) T ();
                   active.fetch_add (1);

                }

                return obj;
            }

            void release_obj (T *obj)
            {
                if (!obj) {
                    return;   
                } 
   
                obj->~T ();
 
                if (!free_pool.push (obj)) {
                    /*
                     * Failed to add back to queue. Better discard it.
                     */
                    fops.free (obj);
                    total.fetch_sub (1);
                    return;
                }

                active.fetch_sub (1);
                return; 
            }

            boost::shared_ptr <T> make_shared (void)
            {
                T * obj = alloc_obj ();

                return boost::shared_ptr <T> (obj, 
                       boost::bind (&openarchive::arch_mem::objpool<T,
                                    COUNT>::destroy, this, _1));

            }

            void destroy (T *obj)
            {
                release_obj (obj);   
            }


            void getstats (std::string &stat)
            {
                /*
                 * The values returned by getstats may not be exactly correct 
                 * because we don't lock before logging. The values returned 
                 * should only be used for trending.
                 */ 
                stat = " name: " + 
                       name +
                       " next_alloc_size: " + 
                       boost::lexical_cast <std::string> (next_alloc_size) +
                       " active: " +
                       boost::lexical_cast <std::string> (active.load ()) +  
                       " total: " +
                       boost::lexical_cast <std::string> (total.load ());
                return;
            }
            
        }; 

        template <class T, uint32_t COUNT> class structpool
        {
            std::string name;             /* Name of the struct pool          */
            uint32_t next_alloc_size;     /* Number of buffers to be allocated*/
                                          /* in the next new request          */
            std::atomic<uint32_t> active; /* Number of current active objects */
            std::atomic<uint32_t> total;  /* Total objects allocated          */
            boost::shared_ptr<malloc_intfx> mem_intfx;
            openarchive::arch_core::mtqueue<T*> free_pool;
            struct libmalloc_fops & fops;  

            private:
            uint32_t expand (void)
            {
                int new_slots = 0;

                for(uint32_t count = 0; count < next_alloc_size; count++) {

                    T * obj = (T *) fops.malloc (sizeof (T));

                    if (obj) {
                        if (free_pool.push (obj)) {
                            new_slots++;
                        }
                    }

                }

                total.fetch_add (new_slots);

                /* 
                 * Double the number of buffers that will be allocated in the
                 * next malloc request.
                 */  
                next_alloc_size = next_alloc_size<<1;

                return (new_slots);
            }

            public:
            structpool(std::string sn): name (sn),
                                     next_alloc_size (COUNT),
                                     mem_intfx (openarchive::arch_mem::get_malloc_intfx ()),
                                     fops (mem_intfx->get_fops ().get_fops ()) 
            {
                active.store(0);
                total.store(0);

                /*
                 * Get an instance of the malloc_intfx object
                 */ 
                mem_intfx = openarchive::arch_mem::get_malloc_intfx ();

                /*
                 * Allocate queue of buffers.
                 */
                expand ();
            } 

            ~structpool (void)
            {
                T * obj;
              
                while(free_pool.pop (obj)) {
                    
                    fops.free (obj);
                }
  
            }

            T * alloc (void)
            {
                T * obj;

                if (!free_pool.pop (obj)) {

                    expand();
                    free_pool.pop (obj);
                    
                }

                if (obj) {
                  
                    active.fetch_add (1);

                }

                return obj;
            }

            void release (T *obj)
            {

                if (!obj) {
                    return;   
                } 
   
                if (!free_pool.push (obj)) {
                    /*
                     * Failed to add back to queue. Better discard it.
                     */
                    fops.free (obj);
                    total.fetch_sub (1);
                    return;
                }

                active.fetch_sub (1);
                return; 
            }

            void getstats (std::string &stat)
            {
                /*
                 * The values returned by getstats may not be exactly correct 
                 * because we don't lock before logging. The values returned 
                 * should only be used for trending.
                 */ 
                stat = " name: " + 
                       name +
                       " next_alloc_size: " + 
                       boost::lexical_cast <std::string> (next_alloc_size) +
                       " active: " +
                       boost::lexical_cast <std::string> (active.load ()) +  
                       " total: " +
                       boost::lexical_cast <std::string> (total.load ());
                return;
            }
            
        }; 

        class plbuff
        {
            void   * ptr;
            size_t size;
            off_t f_offset;
            size_t f_bytes; 

            public:
            plbuff  (void *p , size_t z): ptr(p), size(z)
            {
            }

            void   set_base (void *p)    { ptr = p;         }
            void   set_size (size_t z)   { size = z;        }
            void   set_offset (off_t of) { f_offset = of;   }
            void   set_bytes (size_t sz) { f_bytes = sz;    }
            void * get_base (void)       { return ptr;      }
            size_t get_size (void)       { return size;     }
            off_t  get_offset (void)     { return f_offset; }
            size_t get_bytes (void)      { return f_bytes;  }
            void   clear (void)          { ptr = NULL;      }
        };

        template <size_t SIZE, uint32_t COUNT> class plbpool
        {
            std::string name;             /* Name of the plain buff pool      */
            uint32_t next_alloc_size;     /* Number of buffers to be allocated*/
                                          /* in the next new request          */
            std::atomic<uint32_t> active; /* Number of current active objects */
            std::atomic<uint32_t> total;  /* Total objects allocated          */
            boost::shared_ptr<malloc_intfx> mem_intfx;
            openarchive::arch_core::mtqueue<plbuff *> free_pool;
            struct libmalloc_fops & fops;  

            private:
            uint32_t expand (void)
            {
                int new_slots = 0;
                size_t page_size = mem_intfx->get_page_size ();
               
                for(uint32_t count = 0; count < next_alloc_size; count++) {
                    
                    plbuff * pb = (plbuff *) fops.calloc (1, sizeof (plbuff));
                    if (pb) { 
                        void * ptr = NULL;
                        int32_t ret = fops.posix_memalign (&ptr, page_size, 
                                                           SIZE);
                        if (!ret) {
                            pb->set_size (SIZE);
                            pb->set_base (ptr);
                            if (free_pool.push (pb)) {
                                new_slots++;
                            } else {
                                free (pb);    
                            }
                        } else {
                            free (pb);
                        }
                    } 

                }

                total.fetch_add (new_slots);

                /* 
                 * Double the number of buffers that will be allocated in the
                 * next posix_memalign request.
                 */  
                next_alloc_size = next_alloc_size<<1;

                return (new_slots);
            }

            public:
            plbpool(std::string sn): name(sn),
                                     next_alloc_size (COUNT),
                                     mem_intfx (openarchive::arch_mem::get_malloc_intfx ()),
                                     fops (mem_intfx->get_fops ().get_fops ()) 
            {
                active.store(0);
                total.store(0);

                /*
                 * Get an instance of the malloc_intfx object
                 */ 
                mem_intfx = openarchive::arch_mem::get_malloc_intfx ();

                /*
                 * Allocate queue of buffers.
                 */
                expand ();
            } 

            ~plbpool (void)
            {
                plbuff * pb;
              
                while(free_pool.pop (pb)) {
                    free (pb);
                }
            }

            boost::shared_ptr <plbuff> make_shared (void)
            {
                plbuff * pb;

                if (!free_pool.pop (pb)) {
                    expand();
                    free_pool.pop (pb);
                }

                if (pb) {
                   active.fetch_add (1);
                }

                return boost::shared_ptr <plbuff> (pb, 
                       boost::bind (&openarchive::arch_mem::plbpool<SIZE,
                                    COUNT>::destroy, this, _1));

            }

            void destroy (plbuff *pb)
            {

                if (!pb) {
                    return;   
                } 
   
                if (!free_pool.push (pb)) {
                    /*
                     * Failed to add back to queue. Better discard it.
                     */
                    free (pb);
                    total.fetch_sub (1);
                    return;
                }

                active.fetch_sub (1);
                return; 
            }

            void free (plbuff *pb)
            {
                if (pb) {
                    void * ptr = pb->get_base();   
                    if (ptr) {
                        fops.free (ptr);
                    }
                    fops.free (pb);
                } 

                return; 
            }

            void getstats (std::string &stat)
            {
                /*
                 * The values returned by getstats may not be exactly correct 
                 * because we don't lock before logging. The values returned 
                 * should only be used for trending.
                 */ 
                stat = " name: " + 
                       name +
                       " next_alloc_size: " + 
                       boost::lexical_cast <std::string> (next_alloc_size) +
                       " active: " +
                       boost::lexical_cast <std::string> (active.load ()) +  
                       " total: " +
                       boost::lexical_cast <std::string> (total.load ());
                return;
            }
            
        }; 

    } /*namespace arch_mem */

    typedef openarchive::arch_mem::malloc_intfx malloc_intfx_t; 
    typedef boost::shared_ptr<malloc_intfx_t>   malloc_intfx_ptr_t;
    typedef openarchive::arch_mem::buff         buff_t;
    typedef boost::shared_ptr<buff_t>           buff_ptr_t; 
    typedef openarchive::arch_mem::plbuff       plbuff_t;
    typedef boost::shared_ptr <plbuff_t>        plbuff_ptr_t; 

} /*namespace openarchive */

#endif
