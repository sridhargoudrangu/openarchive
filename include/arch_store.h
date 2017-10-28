/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_STORE__
#define __ARCH_STORE__

#include <archivestore.h>

namespace openarchive
{
    namespace arch_store 
    {
        class arch_store_cbk_info;  
        typedef void (*arch_store_callback_t) (boost::shared_ptr<arch_store_cbk_info>);

        class arch_store_cbk_info
        {
            archstore_desc_t     * store_desc;
            app_callback_info_t    app_cbk_info;
            app_callback_t         app_cbk;
            void                 * app_ck;
            arch_store_callback_t  store_cbk;
            int64_t                ret_code;
            int32_t                err_code;
            atomic_vol_uint64_t    req;
            atomic_vol_bool        done;

            public:

            arch_store_cbk_info (void): store_desc (NULL),
                                        app_cbk (NULL),
                                        app_ck (NULL),
                                        store_cbk (NULL),
                                        ret_code (0),
                                        err_code (0)
            {
                app_cbk_info = {0,};
                req.store (0);
                done.store (false);
            }

            inline void set_store_desc (archstore_desc_t *desc)
            {
                store_desc = desc;
            }

            inline void set_src_store (archstore_info_t *store)
            {
                app_cbk_info.src_archstore = store; 
            } 

            inline void set_src_archfile (archstore_fileinfo_t *file)
            {
                app_cbk_info.src_archfile = file;
            }

            inline void set_dest_store (archstore_info_t *store)
            {
                app_cbk_info.dest_archstore = store; 
            }

            inline void set_dest_archfile (archstore_fileinfo_t *file)
            {
                app_cbk_info.dest_archfile = file;
            }

            inline void set_app_cbk (app_callback_t cbk)
            {
                app_cbk = cbk;
            }

            inline void set_app_cookie ( void *ck)
            {
                app_ck = ck;
            }

            inline void set_store_cbk (arch_store_callback_t cbk)
            {
                store_cbk = cbk;
            }   

            inline void set_ret_code (int64_t r)
            {
                ret_code = r;
            }

            inline void set_err_code (int32_t e)
            {
                err_code = e;
            }

            inline uint64_t incr_req (void)
            {
                return req.fetch_add (1);
            } 

            inline uint64_t decr_req (void)
            {
                return req.fetch_sub (1);
            } 

            inline void set_done (bool b)
            {
                done.store (b);
            } 

            inline archstore_info_t * get_src_store (void)
            {
                return app_cbk_info.src_archstore;
            }

            inline archstore_fileinfo_t * get_src_archfile (void)
            {
                return app_cbk_info.src_archfile;
            }

            inline archstore_info_t * get_dest_store   (void)
            {
                return app_cbk_info.dest_archstore;
            }

            inline archstore_fileinfo_t * get_dest_archfile (void)
            {
                return app_cbk_info.dest_archfile;
            }

            inline app_callback_t get_app_cbk (void)
            {
                return app_cbk;
            }

            inline void * get_app_cookie (void)
            {
                return app_ck;
            }

            inline arch_store_callback_t get_store_cbk (void)
            {
                return store_cbk;
            }   

            inline int64_t get_ret_code (void)
            {
                return ret_code;
            }

            inline int32_t get_err_code (void)
            {
                return err_code;
            } 

            inline app_callback_info_t * get_app_callback_info_ptr (void)
            {
                return &app_cbk_info;
            }  

            inline archstore_desc_t * get_store_desc (void)
            {
                return store_desc; 
            } 

            inline uint64_t get_req (void)
            {
                return req.load ();
            } 

            inline bool get_done (void)
            {
                return done.load ();
            } 

        };
    }

}

typedef openarchive::arch_store::arch_store_cbk_info arch_store_cbk_info_t;
typedef boost::shared_ptr <arch_store_cbk_info_t>    arch_store_cbk_info_ptr_t;

#endif /* End of __ARCH_STORE__ */
