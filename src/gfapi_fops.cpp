/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include<gfapi_fops.h> 


namespace openarchive
{
    namespace gfapi_fops
    {
        gfapi_fops::gfapi_fops (std::string name): ready(false), lib(name)
        {
            log_level = openarchive::cfgparams::get_log_level();
            init_fops(); 

            /*
             * Open libgfapi dll
             */
            dlerror ();
            handle = dlopen (lib.c_str(), RTLD_NOW);
            if (!handle) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open  " << lib
                               << " error : " << dlerror (); 
                return;
            }

            /*
             * Extract the required symbols from dll
             */
            extract_all_symbols();
            dump_all_symbols();

            ready = true; 
            
        }

        gfapi_fops::~gfapi_fops (void)
        {
            if (handle) {
                dlclose (handle);
                handle = NULL; 
            }
        } 

        void gfapi_fops::init_fops (void)
        {
            fops.gl_new                   =  NULL;
            fops.gl_set_volfile           =  NULL;
            fops.gl_set_volfile_server    =  NULL;
            fops.gl_unset_volfile_server  =  NULL;
            fops.gl_set_logging           =  NULL;
            fops.gl_init                  =  NULL;
            fops.gl_fini                  =  NULL;
            fops.gl_get_volfile           =  NULL;
            fops.gl_open                  =  NULL;
            fops.gl_creat                 =  NULL;
            fops.gl_close                 =  NULL;
            fops.gl_from_glfd             =  NULL;
            fops.gl_io_cbk                =  NULL; 
            fops.gl_pread                 =  NULL;
            fops.gl_pwrite                =  NULL;
            fops.gl_pread_async           =  NULL;
            fops.gl_pwrite_async          =  NULL;
            fops.gl_lseek                 =  NULL;
            fops.gl_truncate              =  NULL;
            fops.gl_ftruncate             =  NULL;
            fops.gl_stat                  =  NULL;
            fops.gl_fstat                 =  NULL; 
            fops.gl_getxattr              =  NULL;
            fops.gl_fgetxattr             =  NULL;
            fops.gl_setxattr              =  NULL;
            fops.gl_fsetxattr             =  NULL;
            fops.gl_removexattr           =  NULL;
            fops.gl_fremovexattr          =  NULL;
            fops.gl_free                  =  NULL;
            fops.gl_mkdir                 =  NULL;
            fops.gl_h_extract_handle      =  NULL;
            fops.gl_h_lookupat            =  NULL;
            fops.gl_dup                   =  NULL; 
            fops.gl_unlink                =  NULL;

            return;
        }  

        void* gfapi_fops::extract_symbol (std::string name)
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
       
        void gfapi_fops::dump_all_symbols (void)
        {
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Dumping all the symbols from " << lib;
            } 
                
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_new:                  0x" 
                               << std::hex
                               << (uint64_t) fops.gl_new; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_set_volfile:          0x" 
                               << std::hex
                               << (uint64_t) fops.gl_set_volfile; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_set_volfile_server:   0x" 
                               << std::hex
                               << (uint64_t) fops.gl_set_volfile_server; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_unset_volfile_server: 0x" 
                               << std::hex
                               << (uint64_t) fops.gl_unset_volfile_server; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_set_logging:          0x"
                               << std::hex
                               << (uint64_t) fops.gl_set_logging;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_init:                 0x"
                               << std::hex
                               << (uint64_t) fops.gl_init;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_fini:                 0x"
                               << std::hex
                               << (uint64_t) fops.gl_fini;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_get_volfile:          0x"
                               << std::hex
                               << (uint64_t) fops.gl_get_volfile;            
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_open:                 0x"
                               << std::hex
                               << (uint64_t) fops.gl_open;            
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_creat:                0x"
                               << std::hex
                               << (uint64_t) fops.gl_creat;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_close:                0x"
                               << std::hex
                               << (uint64_t) fops.gl_close;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_from_glfd:            0x"
                               << std::hex
                               << (uint64_t) fops.gl_from_glfd;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_pread:                0x"
                               << std::hex
                               << (uint64_t) fops.gl_pread;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_pwrite:               0x"
                               << std::hex
                               << (uint64_t) fops.gl_pwrite;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_pread_async:          0x"
                               << std::hex
                               << (uint64_t) fops.gl_pread_async;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_pwrite_async:         0x"
                               << std::hex
                               << (uint64_t) fops.gl_pwrite_async;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_lseek:                0x"
                               << std::hex
                               << (uint64_t) fops.gl_lseek;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_truncate:             0x"
                               << std::hex
                               << (uint64_t) fops.gl_truncate;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_ftruncate:            0x"
                               << std::hex
                               << (uint64_t) fops.gl_ftruncate;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_stat:                 0x"
                               << std::hex
                               << (uint64_t) fops.gl_stat;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_fstat:                0x"
                               << std::hex
                               << (uint64_t) fops.gl_fstat;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_getxattr:             0x"
                               << std::hex
                               << (uint64_t) fops.gl_getxattr;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_fgetxattr:            0x"
                               << std::hex
                               << (uint64_t) fops.gl_fgetxattr;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_setxattr:             0x"
                               << std::hex
                               << (uint64_t) fops.gl_fsetxattr;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_removexattr:          0x"
                               << std::hex
                               << (uint64_t) fops.gl_removexattr;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_fremovexattr:         0x"
                               << std::hex
                               << (uint64_t) fops.gl_fremovexattr;
            } 
            
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_free:                 0x"
                               << std::hex
                               << (uint64_t) fops.gl_free;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_mkdir:                0x"
                               << std::hex
                               << (uint64_t) fops.gl_mkdir;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_h_extract_handle:     0x"
                               << std::hex
                               << (uint64_t) fops.gl_h_extract_handle;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_h_lookupat:           0x"
                               << std::hex
                               << (uint64_t) fops.gl_h_lookupat;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_dup:                  0x"
                               << std::hex
                               << (uint64_t) fops.gl_dup;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_unlink:               0x"
                               << std::hex
                               << (uint64_t) fops.gl_unlink;
            } 
            
        }
 
        void  gfapi_fops::extract_all_symbols (void)
        {
            void *fptr;

            fptr = extract_symbol ("glfs_new");
            fops.gl_new = (glfs_new_t) fptr;

            fptr = extract_symbol ("glfs_set_volfile");
            fops.gl_set_volfile = (glfs_set_volfile_t) fptr;

            fptr = extract_symbol ("glfs_set_volfile_server");
            fops.gl_set_volfile_server = (glfs_set_volfile_server_t) fptr;

            fptr = extract_symbol ("glfs_unset_volfile_server");
            fops.gl_unset_volfile_server = (glfs_unset_volfile_server_t) fptr;

            fptr = extract_symbol ("glfs_set_logging");
            fops.gl_set_logging = (glfs_set_logging_t) fptr;

            fptr = extract_symbol ("glfs_init");
            fops.gl_init = (glfs_init_t) fptr;

            fptr = extract_symbol ("glfs_fini");
            fops.gl_fini = (glfs_fini_t) fptr;

            fptr = extract_symbol("glfs_get_volfile");
            fops.gl_get_volfile = (glfs_get_volfile_t) fptr;

            fptr = extract_symbol ("glfs_open");
            fops.gl_open = (glfs_open_t) fptr;

            fptr = extract_symbol ("glfs_creat");
            fops.gl_creat = (glfs_creat_t) fptr;

            fptr = extract_symbol ("glfs_close");
            fops.gl_close = (glfs_close_t) fptr;

            fptr = extract_symbol ("glfs_from_glfd");
            fops.gl_from_glfd = (glfs_from_glfd_t) fptr;

            fptr = extract_symbol ("glfs_pread");
            fops.gl_pread = (glfs_pread_t) fptr;

            fptr = extract_symbol ("glfs_pwrite");
            fops.gl_pwrite = (glfs_pwrite_t) fptr;

            fptr = extract_symbol ("glfs_pread_async");
            fops.gl_pread_async = (glfs_pread_async_t) fptr;

            fptr = extract_symbol ("glfs_pwrite_async");
            fops.gl_pwrite_async = (glfs_pwrite_async_t) fptr;

            fptr = extract_symbol ("glfs_lseek");
            fops.gl_lseek = (glfs_lseek_t) fptr;

            fptr = extract_symbol ("glfs_truncate");
            fops.gl_truncate = (glfs_truncate_t) fptr;

            fptr = extract_symbol ("glfs_ftruncate");
            fops.gl_ftruncate = (glfs_ftruncate_t) fptr;

            fptr = extract_symbol ("glfs_stat");
            fops.gl_stat = (glfs_stat_t) fptr;

            fptr = extract_symbol ("glfs_fstat");
            fops.gl_fstat = (glfs_fstat_t) fptr;

            fptr = extract_symbol ("glfs_getxattr");
            fops.gl_getxattr = (glfs_getxattr_t) fptr;
 
            fptr = extract_symbol ("glfs_fgetxattr");
            fops.gl_fgetxattr = (glfs_fgetxattr_t) fptr;

            fptr = extract_symbol ("glfs_setxattr");
            fops.gl_setxattr = (glfs_setxattr_t) fptr;
 
            fptr = extract_symbol ("glfs_fsetxattr");
            fops.gl_fsetxattr = (glfs_fsetxattr_t) fptr;

            fptr = extract_symbol ("glfs_removexattr");
            fops.gl_removexattr = (glfs_removexattr_t) fptr;

            fptr = extract_symbol ("glfs_fremovexattr");
            fops.gl_fremovexattr = (glfs_fremovexattr_t) fptr;

            fptr = extract_symbol ("glfs_free");
            fops.gl_free = (glfs_free_t) fptr;

            fptr = extract_symbol ("glfs_mkdir");
            fops.gl_mkdir = (glfs_mkdir_t) fptr;

            fptr = extract_symbol ("glfs_h_extract_handle");
            fops.gl_h_extract_handle = (glfs_h_extract_handle_t) fptr;

            fptr = extract_symbol ("glfs_h_lookupat");
            fops.gl_h_lookupat = (glfs_h_lookupat_t) fptr;

            fptr = extract_symbol ("glfs_dup");
            fops.gl_dup = (glfs_dup_t) fptr;

            fptr = extract_symbol ("glfs_unlink");
            fops.gl_unlink = (glfs_unlink_t) fptr;

            return;

        }

        struct libgfapi_fops & gfapi_fops::get_fops (void)
        {
            return fops;
        }

    } /* Namespace gfapi_fops */
} /* Namespace openarchive */
