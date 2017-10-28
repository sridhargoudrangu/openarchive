/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __GFAPI_FOPS_H__
#define __GFAPI_FOPS_H__

#include <dlfcn.h>
#include <glfs.h>
#include <glfs-handles.h>
#include <logger.h>
#include <cfgparams.h>

namespace openarchive
{
    namespace gfapi_fops
    {
        typedef glfs_t *     (*glfs_new_t)                   (const char *);

        typedef int          (*glfs_set_volfile_t)           (glfs_t *, 
                                                              const char *);

        typedef int          (*glfs_set_volfile_server_t)    (glfs_t *fs, 
                                                              const char *,
                                                              const char *, 
                                                              int);

        typedef int          (*glfs_unset_volfile_server_t)  (glfs_t *, 
                                                              const char *,
                                                              const char *, 
                                                              int);

        typedef int          (*glfs_set_logging_t)           (glfs_t *,
                                                              const char *, 
                                                              int);

        typedef int          (*glfs_init_t)                  (glfs_t *);

        typedef int          (*glfs_fini_t)                  (glfs_t *);

        typedef ssize_t      (*glfs_get_volfile_t)           (glfs_t *, void *, 
                                                              size_t);

        typedef glfs_fd_t *  (*glfs_open_t)                  (glfs_t *, 
                                                              const char *, 
                                                              int);

        typedef glfs_fd_t *  (*glfs_creat_t)                 (glfs_t *,
                                                              const char *,
                                                              int, mode_t);

        typedef int          (*glfs_close_t)                 (glfs_fd_t *);

        typedef glfs_t *     (*glfs_from_glfd_t)             (glfs_fd_t *);

        typedef void         (*glfs_io_cbk_t)                (glfs_fd_t *fd, 
                                                              ssize_t, void *);

        typedef ssize_t      (*glfs_pread_t)                 (glfs_fd_t *, 
                                                              void *, size_t,
                                                              off_t, int);

        typedef ssize_t      (*glfs_pwrite_t)                (glfs_fd_t *,
                                                              const void *,
                                                              size_t, off_t,
                                                              int);

        typedef int          (*glfs_pread_async_t)           (glfs_fd_t *, 
                                                              void *, size_t, 
                                                              off_t, int, 
                                                              glfs_io_cbk_t, 
                                                              void *);

        typedef int          (*glfs_pwrite_async_t)          (glfs_fd_t *, 
                                                              const void *, 
                                                              int, off_t, int,
                                                              glfs_io_cbk_t,
                                                              void *);

        typedef off_t        (*glfs_lseek_t)                 (glfs_fd_t *, 
                                                              off_t, int);

        typedef int          (*glfs_truncate_t)              (glfs_t *, 
                                                              const char *, 
                                                              off_t);

        typedef int          (*glfs_ftruncate_t)             (glfs_fd_t *, 
                                                              off_t);

        typedef int          (*glfs_stat_t)                  (glfs_t *, 
                                                              const char *,
                                                              struct stat *);

        typedef int          (*glfs_fstat_t)                 (glfs_fd_t *, 
                                                              struct stat *);

        typedef ssize_t      (*glfs_getxattr_t)              (glfs_t *, 
                                                              const char *, 
                                                              const char *,
		                                              void *, size_t);

        typedef ssize_t      (*glfs_fgetxattr_t)             (glfs_fd_t *, 
                                                              const char *,
			                                      void *, size_t);

        typedef int          (*glfs_setxattr_t)              (glfs_t *, 
                                                              const char *, 
                                                              const char *,
		                                              const void *, 
                                                              size_t, int); 

        typedef int          (*glfs_fsetxattr_t)             (glfs_fd_t *, 
                                                              const char *,
		                                              const void *, 
                                                              size_t, int);

        typedef int          (*glfs_removexattr_t)           (glfs_t *, 
                                                              const char *, 
                                                              const char *);

        typedef int          (*glfs_fremovexattr_t)          (glfs_fd_t *, 
                                                              const char *);

        typedef void         (*glfs_free_t)                  (void *ptr);

        typedef int          (*glfs_mkdir_t)                 (glfs_t *, 
                                                              const char *, 
                                                              mode_t);

        typedef ssize_t      (*glfs_h_extract_handle_t)      (struct glfs_object *,
			                                      unsigned char *, 
                                                              int);

        typedef struct glfs_object * (*glfs_h_lookupat_t)    (struct glfs *,
                                                              struct glfs_object *,
                                                              const char *,
                                                              struct stat *, 
                                                              int);

        typedef glfs_fd_t *  (*glfs_dup_t)                   (glfs_fd_t *);

        typedef int          (*glfs_unlink_t)                (glfs_t *, 
                                                              const char *);


        struct libgfapi_fops
        {
            glfs_new_t                   gl_new;
            glfs_set_volfile_t           gl_set_volfile;
            glfs_set_volfile_server_t    gl_set_volfile_server;
            glfs_unset_volfile_server_t  gl_unset_volfile_server;
            glfs_set_logging_t           gl_set_logging;
            glfs_init_t                  gl_init;
            glfs_fini_t                  gl_fini;
            glfs_get_volfile_t           gl_get_volfile;
            glfs_open_t                  gl_open;
            glfs_creat_t                 gl_creat;
            glfs_close_t                 gl_close;
            glfs_from_glfd_t             gl_from_glfd;
            glfs_io_cbk_t                gl_io_cbk; 
            glfs_pread_t                 gl_pread;
            glfs_pwrite_t                gl_pwrite;
            glfs_pread_async_t           gl_pread_async;
            glfs_pwrite_async_t          gl_pwrite_async;
            glfs_lseek_t                 gl_lseek;
            glfs_truncate_t              gl_truncate;
            glfs_ftruncate_t             gl_ftruncate;
            glfs_stat_t                  gl_stat;
            glfs_fstat_t                 gl_fstat; 
            glfs_getxattr_t              gl_getxattr;
            glfs_fgetxattr_t             gl_fgetxattr;
            glfs_setxattr_t              gl_setxattr;
            glfs_fsetxattr_t             gl_fsetxattr;
            glfs_removexattr_t           gl_removexattr;
            glfs_fremovexattr_t          gl_fremovexattr;
            glfs_free_t                  gl_free;
            glfs_mkdir_t                 gl_mkdir;
            glfs_h_extract_handle_t      gl_h_extract_handle;
            glfs_h_lookupat_t            gl_h_lookupat; 
            glfs_dup_t                   gl_dup; 
            glfs_unlink_t                gl_unlink; 
        };

        class gfapi_fops
        {
            bool ready;
            void *handle;
            std::string lib;
            struct libgfapi_fops fops;
            src::severity_logger<int> log; 
            int32_t log_level;
        
            private:
            void   init_fops (void);
            void * extract_symbol (std::string);   
            void   extract_all_symbols (void);
            void   dump_all_symbols (void);

            public: 
            gfapi_fops (std::string);
            ~gfapi_fops (void);
            struct libgfapi_fops & get_fops (void); 
        };
    }
}

#endif /* End of __GFAPI_FOPS_H__ */
