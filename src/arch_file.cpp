/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <arch_file.h>
#include <arch_iopx.h>

namespace openarchive
{
    namespace arch_file
    {
        arch_file::arch_file (openarchive::arch_loc::arch_loc &al):loc(al),
                                                                   failed(false)
        {
            cbk_invoked.store (false);
        }

        arch_file::~arch_file (void)
        {
            if (iopx)
            {
                /*
                 * Invoke close on the iopx tree that was using this file 
                 * object.
                 */
                iopx->close (*this); 
            }
        } 
        
        void arch_file::set_file_info (std::string name, file_info_t & info)
        {
            openarchive::arch_core::spinlock_handle handle(lock);

            nvstore.insert (std::pair<std::string, file_info_t> (name, info));
        }

        bool arch_file::get_file_info (std::string name, file_info_t & ret)
        {
            std::map<std::string, file_info_t>::iterator iter;
            bool found = false;

            openarchive::arch_core::spinlock_handle handle(lock);

            iter = nvstore.find (name);
            if (iter != nvstore.end()) {
                ret = iter->second;
                found = true;
            } 
           
            return found;
        }

        void arch_file::erase_file_info (std::string name)
        {
            openarchive::arch_core::spinlock_handle handle(lock);

            nvstore.erase(name);
            return ;
        }
    } /* Namespace arch_file */
} /* Namespace open_archive */
