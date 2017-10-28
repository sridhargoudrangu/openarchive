/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __CFGPARAMS_H__
#define __CFGPARAMS_H__

#include <fstream>
#include <string>
#include <boost/program_options.hpp>
#include <arch_core.h>
#include <arch_loc.h>

namespace openarchive
{
    namespace cfgparams
    {
        void        parse_config_file   (void);
        std::string get_log_dir         (void);
        std::string get_log_prefix      (void);
        uint64_t    get_rotation_size   (void);
        uint64_t    get_min_free_space  (void);
        int32_t     get_log_level       (void);
        bool        create_fast_threads (void);
        bool        create_slow_threads (void);
        uint64_t    get_num_work_items  (arch_op_type);
        bool        extent_based_backups (arch_loc_t &); 
    } /* namespace cfgparams */  
} /* namespace openarchive */
#endif
