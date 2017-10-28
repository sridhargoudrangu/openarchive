/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <cfgparams.h>

namespace openarchive
{
    namespace cfgparams
    {
        /*
         * Constants that define the behaviour of archive store.
         */
        const std::string log_prefix = "archivestore"; /* prefix for logging */
        const std::string config_file = "/etc/archivestore.conf";

        /*
         * Configurable parameters that define the behaviour of archive store.
         * These config parameters have to be defined in the config file 
         * /etc/archivestore.conf
         */ 
        std::string log_dir = "/var/log/archivestore"; /* log directory */
        uint64_t rotation_size = 100*1024*1024; /* Max size of log files 16MB */
        uint64_t min_free_space = 500*1024*1024; /* Min free space on drive */
        int32_t log_level = 0; /* Current log level */
        uint64_t work_items = 128;   

        void extract_str (boost::program_options::variables_map &map,
                          std::string name, std::string &val)
        {
            if (map.count(name)) {
                val = map[name].as<std::string>();
            }
        }

        template<typename T> 
        void extract_val (boost::program_options::variables_map &map,
                          std::string name, T &val)
        {
            if (map.count(name)) {
                if (map[name].as<T>() > 0) {
                    val = map[name].as<T>();
                }
            } 
        }

        void parse_config_file (void)
        {
            boost::program_options::options_description archive_ops{"File"};
            archive_ops.add_options()
                       ("log_dir", boost::program_options::value<std::string>(),
                        "Log files directory")
                       ("rotation_size", boost::program_options::value<long>(), 
                        "Max size of log files")
                       ("free_space", boost::program_options::value<long>(), 
                        "Min free space on drive")
                       ("log_level", boost::program_options::value<int>(), 
                        "Current log level")
                       ("expand_val", boost::program_options::value<int>(), 
                        "Get request processing queue expansion factor")
                       ("flush_interval", boost::program_options::value<int>(), 
                        "Log entries flush frequency");  
            
            boost::program_options::variables_map var_map;
            std::ifstream inpstream { config_file.c_str() };
            if (inpstream) {
                store (parse_config_file (inpstream, archive_ops), var_map);
                notify (var_map);
  
                /*
                 * Extract the parameters from config file.
                 */
                extract_str (var_map, "log_dir", log_dir);
                extract_val (var_map, "rotation_size", rotation_size); 
                extract_val (var_map, "free_space", min_free_space);
                extract_val (var_map, "log_level", log_level);
            }
        }
        
        std::string get_log_dir         (void) { return (log_dir);         }
        std::string get_log_prefix      (void) { return (log_prefix);      }
        uint64_t    get_rotation_size   (void) { return (rotation_size);   }
        uint64_t    get_min_free_space  (void) { return (min_free_space);  }
        int         get_log_level       (void) { return (log_level);       }
        bool        create_fast_threads (void) { return true;              }
        bool        create_slow_threads (void) { return false;             }

        uint64_t    get_num_work_items (arch_op_type op_type) 
        { 
            switch (op_type)
            {
                case BACKUP:
                case RESTORE:
                case ARCHIVE:
                     return work_items; 
            }

            return 0;
        }

        bool        extent_based_backups (arch_loc_t &loc) 
        {
            /*
             * Based on the product id and store id determine whether extent
             * based backups can be performed.
             */    
            if (loc.get_product() == "glusterfs") {
                /*
                 * Check whether sharding is currently enabled on the volume by 
                 * extracting the volume property.
                 */
                std::string cmd = std::string("gluster volume get ") + 
                                 loc.get_store () + 
                                 " features.shard | tail -n1 | awk \'{print $2}\'";
                
                openarchive::arch_core::popen process (cmd, 100);

                if (process.is_valid ()) {

                    std::vector<std::string> & vec = process.get_output ();
                    if (vec.size()) {

                        if (vec[0] == "off") {
                            return (false);
                        } else {
                            return (true);
                        }
                        

                    }
                } 
            }
 
            return false;
        } 
    } /* namespace cfgparams */  
} /* namespace openarchive */
