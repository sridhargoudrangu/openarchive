/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <logger.h>
#include <cfgparams.h>

namespace openarchive
{
    namespace logger
    { 
        #define timeattr  expr::attr< boost::posix_time::ptime >
        #define pidattr   expr::attr< mt_attrs::current_process_id::value_type >
        #define tidattr   expr::attr< mt_attrs::current_thread_id::value_type >
        #define decorattr expr::xml_decor[ expr::stream << expr::smessage ]
        #define sevattr   expr::attr<boost::log::trivial::severity_level>
        #define scopeattr expr::format_named_scope 
        #define keyfmt    boost::log::keywords::format
        #define keyiter   boost::log::keywords::iteration
        #define keydepth  boost::log::keywords::depth
        #define formatter boost::log::formatter
       
          

        bool check_loglevel (const boost::log::attribute_value_set &attr_set)
        {
            return (openarchive::cfgparams::get_log_level() >= 
                    attr_set["Severity"].extract<int>());
        }

        void logger::set_log_rotation (std::string dir, uint max_size,
                                       uint min_free_space) 
        {
            sink->locked_backend()->set_file_collector(\
                sinks::file::make_collector (
                    /* 
                     * Target directory 
                     */
                    keywords::target = dir, 
                    /* 
                     * Max total size of files on the drive, in bytes 
                     */
                    keywords::max_size = max_size, 
                    /* 
                     * Minimum free space on the drive, in bytes 
                     */
                    keywords::min_free_space = min_free_space 
                )
            );
        }
                    
        void logger::init_logger (std::string dir, std::string log_prefix,
                                  uint rotation_size, uint min_free_space)
        {
            std::string log_file_name = dir+"/"+log_prefix;
            log_file_name.append ("_%Y%m%d_%H%M%S_%5N.log");

            sink.reset (new file_sink (
                        keywords::file_name = log_file_name,
                        keywords::rotation_size = rotation_size));

            /*
             * Upon restart, scan the directory for files 
             * matching the file_name pattern
             */
            sink->locked_backend()->auto_flush(true);  
            set_log_rotation(dir, rotation_size, min_free_space);
            sink->locked_backend()->scan_for_files();

            auto fmttime     = timeattr  ("TimeStamp");
            auto fmtpid      = pidattr   ("ProcessID");
            auto fmttid      = tidattr   ("ThreadID");
            auto fmtscope    = scopeattr ("Scope", 
                                          keyfmt  = "[ %C @ %F:%l ]",
                                          keyiter = expr::reverse,
                                          keydepth = 2);
  
            formatter logfmt = expr::format ("%1% %2% %3% %4% %5%")
                                             % fmttime 
                                             % fmtpid
                                             % fmttid
                                             % fmtscope
                                             % decorattr;
                                              
            sink->set_formatter (logfmt); 
            sink->set_filter (&check_loglevel);
            
            /*
             * Add the sink to the core
             */
            logging::core::get()->add_sink(sink);

            /*
             * And also add some attributes
             */
            logging::core::get()->add_global_attribute("TimeStamp",
                                  attrs::utc_clock());
            logging::core::get()->add_global_attribute ("ProcessID", 
                                  attrs::current_process_id());
            logging::core::get()->add_global_attribute ("ThreadID",
                                  attrs::current_thread_id());
            logging::core::get()->add_global_attribute ("Scope",
                                  attrs::named_scope());

            return;
        }


        logger::logger (std::string dir, std::string log_prefix,
                        uint rotation_size, uint min_free_space)
        {
            init_logger (dir, log_prefix, rotation_size, min_free_space);
        }

        logger::~logger (void)
        {
            flush();
            
            /*
             * Remove the sink from the core
             */
            logging::core::get()->remove_sink(sink);
        }

        void logger::flush (void)
        {
            /*
             * Flush all the entries to the log file.
             */
            if (sink) { 
                sink->flush ();
            }
        }

        /*
         * Log a maximum of 100 functions.
         */
        const int stack_depth = 100;

        void get_backtrace (std::string &trace)
        {
            trace = "";
            void *buffer[stack_depth];
            char **symbols;
            int counter,num_symbols;

            num_symbols = backtrace (buffer, stack_depth);
            symbols = backtrace_symbols (buffer, num_symbols);
            if (symbols) {
                /*
                 * Discard the first index as it contains name of this function.
                 */
                for(counter = 1;counter < num_symbols;counter++) {
                    if (symbols[counter]) {
                        trace.append (symbols[counter]);
                        trace.append ("\n");
                        free (symbols[counter]);
                    }
                }
                free (symbols);
            }
        } 

    } /* namespace logger */
} /* namespace openarchive */
