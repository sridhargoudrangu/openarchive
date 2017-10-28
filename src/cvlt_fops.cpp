#include <cvlt_fops.h>

namespace openarchive
{
    namespace cvlt_fops
    {
        cvlt_fops::cvlt_fops (std::string name): ready(false), lib(name)
        {
            log_level = openarchive::cfgparams::get_log_level();
            init_fops(); 

            /*
             * Open libCVOpenBackup.so dll
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
            extract_all_symbols ();
            dump_all_symbols ();

            ready = validate_fops ();
            
        }

        cvlt_fops::~cvlt_fops (void)
        {
            if (handle) {
                dlclose (handle);
                handle = NULL; 
            }
        } 

        void cvlt_fops::init_fops (void)
        {
            fops.init             = NULL;
            fops.init2            = NULL;   
            fops.deinit           = NULL;
            fops.start_job        = NULL;
            fops.end_job          = NULL;
            fops.start_stream     = NULL;
            fops.end_stream       = NULL;
            fops.send_item        = NULL;
            fops.send_item_begin  = NULL;
            fops.send_metadata    = NULL;
            fops.send_data        = NULL; 
            fops.send_end         = NULL;
            fops.commit_stream    = NULL;
            fops.get_backuplist   = NULL;
            fops.mark_deleted     = NULL;
            fops.send_contentlist = NULL;
            fops.restore_object   = NULL;
            fops.get_commit_items = NULL;
            fops.get_error        = NULL;
            fops.free_error       = NULL;
            fops.enable_logging   = NULL;

            return;
        }  

        void* cvlt_fops::extract_symbol (std::string name)
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
       
        void cvlt_fops::dump_all_symbols (void)
        {
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Dumping all the symbols from " << lib;
            } 
                
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_Init:                 0x" 
                               << std::hex
                               << (uint64_t) fops.init; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_Init2:                0x" 
                               << std::hex
                               << (uint64_t) fops.init; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_Deinit:               0x" 
                               << std::hex
                               << (uint64_t) fops.deinit; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_StartJob:             0x" 
                               << std::hex
                               << (uint64_t) fops.start_job; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_EndJob:               0x" 
                               << std::hex
                               << (uint64_t) fops.end_job; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_StartStream:          0x" 
                               << std::hex
                               << (uint64_t) fops.start_stream; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_EndStream:            0x" 
                               << std::hex
                               << (uint64_t) fops.end_stream; 
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_SendItem:             0x" 
                               << std::hex
                               << (uint64_t) fops.send_item;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_SendItemBegin:        0x" 
                               << std::hex
                               << (uint64_t) fops.send_item_begin;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_SendMetadata:         0x" 
                               << std::hex
                               << (uint64_t) fops.send_metadata;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_SendData:             0x" 
                               << std::hex
                               << (uint64_t) fops.send_data;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_SendEnd:              0x" 
                               << std::hex
                               << (uint64_t) fops.send_end;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_CommitStream:         0x" 
                               << std::hex
                               << (uint64_t) fops.commit_stream;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_GetBackuplist:        0x" 
                               << std::hex
                               << (uint64_t) fops.get_backuplist;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_MarkDeleted:          0x" 
                               << std::hex
                               << (uint64_t) fops.mark_deleted;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_SendContentList:      0x" 
                               << std::hex
                               << (uint64_t) fops.send_contentlist;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_RestoreObject:        0x" 
                               << std::hex
                               << (uint64_t) fops.restore_object;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_GetCommitedItems:     0x" 
                               << std::hex
                               << (uint64_t) fops.get_commit_items;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_GetError:             0x" 
                               << std::hex
                               << (uint64_t) fops.get_error;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_FreeError:            0x" 
                               << std::hex
                               << (uint64_t) fops.free_error;
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " CVOB_EnableLogging:        0x" 
                               << std::hex
                               << (uint64_t) fops.enable_logging;
            } 

            return;
        }
 
        void  cvlt_fops::extract_all_symbols (void)
        {
            void *fptr;

            fptr = extract_symbol ("CVOB_Init");
            fops.init = (cvob_init_t) fptr;

            fptr = extract_symbol ("CVOB_Init2");
            fops.init2 = (cvob_init2_t) fptr;

            fptr = extract_symbol ("CVOB_Deinit");
            fops.deinit = (cvob_deinit_t) fptr;

            fptr = extract_symbol ("CVOB_StartJob");
            fops.start_job = (cvob_startjob_t) fptr;

            fptr = extract_symbol ("CVOB_EndJob"); 
            fops.end_job = (cvob_endjob_t) fptr;

            fptr = extract_symbol ("CVOB_StartStream");
            fops.start_stream = (cvob_startstream_t) fptr;
 
            fptr = extract_symbol ("CVOB_EndStream"); 
            fops.end_stream = (cvob_endstream_t) fptr;

            fptr = extract_symbol ("CVOB_SendItem"); 
            fops.send_item = (cvob_senditem_t) fptr;

            fptr = extract_symbol ("CVOB_SendItemBegin");
            fops.send_item_begin = (cvob_senditem_begin_t) fptr;
 
            fptr = extract_symbol ("CVOB_SendMetadata");
            fops.send_metadata = (cvob_sendmetadata_t) fptr;
 
            fptr = extract_symbol ("CVOB_SendData");
            fops.send_data = (cvob_senddata_t) fptr;
 
            fptr = extract_symbol ("CVOB_SendEnd");
            fops.send_end = (cvob_sendend_t) fptr;
 
            fptr = extract_symbol ("CVOB_CommitStream");
            fops.commit_stream = (cvob_commitstream_t) fptr;
 
            fptr = extract_symbol ("CVOB_GetBackuplist");
            fops.get_backuplist = (cvob_getbackuplist_t) fptr;
 
            fptr = extract_symbol ("CVOB_MarkDeleted");
            fops.mark_deleted = (cvob_mark_deleted_t)  fptr;
 
            fptr = extract_symbol ("CVOB_SendContentList");
            fops.send_contentlist = (cvob_send_contentlist_t) fptr;
 
            fptr = extract_symbol ("CVOB_RestoreObject");
            fops.restore_object = (cvob_restore_object_t) fptr;
 
            fptr = extract_symbol ("CVOB_GetCommitedItems");
            fops.get_commit_items = (cvob_get_commit_items_t) fptr;
 
            fptr = extract_symbol ("CVOB_GetError");
            fops.get_error = (cvob_get_error_t) fptr;
 
            fptr = extract_symbol ("CVOB_FreeError");
            fops.free_error = (cvob_freeerror_t) fptr;
 
            fptr = extract_symbol ("CVOB_EnableLogging");
            fops.enable_logging = (cvob_enable_logging_t) fptr; 

            return;

        }

        bool cvlt_fops::validate_fops (void)
        {
            if (fops.init             &&
                fops.init2            && 
                fops.deinit           &&
                fops.start_job        &&
                fops.end_job          &&
                fops.start_stream     &&
                fops.end_stream       &&
                fops.send_item        &&
                fops.send_item_begin  &&
                fops.send_metadata    &&
                fops.send_data        &&
                fops.send_end         &&
                fops.commit_stream    &&
                fops.get_backuplist   &&
                fops.mark_deleted     &&
                fops.restore_object   &&
                fops.get_commit_items &&
                fops.get_error        &&
                fops.free_error       &&
                fops.enable_logging) {
                return true;
            } else {
                return false;
            }
        } 

        struct libcvob_fops & cvlt_fops::get_fops (void)
        {
            return fops;
        }
    }
}
