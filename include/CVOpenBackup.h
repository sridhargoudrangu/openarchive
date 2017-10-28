/******************************************************************************/
/*  Copyright (c) CommVault Systems                                           */
/*  All Rights Reserved                                                       */
/*                                                                            */
/*  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF CommVault Systems          */
/*  The copyright notice above does not evidence any                          */
/*  actual or intended publication of such source code.                       */
/*                                                                            */
/*  File name   : CVOpenBackup.h                                              */
/*                                                                            */
/*  Description : Defines the Simpana Open Backup API                         */
/*                                                                            */
/*  Author      : Dmitriy Zakharkin                                           */
/*                                                                            */
/******************************************************************************/
#if !defined(_CVOPENBACKUP_H_306138BF_990F_473F_9F9A_5814886EEC95)
#define _CVOPENBACKUP_H_306138BF_990F_473F_9F9A_5814886EEC95

#if defined(_MSC_VER)
#pragma once
#endif // _MSC_VER

class CCommandLineArguments;


#if !defined(_MSC_VER) || defined(CVOPENBACKUP_LIB)
#define CVOPENBACKUP_CVOPENBACKUP_API 
#elif defined(CVOPENBACKUP_EXPORTS)
#define CVOPENBACKUP_CVOPENBACKUP_API __declspec(dllexport)
#else
#define CVOPENBACKUP_CVOPENBACKUP_API __declspec(dllimport)
#endif

/*
 * Data format:
 * 	Assumes that a file will consist of <header>[metadata]...<data><eof>, where metadata could be n entries
 * 	of blob of data that the caller is responsible for translation that each metadata will have a
 * 	designation either by number or string.
 * 	There could be predefined metadata that can accommodate ACL information so that non-fs oriented data
 * 	can store security information and the format of such shall be defined.
 *
 * Indexing usage:
 * 	The incremental indexing mode will be used in general since there won't be any scan phase nor the job level.
 *
 * Interface:
 * 	Have added some additional parameters while writing this down so please review and modify as
 * 	it suits your need. The bottom line is we need to publish the interface first as soon as possible.
 *
 */


#include <time.h>
#if defined(UNIX)
#include <inttypes.h>
#endif // UNIX

// Opaque structures
class CVOB_session_t;
class CVOB_hJob;
class CVOB_hStream;
struct CVOB_hError {};
class CVOB_hItem;

#ifdef __cplusplus
extern "C" {
#endif

#define CVOB_MAX_BUFFER_SIZE (64*1024)

typedef uint32_t  CVOB_MetadataID;

typedef enum {
	CVOB_ItemType_file,        // Item containing a data. For example file.
	CVOB_ItemType_container,   // Item containing other items. For example folder.
	CVOB_ItemType_link         // Item which is a link to another item. The metadata will have link target information
} CVOB_ItemType;

typedef enum {
	CVOB_ItemStatus_Good,      // Item is backed up
	CVOB_ItemStatus_Failed,    // Item failed to backed up
	CVOB_ItemStatus_Deleted,   // Item was deleted
	CVOB_ItemStatus_NotFound   // Item not found
} CVOB_ItemStatus;

typedef enum {
	CVOB_JobStatus_Success=0,      // Item is backed up
	CVOB_JobStatus_Failed,    // Item failed to backed up
} CVOB_JobStatus;

typedef enum {
	CVOB_JobType_Backup,       // Backup job
	CVOB_JobType_Restore       // Restore job
} CVOB_JobType;

// Job Options bit flags
#define CVOB_FULLBKP          0x0001
#define CVOB_RESTARTJOB       0x0010
#define CVOB_SKIPCREATEINDEX  0x0020
#define CVOB_FORCECREATEINDEX 0x0040
#define CVOB_FORONDEMANDBKP 0x0080
#define CVOB_JOB_FAILED		0x0100
#define CVOB_JOB_SUCCESS	0x0200


/*
 * query using job id, or 0
 * query all backup items on datetime time, or 0
 * if both, jobId and datetime are set to 0, result will include all up-to-date items
 */
typedef struct {
	int64_t    jobId;          // Job ID
	time_t     datatime_from;  // GTM+0 date-time
	time_t     datatime_to;    // GTM+0 date-time
	int16_t    includeDeleted; // 1 - include deleted items in the report
	int16_t    includeFailed;  // 1 - include failed items in the report
	char*      filter;         // filter expression to filter the result
	int32_t    reserve_1;
	int32_t    reserve_2;
	int32_t    reserve_3;
	int32_t    reserve_4;
	char*      reserve_a;
	char*      reserve_b;
	char*      reserve_c;
	char*      reserve_d;
} CVOB_BackupItemsQuery_t;

/*
 * Additional backup item attributes.
 * These attributes will be stored in file header
 */
typedef struct {
    uint64_t mode;
    uint64_t uid;
    uint64_t gid;
    uint64_t cTime;
    uint64_t mTime;
    uint64_t aTime;
    uint64_t flags; // == platformFlags
	uint64_t restartOffset;
} CVOB_BackupItemAttributes_t;

/*
* getItemMetadata_f called to the client layer to populate the a_type, a_buffer and a_bufferSize with meta data information.
* When all data for this metadata is processed, the a_bufferSize must be set to 0.
*
* Return
* 		- 0 when a_bufferSize is 0 - no more metadata, otherwise indicate successful call
* 		- 1 has additional metadata to send (valid only when a_bufferSize is 0)
* 		- <0 failure
*/
typedef int32_t(*getItemMetadata_f)( 
	void*            a_context,   // item context
	CVOB_MetadataID* a_typeId,    // pointer to metadata type ID
	char*            a_buffer,    // pointer to raw data buffer (CVOB_MAX_BUFFER_SIZE max)
	uint32_t*        a_bufferSize // pointer to buffer size
	);

/*
 * getItemData_f called to the client layer to populate the a_buffer and a_bufferSize with item's data information.
 * When all data is processed, the a_bufferSize must be returned with 0.
 *
Return 0 for success, <0 in case of failure.
 */
typedef int32_t(*getItemData_f)( 
	void*     a_context,     // item context
	char*     a_buffer,      // raw data buffer (CVOB_MAX_BUFFER_SIZE max)
	uint32_t* a_bufferSize   // buffer size
	);

typedef int32_t(*onGuidAssigned_f)(
	void*       a_context,     // item context
	const char* a_appGuid,     // guid provided by application
	const char* a_guid         // guid assigned by CommVault
	);

/*
 * onItemHeader_f called to the client layer with the header information for restoring item.
 * Client layer should initialize for restore.
 *
Return 0 for success, otherwise failure.
 */
typedef int32_t(*onItemHeader_f)( 
	void*         a_context, // item context
	const char*   a_name,    // item full name during backup (UNIX style path)
	const char*   a_guid,    // guid associated with the item
	CVOB_ItemType a_type,    // item type
	CVOB_BackupItemAttributes_t* a_attr, // Optionl attributes
	int64_t       a_dataSize // expected data size
	);

/*
 * onItemMetadate_f called to the client layer to pass data with metadata.
 * The metadata is complete when client is called with a_bufferSize set to 0.
 *
Return 0 for success, otherwise failure.
 */
typedef int32_t(*onItemMetadate_f)( 
	void*           a_context,    // item context
	CVOB_MetadataID a_type,       // metadata type ID
	char*           a_buffer,     // raw data buffer
	size_t          a_bufferSize  // buffer size
	);

/*
 * onItemData_f called to the client layer to store item data.
 * The data is complete when client is called with a_bufferSize set to 0.
 *
Return 0 for success, otherwise failure.
 */
typedef int32_t(*onItemData_f)( 
	void*  a_context,     // item context
	char*  a_buffer,      // raw data buffer (CVOB_MAX_BUFFER_SIZE max)
	size_t a_bufferSize   // buffer size
	);

/*
 * onItemEof_f called to the client layer when all data has been received and it will be no more communication for this item.
 * Client code can close all active handlers or connections associated with this item.
 *
Return 0 for success, otherwise failure.
 */
typedef int32_t(*onItemEof_f)( 
	void* a_context,      // item context
	int32_t a_success     // 0 - success, 1 - failure (need to discard)
	);

/*
 * contentItem_f called to obtain list of objects (path) that currently exist on a client.
 * Objects which are not in this list will be marked deleted in index.
 * This function will be called multiple times until it return 0.
 *
Return 0 after all items was reproted, >0 the number of objects in a_items, and <0 failure detected.
 */
typedef int32_t(*contentItem_f)( 
	void* a_context,       // context pointer
	const char** a_items[]  // pointer to the array of path 
	);

/*
 * backupItemInfo_f called to report item
 *
Return 0 for success, otherwise failure.
 */
typedef int32_t(*backupItemInfo_f)(
	void*         a_context,  // context pointer
	const char*   a_itemPath, // item full name (UNIX style path)
	const char*   a_guid,     // guid associated with the item
	const char*   a_CVguid,   // Simpana GUID which must be use to restore an item
	CVOB_ItemType a_type,     // item type
	int64_t       a_dataSize  // data size
	);


/*
 * reportError_f will be called to notify client about the errors.
 * Client code can report this error to the local log file.
 */
typedef void(*reportError_f)( 
	void*       a_context,  // context pointer
	int32_t     a_errno,    // error number
	const char* a_errString // error message
	);

/*
 * Definition of function pointers to be used during backup.
 */
typedef struct {
	getItemMetadata_f   getMetadata; // function pointer to populate metadata buffer during backup
	getItemData_f       getData;     // function pointer to populate data buffer during backup
	onGuidAssigned_f    objectGuid;  // function pointer to call client to notify GUID value assigned to the backup item
} CVOB_BackupCallbackInfo_t;

/*
 * Definition of function pointers to be used during restore.
 */
typedef struct {
	onItemHeader_f      onHeader;   // function pointer to send item header to application
	onItemMetadate_f    onMetadata; // function pointer to send metadata to application
	onItemData_f        onData;     // function pointer to send data to application 
	onItemEof_f         onEof;      // function pointer to indicate end of item
} CVOB_RestoreCallbackInfo_t;

/*
 * Certificate
 */
typedef struct
{
	const char* clientName;          // client name associated with Application on CommCell
	const char* cretificateLocation; // directory path where certificates are storred
	const char* privateKeyLocation;  // private key. If NULL, the default public/private pair will be used/created.
	const char* publicKeyLocation;   // public key. If NULL, the default public/private pair will be used/created.
} CertificateInfo_t;


/*
 * session Init2 response
 */
typedef struct
{
	int32_t a_commCellId;
	int32_t a_clientId;
	int32_t a_instanceId;
	int32_t a_backupsetId;
	int32_t a_appId;
} ClientInfo_t;

/**********************************************************************
CVOB_Init will create a new session object and connect to services on 
specified proxy host. The connection will be authenticated using a_cert - a SSL
certificate assigned to TBD. The authenticated connection with CVD 
will be kept for duration of the session.

If no activities noticed for the session for TBD, the session will 
be closed from proxy machine and any running operations will canceled. 
If backup job was running, the job will be TBD.

The application should use a session handler obtained from this call 
for other API calls where required.

This call will established certificate signing if this is a first call 
and perform certificate rotation when certificate is about to expire. 
The API will automatically reconnect with a new certificate when it was 
successfully updated. 

When certificate has been expired, the new certificate will be created 
and signed. The Application would have to provide a hand-deliver temporary 
Simpana-issued certificate or the build-in certificate can be used to 
perform signing unless Simpana is running in lock-down mode.

If session can't be established or another issue was detected while session
was created an error code will be returned to the Application and error 
object created when a_error is set. 
 
Return 0 for success, non 0 value is failure.

ERR_INVALID_CA
ERR_CA_EXPIRED
ERR_CA_REVOKED
ERR_CA_DENIED
ERR_INVALID_SESSION
ERR_OUT_OF_MEMORY
ERR_INVALID_HSTREAM
ERR_BAD_STREAM
-	host name of the proxy host can't be resolved
-	cannot connect to a_proxyhost
-	a_commCellId invalid value
-	a_clientId invalid value
-	a_appId invalid value
-	a_appType invalid value
-	failure while registering session on a proxy server

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_Init(
	const CertificateInfo_t* a_cert,          // certificate info associated with client
	const char*              a_proxyhost,     // host name or IP of the proxy machine
	int16_t                  a_proxyPort,     // port number for proxy CVD service
	int32_t					 a_proxyClientId, // id of the proxy client
	int32_t                  a_commCellId,    // CommCell ID
	int32_t                  a_clientId,      // client id
	int32_t                  a_appId,         // application id
	int32_t                  a_appType,       // application type
	const char*              a_configuration, // pointer to a string containing API configuration (can be NULL)
	reportError_f            a_reportError,   // callback handler to report error back to application
	CVOB_session_t**         a_session,       // [out] session object
	CVOB_hError**            a_error=NULL     // [out] pointer to store a new error handler object (can be NULL)
	);


/**********************************************************************
CVOB_Init2 will create a new session object and connect to services on 
specified proxy host. The connection will be authenticated using a_cert - a SSL
certificate assigned to TBD. The authenticated connection with CVD 
will be kept for duration of the session.

If no activities noticed for the session for TBD, the session will 
be closed from proxy machine and any running operations will canceled. 
If backup job was running, the job will be TBD.

The application should use a session handler obtained from this call 
for other API calls where required.

This call will established certificate signing if this is a first call 
and perform certificate rotation when certificate is about to expire. 
The API will automatically reconnect with a new certificate when it was 
successfully updated. 

When certificate has been expired, the new certificate will be created 
and signed. The Application would have to provide a hand-deliver temporary 
Simpana-issued certificate or the build-in certificate can be used to 
perform signing unless Simpana is running in lock-down mode.

If session can't be established or another issue was detected while session
was created an error code will be returned to the Application and error 
object created when a_error is set. 

This is enhanced from CVOB_Init api where it uses named entities for client,
instance, backupset etc. Other functionality is same as the other API
 
Return 0 for success, non 0 value is failure.

ERR_INVALID_CA
ERR_CA_EXPIRED
ERR_CA_REVOKED
ERR_CA_DENIED
ERR_INVALID_SESSION
ERR_OUT_OF_MEMORY
ERR_INVALID_HSTREAM
ERR_BAD_STREAM
-	host name of the proxy host can't be resolved
-	cannot connect to a_proxyhost
-	a_commCellId invalid value
-	a_clientId invalid value
-	a_appId invalid value
-	a_appType invalid value
-	failure while registering session on a proxy server

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_Init2(
	const CertificateInfo_t* a_cert,          // certificate info associated with client
	const char*              a_proxyhost,     // host name or IP of the proxy machine
	int16_t                  a_proxyPort,     // port number for proxy CVD service
	const char*			 	 a_clientName,	  // source clientName with which backup or restore needs to be associated
	int32_t					 a_appType,		  // Default is FS application type. Ex: distributed apptype (64)
	reportError_f            a_reportError,   // callback handler to report error back to application
	ClientInfo_t*		     a_clientInfo,	  // [out] API response returns the commcellId, clientId and appId
	CVOB_session_t**         a_session,       // [out] session object
	const char*              a_configuration, // pointer to a string containing API configuration (can be NULL)
	const char* 			 a_instanceName,  //If not passed will get the default instance name for that client
	const char* 			 a_backupsetName, //If not passed will get the default backupsetName for that instance
	const char* 			 a_subclientName, //If not passed will get the default subclientName for that subclient
	const char*			 	 a_securityToken,// security token for client authentication
	int32_t					 a_clientTimeOut=120,		
	int32_t					 a_proxyClientId=0,// id of the proxy client
	const char *			 a_nameValuePairList=NULL,
	CVOB_hError**            a_error=NULL     // [out] pointer to store a new error handler object (can be NULL)
	);


/**********************************************************************
The session is closed when Application called CVOB_Deinit. This call
will verify that there is no active job or request associated with this session. 

When this method is called with a_forceClose set to 1, it will gracefully
cancel all operations (if job is running, it will be TBD), close any
active streams and jobs, and disconnect from the CVD.

If error is detected or a_forceClose set to 0 and there are active
streams or job, an error will be returned.

Return 0 for success, non 0 value is failure.

ERR_INVALID_SESSION
ERR_BAD_SESSION
ERR_BUSY_JOB
ERR_BUSY_STREAM

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_Deinit(
	CVOB_session_t* a_session,    // current session
	int16_t         a_forceClose, // 1 to cancel active operations and close all active handers
	CVOB_hError**   a_error=NULL  // [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Application will call this function to start a new backup job. By creating a job,
the Application can associate multiple streams with a single job and backup
objects simultaneously.

The Application can request reserve a_numOfStreams. The reservation will
be executed depending on available resources.

The Application must be configured on a CommCell to support multiple
streams. The number of streams cannot exceed configured value.

Return 0 for success, non 0 value is failure.

ERR_INVALID_SESSION
ERR_BAD_SESSION
ERR_JOB_CNT_LIMIT
ERR_INVALID_HJOB
ERR_INVALID_JOBID
ERR_OUT_OF_MEMORY

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_StartJob(
	CVOB_session_t* a_session,      // current session
	int16_t         a_numOfStreams, // number of streams to reserve
	int32_t         a_options,      // options (bit flags)
	CVOB_JobType    a_jobType,      // job type: backup or restore
	const char*     a_jobToken,     // job token (must be provided) if job already started by JM) or NULL
	CVOB_hJob**     a_hJob,         // [out] job object
	uint32_t*       a_jobID,        // [out] job number. Use 0 to register a new job. Non-zero value will be used to attach to exiting job.
	CVOB_hError**   a_error=NULL    // [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Will finish the job. All active streams will be forcefully closed.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HJOB
ERR_BAD_JOB
ERR_JOB_OBJ_INVAL

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_EndJob(
	CVOB_hJob*    a_hJob,      		// job object
	CVOB_hError** a_error=NULL, 	// [out] pointer to store a new error handler object (can be NULL)
	int32_t       a_options=0      // job options (optional) used to send application exit status to proxy
	);


/**********************************************************************
Will start a stream in the given job and session.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HJOB
ERR_BAD_JOB
ERR_JOB_OBJ_INVAL
ERR_INVALID_HSTREAM
ERR_OUT_OF_MEMORY

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_StartStream(
	CVOB_hJob*     a_hJob,       // job object
	CVOB_hStream** a_stream,     // [out] stream object
	CVOB_hError**  a_error=NULL  // [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Will close the stream.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HJOB
ERR_BAD_JOB
ERR_JOB_OBJ_INVAL
ERR_INVALID_HSTREAM
ERR_BAD_STREAM

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_EndStream(
	CVOB_hStream* a_stream,    // stream object
	CVOB_hError** a_error=NULL // [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Backup an object using callback mechanism.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HSTREAM
ERR_BAD_STREAM
ERR_OUT_OF_MEMORY
ERR_CVEXCEPTION
ERR_MSG_SERIALIZER
ERR_MSG_SEND
ERR_INTERFACE_ERROR

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_SendItem(
	CVOB_hStream*                a_stream,   // stream object                                                           
	void*                        a_context,  // client provided pointer to the object associated with this backup item
	const char*                  a_name,     // item full name (UNIX style path)                                        
	CVOB_ItemType                a_type,     // item type                                                               
	const char*                  a_guid,     // guid associated with the item
	char*                        a_cvguid,   // assigned guid (must be at least 255 length)
	int64_t                      a_dataSize, // expected data size
	CVOB_BackupItemAttributes_t* a_attr,     // Optionl attributes (can be NULL)
	CVOB_BackupCallbackInfo_t*   a_callback, // callback functions to be called during backup                        
	CVOB_hError**                a_error=NULL// [out] pointer to store a new error handler object (can be NULL)       
	);

/**********************************************************************
Backup an object using direct calls.
**/


/**********************************************************************
Begin backup an object using direct calls.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HSTREAM
ERR_BAD_STREAM
ERR_OUT_OF_MEMORY
ERR_CVEXCEPTION
ERR_MSG_SERIALIZER
ERR_MSG_SEND
ERR_INTERFACE_ERROR

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_SendItemBegin(
	CVOB_hStream*                a_stream,   // stream object
	CVOB_hItem**                 a_item,     // [out] item object associated with this backukp item
	const char*                  a_name,     // item full name (UNIX style path)
	CVOB_ItemType                a_type,     // item type
	const char*                  a_guid,     // guid associated with the item
	char*                        a_cvguid,   // assigned guid (must be at least 255 length)
	int64_t                      a_dataSize, // expected data size
	CVOB_BackupItemAttributes_t* a_attr,     // Optionl attributes (can be NULL)
	CVOB_hError**                a_error=NULL// [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Send meta data using direct calls.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HSTREAM
ERR_BAD_STREAM
ERR_OUT_OF_MEMORY
ERR_CVEXCEPTION
ERR_MSG_SERIALIZER
ERR_MSG_SEND
ERR_INTERFACE_ERROR

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_SendMetadata(
	CVOB_hStream*     a_stream,      // stream object
	CVOB_hItem*       a_item,        // item object associated with this backup item
	CVOB_MetadataID   a_type,        // metadata type ID
	const char*       a_buffer,      // pointer to raw data buffer
	size_t            a_bufferSize,  // pointer to buffer size
	CVOB_hError**     a_error=NULL   // [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Send data using direct calls.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HSTREAM
ERR_BAD_STREAM
ERR_OUT_OF_MEMORY
ERR_CVEXCEPTION
ERR_MSG_SERIALIZER
ERR_MSG_SEND
ERR_INTERFACE_ERROR

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_SendData(
	CVOB_hStream*     a_stream,   // stream object
	CVOB_hItem*       a_item,     // item object associated with this backup item
	const char*       a_data,     // pointer to raw data buffer
	int64_t           a_dataSize, // pointer to buffer size
	CVOB_hError**     a_error=NULL// [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Finish sending data and close the item using direct calls.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HSTREAM
ERR_BAD_STREAM
ERR_OUT_OF_MEMORY
ERR_CVEXCEPTION
ERR_MSG_SERIALIZER
ERR_MSG_SEND
ERR_INTERFACE_ERROR

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_SendEnd(
	CVOB_hStream*     a_stream,    // stream object
	CVOB_hItem*       a_item,      // item object associated with this backup item
	CVOB_ItemStatus   a_status,    // status associated with this item
	const char*       a_statusMsg, // attach this message to the item to specify failure reason. Used when a_status is not CVOB_ItemStatus_Good.
	CVOB_hError**     a_error=NULL // [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Request Commit operation for backup objects.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HSTREAM
ERR_BAD_STREAM
ERR_MSG_SEND

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_CommitStream(
	CVOB_hStream*     a_stream,          // stream object
	void*             a_context,         // client provided pointer to the object associated with this backup item
	backupItemInfo_f  a_callback_func,   // callback functions to be called to notify commit objects
	CVOB_hError**     a_error=NULL       // [out] pointer to store a new error handler object (can be NULL)
	);

/**********************************************************************
Will return the list of protected items.

Return 0 for success, non 0 value is failure.

ERR_INVALID_SESSION
ERR_BAD_SESSION
ERR_MSG_SEND

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_GetBackuplist(
	CVOB_session_t*          a_session,         // current session
	void*                    a_context,         // client provided pointer to the object associated with this backup item
	CVOB_BackupItemsQuery_t* a_query,           // query arguments       
	backupItemInfo_f         a_callback_func,   // callback function to notify application about missing items.
	CVOB_hError**            a_error=NULL       // [out] pointer to store a new error handler object (can be NULL)       
	);

/**********************************************************************
Mark item as deleted based on logical name, GUID, and job id.

Return 0 for success, non 0 value is failure.

ERR_INVALID_SESSION
ERR_BAD_SESSION
ERR_MSG_SEND

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_MarkDeleted(
	CVOB_session_t* a_session,   // stream object                                                                                                                       
	const char*     a_name,      // item logical name (UNIX style path /cont1/cont2/cont3/item )                                                                        
	const char*     a_guid,      // GUID associated with the item. If NULL, all versions of the a_name will be deleted.                                                 
	CVOB_hError**   a_error=NULL // [out] pointer to store a new error handler object (can be NULL)       
	);

/**********************************************************************
Send content list to verify content.
Items which are previously backed up and not reported during this call will be marked deleted in index.
Items which are reported with this call, but do not recorded in index will be reported with itemCallback_func.
Application can use this call to (a) purge the removed items, and (b) verify backup and backup missing items.
If flag a_pruneDeletedObjects is set to 1, the object will be marked as delete and will be pruned based on policy rules.
If operation failed or interrupted, no items will be marked for deletion.

Return 0 for success, non 0 value is failure.

ERR_INVALID_SESSION
ERR_BAD_SESSION
ERR_MSG_SEND

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_SendContentList(
	// TBD should we use session or job?
	CVOB_hJob*       a_hJob,                   //     
	void*            a_context,                // application provided pointer to the object associated with this operation                                                         
	int16_t          a_pruneDeletedObjects,    // mark objects found in index but not in the content list as deleted.                                                               
	contentItem_f    a_contentCallback_func,   // callback function to get list of items to verify index                                                                            
	backupItemInfo_f a_itemCallback_func,      // callback function to notify application about missing items.                                                                      
	CVOB_hError**    a_error=NULL              // [out] pointer to store a new error handler object (can be NULL)       
	);


/**********************************************************************
Restore item from the backup based on a_guid

Return 0 for success, non 0 value is failure.

ERR_INVALID_SESSION
ERR_BAD_SESSION
ERR_MSG_SEND

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_RestoreObject(
	CVOB_hJob*                  a_hJob,           // current job object
	void*                       a_context,        // client provided pointer to the object associated with this restore item
	const char*                 a_pathOrGuid,     // Path or GUID reported with backupItemInfo_f call.
	uint64_t                    a_offset,         // offset from where start restore (0 from beginning)                     
	uint64_t                    a_length,         // length of data to be restore (0 for all data)                          
	CVOB_RestoreCallbackInfo_t* a_callbackInfo,   // callback functions to be called during restore                         
	CVOB_hError**               a_error=NULL      // [out] pointer to store a new error handler object (can be NULL)       
	);

/**********************************************************************
Request list of committed items for the stream.

Return 0 for success, non 0 value is failure.

ERR_INVALID_HSTREAM
ERR_BAD_STREAM
ERR_MSG_SEND

**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_GetCommitedItems(
	CVOB_hStream*    a_stream,          // stream object                                                                                                                    
	void*            a_context,         // callback function to report an item                                                                                               
	backupItemInfo_f a_callback_func,   // application provided pointer to the object associated with this backup job                                                        
	CVOB_hError**    a_error=NULL       // [out] pointer to store a new error handler object (can be NULL)       
	);

int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_GetError( 
	CVOB_hError* a_error,  // error object
	int32_t*     a_errorCode,       // address of int32_t to populate error code (can be NULL)
	char*        a_errorString,     // buffer for error message (can be NULL)
	int32_t      a_errorStringSize  // size of the buffer for error message
	);

/**********************************************************************
Application must call this method when error object returned to the application
**/

void CVOPENBACKUP_CVOPENBACKUP_API CVOB_FreeError( 
	CVOB_hError* a_error  // error object
	);

/**********************************************************************
Applications must invoke this method to control the logging
**/
int32_t CVOPENBACKUP_CVOPENBACKUP_API CVOB_EnableLogging(
	const char*        a_loggingDirectory,  // Directory where log files are to be generated
	const char*        a_loggingFile,       // Name of the log file
	int32_t            a_loggingLevel       // Logging level
	);

#ifdef __cplusplus
}
#endif


#endif // _CVOPENBACKUP_H_306138BF_990F_473F_9F9A_5814886EEC95

