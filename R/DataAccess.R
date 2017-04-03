# Style guide:
# 1. Variable names are all lower case use _ to separate words (i.e. day_one)
# 2. Functions names are camel case, verbs i.e. (FiInstrumentGet)
# 3. Functions (methods) associated with S3 classes follow the naming convention ClassName.FunctionName(class_object_instance,...)


#' @title Access and interact with Solvas|Capital database (da_obj).
#' @description 
#' DataAccess is a required object which is always the first parameter passed to any of the other functions.
#' @examples
#' \dontrun{
#' connection_string = "Driver={Sql Server};server=(local);trusted_connection=True;
#' database=Internal_Capital_DEV;"
#' sc_da <- DataAccess(connection_string_param=connection_string, fs_id_param=1)
#' DataAccess.FiInstrumentGet(sc_da, NULL, 1)
#' }
#' @param connection_string_param -  SQL Server connection string
#' @param fs_id_param - Solvas|Capital scenario ID used in the run
#' @param event_id_param - Solvas|Capital event ID of the scenario run (passed from the server or NULL for local development).
#' @export
DataAccess <- function(connection_string_param = "", fs_id_param = NULL, event_id_param = NULL)
{
  # save da_obj variables
  me <- list(
    connection = connection_string_param,
    fs_id = fs_id_param,
    event_id = event_id_param,
    # locals
    is_init = 0
  )

  # set name for class
  class(me) <- append(class(me), "DataAccess")
  return(me)
}


#' @title  Check current database connection status
#' @description 
#' Takes a DataAccess da_obj and returns "success" if the connection is 
#' working or an error if the connection is invalid.
#' @param da_obj - current instance of Solvas|Capital's DataAccess class.
#' @return "success" confirms a valid connection, otherwise an error message is returned
#' @import RODBC
#' @export
DataAccess.ConnectionStatus <- function(da_obj) {
  return 
    tryCatch(
      {
        #cn <- odbcDriverConnect(da_obj$connection)
        #odbcClose(cn)
        if (Solvas.Capital.SqlUtility::SPPackageVersionCompatible(da_obj$connection) == FALSE)
          "package version is not compatible with the expected version on the server"
        else
          "success"
      } ,  
      warning = function(cond) paste("ERROR:  ", cond),
      error = function(cond)  paste("ERROR:  ", cond)	  
    )
}

#' @title  Check version compatibility. 
#' @description 
#' Takes a DataAccess da_obj and returns "success" if the connection is working, or an error if the version 
#' is incompatible.
#' @param da_obj - current instance of Solvas|Capital's DataAccess class.
#' @return "success" confirms the version is compatible with the Solvas|Capital database server, otherwise an error message is returned.
#' @import RODBC
#' @export
DataAccess.PackageVersionCompatible <- function(da_obj) {
  return(Solvas.Capital.SqlUtility::SPPackageVersionCompatible(da_obj$connection))
}
  
#' @title Retrieve financial instruments data frame from Solvas|Capital, as of the effective date or period
#' @description 
#' Takes a DataAccess da_obj and an effective_date or effective_period and returns a data frame
#' with the instruments.  Schedule properties are coalesced 
#' to a single value based on the effective_date or effective_period. 
#' NOTE: EITHER effective_date or effective_period must be populated, the other one must be NULL
#' @param da_obj - current instance of Solvas|Capital's DataAccess class. 
#' @param effective_date - effective date to use for schedule data types
#' @param effective_period - effective period to use for schedule data types (1=first period)
#' @return dataframe
#' @import RODBC
#' @export
DataAccess.FiInstrumentGet <- function(da_obj, effective_date = NULL, effective_period = NULL) {
 if (da_obj$is_init == 0)
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection,da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFIInstrumentGet(da_obj$connection,da_obj$fs_id, effective_date, effective_period))
}

#' @title Retrieve economic assumptions data frame from Solvas|Capital
#' @description 
#' Gets economic schedules from Solvas|Capital transformation data 
#' @param da_obj - current instance of Solvas|Capital's DataAccess class.
#' @param tm_desc - description of transformation (i.e. '(apply to all)'). The first transformation, by sequence 
#' order, will be used if NULL. If two or more transformations have the same description
#'  an error message is returned.
#' @param use_dates - if TRUE, matrix columns will be dates, else periods. Default to false.
#' @return dataframe
#' @export
DataAccess.FsAssumptionsGet <- function(da_obj, tm_desc = NULL, use_dates = FALSE) {
  if (da_obj$is_init == 0)
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection,da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFSAssumptionsGet(da_obj$connection, da_obj$fs_id, tm_desc = tm_desc, use_dates))
}

#' @title Retrieve initial account balances from Solvas|Capital
#' @description 
#' Retrieves initial account balances for the scenario. The initial account balance is a direct copy of the entity's 
#' account balance. Returns a data frame of all the initial account balances. The data frame contains all accounts and 
#' dates for the reporting period. NA is used when no account balance exists.
#'  
#' @param da_obj - current instance of Solvas|Capital's DataAccess class.
#' @param use_account_name - if TRUE, will use account_name, false will use account_number.
#' @return dataframe
#' @export
DataAccess.FsInitialAccountBalanceGet <- function(da_obj, use_account_name = TRUE) {
  if (da_obj$is_init == 0)
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection, da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFSInitialAccountBalanceGet(da_obj$connection, da_obj$fs_id, use_account_name))
}


#' @title Push final account balances to Solvas|Capital database.
#' @description 
#' Saves the account balances back to the Solvas|Capital database.  The account_balances parameter should
#' be the same dataframe from FSInitalAccountBalanceGet. NOTE: This will remove any existing
#' account balances for this FS_ID and insert the new balances.
#' 
#' @param da_obj - current instance of Solvas|Capital's DataAccess class.
#' @param account_balances - updated data frame returned from DataAccess.FsInitalAccountBalanceGet
#' @export
DataAccess.FsAccountBalancePut <- function(da_obj, account_balances) {
  if (da_obj$is_init == 0) 
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection, da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFSAccountBalancePut(da_obj$connection, da_obj$fs_id, da_obj$event_id, account_balances))
}