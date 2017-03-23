# Style guide:
# 1. Variable names are all lower case use _ to separate words (i.e. day_one)
# 2. Functions names are camel case, verbs i.e. (FiInstrumentGet)
# 3. Functions (methods) associated with S3 classes follow the naming convention ClassName.FunctionName(class_object_instance,...)


#' @title DataAccess: da_obj to connect and interact with the Solvas Capital database
#' @description 
#' DataAccess is a required object which always serves as the first parameter 
#' passed to any of the other functions.
#' @examples
#' \dontrun{
#' connection_string = "Driver={Sql Server};server=(local);trusted_connection=True;
#' database=Internal_Capital_DEV;"
#' sc_da <- DataAccess(connection_string_param=connection_string, fs_id_param=1)
#' DataAccess.FiInstrumentGet(sc_da, NULL, 1)
#' }
#' @param connection_string_param - The SQL Server connection string
#' @param fs_id_param - The scenario id to run against
#' @param event_id_param - Event Id of the for the scenario run (passed from the server or NULL for local development)
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


#' @title  Returns the current connection status
#' @description 
#' Takes a DataAccess da_obj and returns "success" if the connection is 
#' working otherwise the error message is returned.
#' @param da_obj - Current instance of Solvas|Capital's DataAccess class.
#' @return "success" if connection is valid otherwise an error message is returned
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

#' @title  Returns the connection status 
#' @description 
#' Takes a DataAccess da_obj and returns "success" if the connection is 
#' working otherwise the error message is returned.
#' @param da_obj - Current instance of Solvas|Capital's DataAccess class.
#' @return "success" if the version is compatible with the Solvas|Capital database server
#' @import RODBC
#' @export
DataAccess.PackageVersionCompatible <- function(da_obj) {
  return(Solvas.Capital.SqlUtility::SPPackageVersionCompatible(da_obj$connection))
}
  
#' @title Get Financial Instruments data frame as of an effective date or period
#' @description 
#' Takes a DataAccess da_obj and effective_date or effective_period and returns a data frame
#' with the instruments.  Properties that are schedules are coalesced 
#' to a single value based on the effective_date or effective_period. 
#' NOTE: EITHER effective_date or effective_period must be populated, the other one must be NULL
#' @param da_obj - Current instance of Solvas|Capital's DataAccess class. 
#' @param effective_date - The date to use for schedule data types
#' @param effective_period - The period to use for schedule data types (1=first period)
#' @return dataframe
#' @import RODBC
#' @export
DataAccess.FiInstrumentGet <- function(da_obj, effective_date = NULL, effective_period = NULL) {
 if (da_obj$is_init == 0)
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection,da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFIInstrumentGet(da_obj$connection,da_obj$fs_id, effective_date, effective_period))
}

#' @title Get Economic Assumptions data frame
#' @description 
#' Gets economic schedules from the transformation data 
#' @param da_obj - Current instance of Solvas|Capital's DataAccess class.
#' @param tm_desc - description of transformation (i.e. '(apply to all)').  the first
#' transformation by sequence order will be used if NULL, if two or more transformations have the
#' same description an error message is returned
#' @param use_dates - if TRUE then matrix columns will be dates, else periods. default to false 
#' @return dataframe
#' @export
DataAccess.FsAssumptionsGet <- function(da_obj, tm_desc = NULL, use_dates = FALSE) {
  if (da_obj$is_init == 0)
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection,da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFSAssumptionsGet(da_obj$connection, da_obj$fs_id, tm_desc = tm_desc, use_dates))
}

#' @title Gets the INITIAL Account Balance data frame for the scenario
#' @description 
#' Get initial account balances for the scenario.  The inital account balance is a direct copy
#' of the entity's account balances.
#' Returns a dataframe of all the initial account balances. The dataframe will contain all accounts 
#' and all dates for the reporting period.  NA will be used when no account balance exists.
#' 
#' @param da_obj - Current instance of Solvas|Capital's DataAccess class.
#' @param use_account_name if TRUE will use account_name, false will use account_number
#' @return dataframe
#' @export
DataAccess.FsInitalAccountBalanceGet <- function(da_obj, use_account_name = TRUE) {
  if (da_obj$is_init == 0)
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection, da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFSInitialAccountBalanceGet(da_obj$connection, da_obj$fs_id, use_account_name))
}


#' @title Updates the database Account Balances for the scenario
#' @description 
#' Saves the account balances back to the database.  The account_balances parameter should
#' be the same dataframe from FSInitalAccountBalanceGet. NOTE: This will remove any existing
#' account balances for this FS_ID and insert the new balances.
#' 
#' @param da_obj - Current instance of Solvas|Capital's DataAccess class.
#' @param account_balances - updated dataframe retured from DataAccess.FsInitalAccountBalanceGet
#' @export
DataAccess.FsAccountBalancePut <- function(da_obj, account_balances) {
  if (da_obj$is_init == 0)
    Solvas.Capital.SqlUtility::SPInstrumentPropertyLoad(da_obj$connection, da_obj$fs_id)
  da_obj$is_init = 1
  return(Solvas.Capital.SqlUtility::SPFSAccountBalancePut(da_obj$connection, da_obj$fs_id, da_obj$event_id, account_balances))
}