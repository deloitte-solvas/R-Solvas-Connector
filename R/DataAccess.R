# Style guide:
# 1. Variable names are all lower case use _ to separate words (i.e. day_one)
# 2. Functions names are camel case, verbs i.e. (FiInstrumentGet)
# 3. Functions (methods) associated with S3 classes follow the naming convention ClassName.FunctionName(class_object_instance,...)


#' @title Access and interact with Solvas|Capital database (da_obj).
#' @description 
#' The DataAccess object is a required object which is always the first parameter passed to any of the other functions. The 
#' DataAccess object is created by the SERVER script and is passed into the required entry point of the model.  The name of this 
#' required entry point is 'solvas_capital_model_run' and looks like this:\cr\cr
#'
#' solvas_capital_model_run <- function(sc_da) ...
#' 
#' When the model is run on the server the server generates a script, creates the 
#' DataAccess object and calls solvas_capital_model_run defined in the model associated with the scenario.
#'
#' When creating the DataAccessObject for local development the create_instruments can be set to true initially and the changed to 
#' to false after it is called once and the instruments are created.  This will save time not having to re-generate the instruments.
#' 
#' 
#' @examples
#' \dontrun{
#'For example: 
#'  
#' If the model to be uploaded is the following (minus header info):
#' 
#'  solvas_capital_model_run <- function(sc_da) {
#'    # insert model code i.e.
#'    DataAccess.FiInstrumentGet(sc_da, NULL, 1)
#'  }
#'  
#'  The server will generate something along these lines:
#'
#'  # the user model injected here:
#'  solvas_capital_model_run <- function(sc_da) {
#'    # user code
#'    DataAccess.FiInstrumentGet(sc_da, NULL, 1)
#'  }

#'  # end user model injection
#'  
#'  # start of script code to create the DataAccess object from the parameters and run 
#'  the solvas_capital_model_run
#'  
#'  sc_da <- DataAccess(connection_string_param = sc_connection_string, fs_id_param = sc_fs_id, event_id_param = sc_event_id, TRUE)
#'  solvas_capital_model_run(sc_da)
#'  
#' }
#' 
#' @param connection_string_param -  SQL Server connection string
#' @param fs_id_param - Solvas|Capital scenario ID used in the run
#' @param event_id_param - Solvas|Capital event ID of the scenario run (passed from the server or NULL for local development).
#' @param create_instruments - If true and the scenario has not been run this will create a 
#'      snapshot of instruments and properties for the passed fs_id_param
#' @export
DataAccess <- function(connection_string_param = "", fs_id_param = NULL, event_id_param = NULL, create_instruments = FALSE)
{
  # save da_obj variables
  me <- list(
    connection = connection_string_param,
    fs_id = fs_id_param,
    event_id = event_id_param,
    # locals - declare 'local' variables here.
    create_instruments_msg = NULL
  )
  # create the instruments on the server 
  if (create_instruments == TRUE) {
    me$create_instruments_msg = Solvas.Capital.SqlUtility::SPFSCreateInstruments(me$connection, me$fs_id)
  }


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
  return(Solvas.Capital.SqlUtility::SPFIInstrumentGet(da_obj$connection,da_obj$fs_id, effective_date, effective_period))
}

#' @title Retrieve schedule assumption data frame from Solvas|Capital
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
  return(Solvas.Capital.SqlUtility::SPFSAssumptionsGet(da_obj$connection, da_obj$fs_id, tm_desc = tm_desc, use_dates))
}

#' @title Retrieve scalar assumption data frame from Solvas|Capital
#' @description 
#' Gets scalar values from Solvas|Capital transformation data 
#' @param da_obj - current instance of Solvas|Capital's DataAccess class.
#' @param tm_desc - description of transformation (i.e. '(apply to all)'). The first transformation, by sequence 
#' order, will be used if NULL. If two or more transformations have the same description
#'  an error message is returned.
#' @return dataframe (rowname is property_code, columns type_name and property_value )
#' @export
DataAccess.FsAssumptionsScalarGet <- function(da_obj, tm_desc = NULL) {
  return(Solvas.Capital.SqlUtility::SPFSAssumptionsScalarGet(da_obj$connection, da_obj$fs_id, tm_desc = tm_desc))
}

#' @title Retrieve initial account balances from Solvas|Capital
#' @description 
#' Retrieves initial account balances for the scenario. The initial account balance is a direct copy of the entity's 
#' account balance. Returns a data frame of all the initial account balances. The data frame contains all accounts and 
#' dates for the reporting period. NA is used when no account balance exists.
#' 
#' If use_account_name is TRUE then variables will be named by the actual account name  (i.e. 
#' 'Net/Gain' would be account_balances$'Net/Gain').
#'
#' If use_account_name is FALSE then variables will be named using the account number prefixed
#' by ACCNT_ (i.e. account number '1000' would be account_balances$ACCNT_1000).
#' 
#' NOTE: It is important not change the structure or order of the columns in the dataframe as this
#' is used when updating the account balances.
#' 
#'
#' @param da_obj - current instance of Solvas|Capital's DataAccess class.
#' @param use_account_name - if TRUE, will use account_name, false will use account_number.
#' @return dataframe
#' @export
DataAccess.FsInitialAccountBalanceGet <- function(da_obj, use_account_name = TRUE) {
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
  return(Solvas.Capital.SqlUtility::SPFSAccountBalancePut(da_obj$connection, da_obj$fs_id, da_obj$event_id, account_balances))
}

#' @title Retrieve general Scenario and Entity information frame from Solvas|Capital, for the scenario ID specified
#' @description 
#' Takes a DataAccess da_obj and returns a dataframe with general information about the scenario including:
#' 
#' entity_start_date - Start date of the entity \cr
#' entity_end_date - End date of the entity \cr
#' effective_start_date - Effective date of the scenario \cr
#' effective_end_date - End Date of the last reporting period defined for the entity \cr
#' scenario_description - Name of the Scenario \cr
#' functional_currency - Currency specified for the Entity \cr
#' coa_description - Name of the Chart of Accounts associated with the Entity \cr
#' reporting_period_length - Length of reporting periods for the entity (Month, Year) \cr
#' first_reporting_period_end_date - End date of first reporting period for the entity \cr
#' first_reporting_year_end_date - End date of first reporting year for the entity \cr
#' 
#' Example:
#'   scenario_info <- DataAccess.FsScenarioInfoGet(sc_da) \cr
#'   Currency <- scenario_info$functional_currency \cr
#' 
#' @param da_obj - current instance of Solvas|Capital's DataAccess class. 
#' @return dataframe
#' @import RODBC
#' @export
DataAccess.FsScenarioInfoGet <- function(da_obj) {
  return(Solvas.Capital.SqlUtility::SPFSScenarioInfoGet(da_obj$connection,da_obj$fs_id))
}