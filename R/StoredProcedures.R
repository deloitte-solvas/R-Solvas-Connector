#  Low-level access to the database stored procedures.  Not public - use 
#  the object model to access these functions. All functions that directly
#  call any SPs should be at this level. 
# 
#  Note: @keywords internal keeps the documentation from being published.
#  

###################################################################
# Helper Functions
###################################################################
# 
#' Internal Solvas|Capital Function 
#' return sinqle quoated passed string if not null, return single quoate 'replacement_value' otherwise 
#' @param fs_id - The scenario id
#' @return retuns a the passed string surrounded by single quoates if not na or null - otherwise returns unquoted string
#' @keywords internal
sqlIsNullStr <- function(string_value, replacement_value = 'NULL') {
  return(ifelse((is.null(string_value) || is.na(string_value)), replacement_value, paste0("'", string_value, "'")))
}


#' Internal Solvas|Capital Function 

#' Capital convention used to return errors from SP calls. All SP should use this before
#' using the results of an SP
#'
#' Checks the data returned from a SP for a r_status_code column - if it exists and the values is 'CERR' 
#' then throw an error
#' Example:
#'   ...
#'   data <- sqlQuery(cn, sp, errors=TRUE)
#'   SPResultCheck(data)
#'   ...
#' 
#' @param data - dataframe returned from a sql call 
#' @return throws status_message if status is CERR
#' @keywords internal
SPResultCheck <- function(data) {
  if (is.null(data[['r_status_code']]) == FALSE & is.null(data$r_status_code) == FALSE){
    if (data$r_status_code  == 'CERR')
      stop(data$r_status_message)
    if (data$r_status_code  == 'CWARN')
      warning(data$r_status_message)
  }
  return(TRUE)
}

#  Initializes the financial instrument records - must be done on scenarios
#  that have not been run
#  
#  Takes a connection string (connection_string) and scenario id (fs_id) and 
#  loads the financial instruments on the server. No action will take place
#  on a scenario that has already been run.
#   
#' Internal Solvas|Capital Function 
#' Load Property Values
#' @param connection_string - The string representing the connection string...
#' @param fs_id - The scenario id
#' @return A data frame containing the financial instrument data
#' @import RODBC
#' @export
#' @keywords internal
SPInstrumentPropertyLoad <- function(connection_string, fs_id) {
  cn <- odbcDriverConnect(connection_string)
  if (is.null(fs_id) == FALSE) {
    sp <- paste("EXEC [app_r].[FI_Financial_Instrument_Property_load] @fs_id = ", fs_id)
    sqlQuery(cn, sp, errors=TRUE)
  }
  odbcClose(cn)
}

#' Internal Solvas|Capital Function 
#' Get financial instruments 
#' @param connection_string  The string representing the connection string...
#' @param fs_id The scenario ID
#' @param effective_date - The date to use for schedule data types
#' @param effective_period - The period to use for schedule data types (1=first period)
#' @return data frame containing the financial instrument data
#' @import RODBC
#' @export
#' @keywords internal
SPFIInstrumentGet <- function(connection_string, fs_id,  effective_date, effective_period) {
  cn <- odbcDriverConnect(connection_string)
  # note: this will return zero rows if the scenario has not been run or fi_instruments_init_sp has not been called
  sp <- paste("EXEC [app_r].[FI_Financial_Instrument_Effective_Scalar_get] @fs_id = ", 
                fs_id, 
                ",@effective_scalar_date = ",ifelse(is.null(effective_date), "NULL", paste("'",effective_date,"'")), 
                ",@effective_scalar_period = ", ifelse(is.null(effective_period), "NULL", effective_period),
                ",@match_LTE = 1")
  data <- sqlQuery(cn, sp, errors=TRUE)
  odbcClose(cn)
  SPResultCheck(data)
  
  return(data)
}

#' Internal Solvas|Capital Function 
#' Checks the passed package version with the expected version on the current server for compatibility
#' @param connection_string  - the string representing the connection string...
#' @return boolean - true if they are compatible 
#' @import RODBC
#' @export
#' @keywords internal
SPPackageVersionCompatible <- function(connection_string) {
  cn <- odbcDriverConnect(connection_string)
  fn <- paste("SELECT  app_r.PackageVersionCompatible('",gsub(" ", "", noquote(utils::packageDescription('Solvas.Capital.SQLUtility')$Version)),"')")
  data <- sqlQuery(cn, fn, errors=TRUE)
  odbcClose(cn)
  if (data[1] == 1) 
    return (TRUE)
  else
    return (FALSE)
}

#' Internal Solvas|Capital Function 
#' gets Solvas|Capital assumptions 
#' 
#' @param connection_string  - the string representing the connection string...
#' @param fs_id The scenario ID
#' @param tm_desc - description/name of transformation (i.e. '(apply to all)').  the first
#  transformation by sequence order will be used if NULL, if two or more transformations have the
#  same description an error message is returned
#' @param use_dates - if true then matrix columns will be dates, else periods. default to false 
#' @return data frame containing the economic factors
#' @import RODBC
#' @import reshape2
#' @export
#' @keywords internal
SPFSAssumptionsGet <- function(connection_string, fs_id, tm_desc, use_dates) {
  cn <- odbcDriverConnect(connection_string)
  sp <- paste("EXEC [app_r].[FS_Assumptions_get] ",
                "@fs_id = ",  fs_id, ", ",
                "@tm_desc = ", ifelse(is.null(tm_desc), "NULL", paste("'",tm_desc,"'"))
              )
  data <- sqlQuery(cn, sp, errors=TRUE)
  odbcClose(cn)
  SPResultCheck(data)
  # reshape so rather than rows of data it is by period in the column
  if (use_dates == TRUE) {
    data <- reshape2::dcast(data, property_code ~effective_date, value.var = 'unified_value')
  } else {
    data <- reshape2::dcast(data, property_code ~effective_period, value.var = 'unified_value')
  }
  rownames(data) <- data[,'property_code']
  data$property_code <- NULL # remove the $ property code column, since its the rownames now
  return(data)
}


#' Internal Solvas|Capital Function 
#' Gets Solvas|Capital initial account balances
#' @param connection_string  - the string representing the connection string...
#' @param fs_id The scenario ID
#' @param use_account_name if TRUE will use account_name, false will use account_number
#' @return data frame containing the account balances 
#' @import RODBC
#' @export
#' @keywords internal
SPFSInitialAccountBalanceGet <- function(connection_string, fs_id, use_account_name = TRUE) {
  cn <- odbcDriverConnect(connection_string)
  sp <- paste("EXEC [app_r].[FS_Initial_Account_Balance_get] ",
              "@fs_id = ",  fs_id)
  data <- sqlQuery(cn, sp, errors=TRUE)
  odbcClose(cn)
  SPResultCheck(data)
  
  # code to pivot by rp_end_date
  if (use_account_name == TRUE) {
    pivotData  <- reshape2::dcast(data, account_name ~rp_end_date, value.var = 'account_balance')
    rownames(pivotData) <- pivotData[,'account_name']
    pivotData$account_name <- NULL
    # add some attributes used on the put
    attr(pivotData, "row_type") <-'account_name'
  } else {
    pivotData  <- reshape2::dcast(data, account_number ~rp_end_date, value.var = 'account_balance')
    rownames(pivotData) <- pivotData[,'account_number']
    pivotData$account_number <- NULL
    # add some attributes used on the put
    attr(pivotData, "row_type") <- 'account_number'
  }

  return(pivotData)
}

#' Internal Solvas|Capital Function 
#' Saves Solvas|Capital account balances
#' @param connection_string  - the string representing the connection string...
#' @param fs_id The scenario ID
#' @param df - data frame of account balances (must match SPFSInitialAccountBalanceGet)
#' @return result of the SP
#' @import RODBC
#' @import stats
#' @export
#' @keywords internal
SPFSAccountBalancePut <- function(connection_string, fs_id, event_id, df) {
  row_type = attr(df, "row_type") # get matrix row_type that we will use for (account_name or account_number)
  key_column = NULL
  # map the matrix row_type to a either account_description or account_number depending on the type of matrix
  if (row_type == "account_name")
    key_column = "account_description"
  if (row_type == "account_number")
    key_column = "account_number"
  
  if (is.null(key_column))
    stop(paste0("Cannot map data frame attr 'row_type' value of ", sqlIsNullStr(key_column), " to a key column"))
  # convert matrix to table 
  df_flat = reshape2::melt(as.matrix(df))
  df_flat = na.omit(df_flat) # remove rows with NA
  # replace NA/NULL with 'actual value' or NULL
  df_flat$value = sapply(df_flat$value, sqlIsNullStr)
  
  insert_sql = ''
  max_row = nrow(df_flat)
  
  # batch up inserts (SQL limits the amount to 1000 per insert)
  batch_size = 500
  lower_bound = 1
  upper_bound = min(lower_bound+batch_size -1, max_row)

  while (lower_bound < max_row)  {
    value_clause = paste0(sprintf("('%s','%s','%s',%s)",
                                  df_flat$Var1[lower_bound:min(upper_bound,max_row)],
                                  "ACCOUNT_BALANCE",
                                  df_flat$Var2[lower_bound:min(upper_bound,max_row)],
                                  df_flat$value[lower_bound:min(upper_bound,max_row)]),
                                collapse = ",")
    value_clause = paste0("INSERT INTO @account_balances ([", key_column, "], [property_code], [rp_end_date], [property_value]) VALUES ", 
                          value_clause, "; ")
    insert_sql = paste0(insert_sql, value_clause)
    lower_bound = upper_bound +  1;
    upper_bound = lower_bound + batch_size - 1;
  }
  
  # create the full statement.
  sp <- paste0("
               SET NOCOUNT ON 
               DECLARE @account_balances app_r.AccountPropertyValueTable;",
               insert_sql,
               "EXEC [app_r].[FS_Account_Balance_put]",
               "@fs_id = ", fs_id ,  
               ",@account_balances = @account_balances",
               ",@event_id = ", sqlIsNullStr(event_id),
               collapse = "")
  sp <- gsub("\\s|\\t", " ", sp) #clean up white space
  cn <- odbcDriverConnect(connection_string)
  data <- sqlQuery(cn, sp, errors=TRUE)
  odbcClose(cn)
  SPResultCheck(data)
  
  return(data)
}
