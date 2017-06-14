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
#'   SPResultCheck(data, sp)
#'   ...
#' 
#' @param data - dataframe returned from a sql call 
#' @param sp - sql text used in the call
#' @return throws status_message if status is CERR
#' @keywords internal
SPResultCheck <- function(data,sp) {
  if (is.null(data[['r_status_code']]) == FALSE & is.null(data$r_status_code) == FALSE){
    if (data$r_status_code  == 'CERR')
      stop(paste0(data$r_status_message, ' SQL: ', sp))
    if (data$r_status_code  == 'CWARN')
      warning(paste0(data$r_status_message, ' SQL: ', sp))
  }
  return(TRUE)
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
                ",@effective_scalar_date = ", sqlIsNullStr(effective_date), 
                ",@effective_scalar_period = ", sqlIsNullStr(effective_period),
                ",@match_LTE = 1")
  data <- sqlQuery(cn, sp, errors=TRUE)
  odbcClose(cn)
  SPResultCheck(data, sp)
  
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
                "@tm_desc = ", sqlIsNullStr(tm_desc))
              
  data <- sqlQuery(cn, sp, errors=TRUE)
  odbcClose(cn)
  SPResultCheck(data, sp)
  # reshape so rather than rows of data it is by period in the column
  if (use_dates == TRUE) {
    data <- reshape2::dcast(data, property_code ~effective_date, value.var = 'unified_value')
  } else {
    data <- reshape2::dcast(data, property_code ~effective_period, value.var = 'unified_value')
  }
  rownames(data) <- data[,'property_code']
  data$property_code <- NULL # remove the $ property code column, since its the rownames now
  data = data.frame(t(data)) #Transpose data to user friendly structure
  return(data)
}

#' Internal Solvas|Capital Function 
#' Creates instruments/properites for the passed fs_id
#' 
#' @param connection_string  - the string representing the connection string...
#' @param fs_id The scenario ID
#' @param tm_desc - description/name of transformation (i.e. '(apply to all)').  the first
#  transformation by sequence order will be used if NULL, if two or more transformations have the
#  same description an error message is returned
#' @return data frame containing the economic factors
#' @import RODBC
#' @import reshape2
#' @export
#' @keywords internal
SPFSCreateInstruments <- function(connection_string, fs_id) {
  cn <- odbcDriverConnect(connection_string)
  sp <- paste0("EXEC [app_r].[FS_Create_Instruments] ",
               "@fs_id = ",  fs_id)
  data <- sqlQuery(cn, sp, errors=TRUE, stringsAsFactors=FALSE)
  odbcClose(cn)
  SPResultCheck(data, sp)
  return(data)
}



#' Internal Solvas|Capital Function 
#' gets Solvas|Capital assumptions 
#' 
#' @param connection_string  - the string representing the connection string...
#' @param fs_id The scenario ID
#' @param tm_desc - description/name of transformation (i.e. '(apply to all)').  the first
#  transformation by sequence order will be used if NULL, if two or more transformations have the
#  same description an error message is returned
#' @return data frame containing the economic factors
#' @import RODBC
#' @import reshape2
#' @export
#' @keywords internal
SPFSAssumptionsScalarGet <- function(connection_string, fs_id, tm_desc) {
  cn <- odbcDriverConnect(connection_string)
  sp <- paste0("EXEC [app_r].[FS_Assumptions_Scalar_get] ",
              "@fs_id = ",  fs_id, ", ",
              "@tm_desc = ", sqlIsNullStr(tm_desc))
  
  data <- sqlQuery(cn, sp, errors=TRUE, stringsAsFactors=FALSE)
  odbcClose(cn)
  SPResultCheck(data, sp)
  rownames(data) <- data[,'property_code']
  data$property_code <- NULL # remove the $ property code column, since its the rownames now
  names(data) <- c('type_name','property_value')
  data = data.frame(t(data), stringsAsFactors=FALSE) #Transpose data to user friendly structure
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
  SPResultCheck(data, sp)
  
  # code to pivot by rp_end_date
  if (use_account_name == TRUE) {
    # by Account Name
    pivotData  <- reshape2::dcast(data, account_name ~rp_end_date, value.var = 'account_balance')
    rownames(pivotData) <- pivotData[,'account_name']
    pivotData$account_name <- NULL
    orig_col_names = rownames(pivotData) # prior to transpose
    pivotData = data.frame(t(pivotData))
    # add some attributes used on the put
    attr(pivotData, "row_type") <-'account_name'
    # save our original names so we can update the database
    attr(pivotData, "orig_col_names") <- orig_col_names
    mod_col_names = orig_col_names # keep original names per
    # commenting out code to sanitize names - Mark J was going through hoops
    # change the column names so they don't contain spaces
    #mod_col_names = gsub("\\s|\\t|-|/", "_", toupper(orig_col_names))
    #mod_col_names = gsub("_+", "_", mod_col_names)
  
    colnames(pivotData) = paste("", mod_col_names, sep="")
  } else {
    # by Account Number 
    pivotData  <- reshape2::dcast(data, account_number ~rp_end_date, value.var = 'account_balance')
    rownames(pivotData) <- pivotData[,'account_number']
    pivotData$account_number <- NULL
    orig_col_names = rownames(pivotData) # prior to transpose
    pivotData = data.frame(t(pivotData))
    # add some attributes used on the put
    attr(pivotData, "row_type") <- 'account_number'
    # save our original names so we can update the database
    attr(pivotData, "orig_col_names") <- orig_col_names
    # change the column names so they don't contain spaces
    mod_col_names = gsub("\\s|\\t|-|/", "_", toupper(orig_col_names))
    mod_col_names = gsub("_+", "_", mod_col_names)
    # change the column names so they don't start with a number
    colnames(pivotData) = paste("ACCNT_", mod_col_names, sep="")
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
  if (is.null(attr(df, "orig_col_names")))
      stop(paste0("Dataframe missing orig_col_names attribute"))
  # transpose the df to a matrix and reset the column names
  tm = t(df)
  # replace the account name/account number with the original
  # and escape single quote with double for SQL Server
  orig_col_names = gsub("'", "''", attr(df, "orig_col_names"))
  if (length(orig_col_names) != length(rownames(tm)))
    stop(paste0("The structure of the passed account balance has changed and cannot be updated.  Possible reasons include adding new rows or columns to this dataframe."))
  rownames(tm) = orig_col_names 
  # convert matrix to table 
  df_flat = reshape2::melt(tm)
  # remove rows with NA
  df_flat = na.omit(df_flat) 
  # replace NA/NULL with 'actual value' or NULL
  df_flat$value = sapply(df_flat$value, sqlIsNullStr)
  
  # construct the sql code to insert the records into an @account_balances 
  # variable based on the type app_r.AccountPropertyValueTable
  # restrict each insert statement to the batch size (max is 1000 on SQL Server 2016)
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
  SPResultCheck(data, sp)
  
  return(data)
}

#' Internal Solvas|Capital Function 
#' Get scenario/entity information 
#' @param connection_string  The string representing the connection string...
#' @param fs_id The scenario ID
#' @return data frame containing the general scenario information
#' @import RODBC
#' @export
#' @keywords internal
SPFSScenarioInfoGet <- function(connection_string, fs_id) {
  cn <- odbcDriverConnect(connection_string)
  sp <- paste("EXEC [app_r].[FS_Scenario_Info_get] @fs_id = ", 
              fs_id)
  data <- sqlQuery(cn, sp, errors=TRUE)
  odbcClose(cn)
  SPResultCheck(data, sp)
  
  return(data)
}
