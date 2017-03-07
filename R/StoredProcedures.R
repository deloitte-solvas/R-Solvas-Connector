#  Low-level access to the database stored procedures.  Not public - use 
#  the object model to access these functions. All functions that directly
#  call any SPs should be at this level. 
# 
#  Note: @keywords internal keeps the documentation from being published.
#  
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
  # reshape so rather than rows of data it is by period in the column
  if (use_dates == TRUE) {
    data <- reshape2::dcast(data, property_code ~date, value.var = 'unified_value')
  } else {
    data <- reshape2::dcast(data, property_code ~period, value.var = 'unified_value')
  }
  rownames(data) <- data[,'property_code']
  data$property_code <- NULL # remove the $ property code column, since its the rownames now
  odbcClose(cn)
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
  
  # code to pivot by rp_end_date
  if (use_account_name == TRUE) {
    pivotData  <- reshape2::dcast(data, account_name ~rp_end_date, value.var = 'account_balance')
    rownames(pivotData) <- pivotData[,'account_name']
    pivotData$account_name <- NULL
    # add some attributes used on the put
    attr(pivotData, "table_type") <-'account_name'
    pivotData_account_number  <- reshape2::dcast(data, account_number ~rp_end_date, value.var = 'account_balance')
    attr(pivotData, "account_number") <- pivotData_account_number[,'account_number']
  } else {
    pivotData  <- reshape2::dcast(data, account_number ~rp_end_date, value.var = 'account_balance')
    rownames(pivotData) <- pivotData[,'account_number']
    pivotData$account_number <- NULL
    # add some attributes used on the put
    attr(pivotData, "table_type") <- 'account_number'
    attr(pivotData, "account_name") <- data[,'account_name']
  }

  odbcClose(cn)
  return(pivotData)
}

#' Internal Solvas|Capital Function 
#' Saves Solvas|Capital account balances
#' @param connection_string  - the string representing the connection string...
#' @param fs_id The scenario ID
#' @param df - data frame of account balances (must match SPFSInitialAccountBalanceGet)
#' @return result of the SP
#' @import RODBC
#' @export
#' @keywords internal
SPFSAccountBalancePut <- function(connection_string, fs_id, df) {
  ###### TODOO...
  # flatten out
  #r = reshape2::melt(as.matrix(df))
  #r = na.omit(r) # remove rows with NA
  # create variables to add
  #value_clause = paste0(sprintf("('%s','%s' %s)", r$Var1, r$Var2, r$value), collapse = ",")
  #value_clause = "('one'),('two')"
  # note this will not work if there are more than 1,000 items .. need to figure if this is a good approach
  # still if it is need to batch the insert calls - could be a limitation on R string size as well...
  #sp = ''
  #sp <- paste0("
  #             DECLARE @codeIdTable bc.CodeIdTable;
  #             INSERT INTO @codeIdTable ([id]) VALUES ",
  #             value_clause,
  #             ";
  #             EXEC [app_r].[Test_Pass_CodeIdTable] @codeIdTable = @codeIdTable", collapse = "")
  #data <- sqlQuery(cn, sp, errors=TRUE)
  #odbcClose(cn)
  #  
  #cn <- odbcDriverConnect(connection_string)
  #query <- paste("
  #  SET NOCOUNT ON;
  #  IF ( OBJECT_ID('app_r.import_account_balance') IS NOT NULL )
  #    DELETE app_r.import_account_balance where fs_id = ", fs_id
  #  )
  #sqlQuery(cn, gsub("\\s|\\t", " ", query))
  # tableData <- df
  # if we use wide data we can melt
  # check row names 
  # View(reshape2::melt(as.matrix(tableData))) 
  # colnames(tableData) <- c("account_number", "rp_end_date", "account_balance")
  # then use table data
  #table_name = paste('app_r.import_account_balance')
  #data <- sqlSave(cn,df,table_name,append = TRUE)
  #odbcClose(cn)
  #return(data)
  return(0)
}
