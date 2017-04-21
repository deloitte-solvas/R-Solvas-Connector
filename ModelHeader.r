

# setup sc_result_set for returning values
rm(list = ls(pattern = "sc_*")) # remove any sc_ from the env
sc_output_table = NULL

sc_is_running_from_server = 1
#sc_connection_string = "Driver=SQL Server Native Client 11.0;server=(local_2016);database=Internal_Forecast_Dev;UId=solvas_user;Pwd=solvas"
sc_connection_string = "Driver={Sql Server};server=(local_2016);trusted_connection=Yes;database=Internal_Forecast_Dev;"
sc_event_id = 24
sc_fs_id = 3
# end setup var for testing

###################################################################################################
# START OF SOLVAS|CAPITAL HEADER - DO NOT MODIFY 
###################################################################################################

# declare diagnostic variables 
sc_undefined = "undefined"

# set array with all the sc_variables to track for existences and return status information
# add new sc_ variables  - passed in or used for diagnostic result set
sc_var_name = c("sc_is_running_from_server","sc_connection_string", "sc_event_id", "sc_fs_id", "sc_diag_rodbc_lib", "sc_diag_solvas_utility_lib", "sc_diag_connection")

# check for required libary RODBC
sc_diag_rodbc_lib =
  tryCatch(
    {
      library(RODBC)
      "success"
    },  
    warning = function(cond) paste("ERROR:  ", cond),
    error = function(cond)  paste("ERROR:  ", cond) 
  )

# check for required library Solvas.Capital.Utility
sc_diag_solvas_utility_lib =
  tryCatch(
    {
      library(Solvas.Capital.SQLUtility)
      "success"
    },  
    warning = function(cond) paste("ERROR:  ", cond),
    error = function(cond) paste("ERROR:  ", cond)	  
  )


# check for odbc connectivity
sc_diag_connection = 
  tryCatch(
    {
      cn <- odbcDriverConnect(connection=sc_connection_string)
      print(sqlQuery(cn, "SELECT CURRENT_USER", errors=TRUE))
      odbcClose(cn)
      "success"
    } ,  
    warning = function(cond) paste("ERROR:  ", cond),
    error = function(cond)  paste("ERROR:  ", cond)	  
)
# set vector with variable values, use sc_undefined if variable does not exist - get0 checks if the var name is a variable ifnotfound is a parameter name to get0
sc_var_value = unname((sapply(sc_var_name, get0, ifnotfound=sc_undefined))) 
# create the result set to return to the server for diagnostic information
sc_result_set = data.frame(sc_var_name, sc_var_value, stringsAsFactors = FALSE)

#print(sc_result_set)
# push the results to the SQL server parameter from sp_execute_external_script
sc_output_table <- as.data.frame(sc_result_set)
###################################################################################################
# END OF SOLVAS|CAPITAL HEADER - DO NOT MODIFY 
###################################################################################################
#
# Example usage for model developers
# Insert after the header:
if (sc_is_running_from_server == 1)
  print('running from the server')
