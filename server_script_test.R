# internal use - for testing server generated R script

library(Solvas.Capital.SqlUtility)

run_user_model <- function()
{
  # declare diagnostic variables 
  sc_undefined = "undefined"
  sc_da <- DataAccess(connection_string_param = sc_connection_string, fs_id_param = sc_fs_id, event_id_param = sc_event_id)
  sc_da_connection_status <- DataAccess.ConnectionStatus(sc_da)
  sc_libPath = .libPaths()
  
  
  sc_diag_solvas_capital_model_run =
    tryCatch( 
      {
        solvas_capital_model_run(sc_da)
        "success"
      },
      warning = function(cond) paste("ERROR: ", cond),
      error = function(cond) paste("ERROR: ", cond)
    )
  
  # collect all the variables in the environment that have sc_ prefix
  sc_var_name = c(unlist(ls(pattern = "sc_*"), use.names = FALSE))
  # set vector with variable values, use sc_undefined if variable does not exist - get0 checks if the var name is a variable ifnotfound is a parameter name to get0
  sc_var_value = unname(sapply(sc_var_name,
                               function(x) 
                               {  
                                 ifelse(is.null(get0(x, ifnotfound = sc_undefined)), 
                                        sc_undefined, 
                                        toString(get0(x, ifnotfound = sc_undefined)))
                               }))
  sc_result_set = data.frame(sc_var_name, sc_var_value, stringsAsFactors = FALSE)
  # return diagnostic/setup variables in the form of a data.frame
  as.data.frame(sc_result_set)
}



sc_connection_string = "Driver={Sql Server};server=(local);trusted_connection=True;database=Internal_Capital_DEV;"
sc_event_id = NULL
sc_fs_id = 1
sc_da <- DataAccess(connection_string_param = sc_connection_string, fs_id_param = sc_fs_id, event_id_param = sc_event_id)
sc_da_connection_status <- DataAccess.ConnectionStatus(sc_da)


sc_output_table <- run_user_model()