###################################################################################################
# R Model developer skeleton for invoking solvas_capital_model_run 
# for testing out model in RStudio before submitting Solvas|Capital database
###################################################################################################

library(Solvas.Capital.SqlUtility)
run_model_from_rstudio <- function(fs_id, use_local = TRUE) {
  # construct the sc_da object and call solvas_model_run
  if (use_local == TRUE)
    sc_connection_string = "Driver={Sql Server};server=(local);trusted_connection=True;database=Internal_Capital_DEV;"
  else  
    sc_connection_string = "Driver={Sql Server};server=ussltc7534v.prod.sltc.com;trusted_connection=True;database=Internal_Capital_QA;"  
  sc_event_id = NULL
  sc_fs_id = fs_id
  sc_da <<- DataAccess(connection_string_param = sc_connection_string, fs_id_param = sc_fs_id, event_id_param = sc_event_id)
  sc_da_connection_status <- DataAccess.ConnectionStatus(sc_da)
  # DataAccess.TBDSimulateScenarioRunFinancialInstrumentCreate(sc_da)
  solvas_capital_model_run(sc_da)
}

