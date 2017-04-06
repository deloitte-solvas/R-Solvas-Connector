###################################################################################################
# R Model developer skeleton to invoking solvas_model_run
###################################################################################################

library(Solvas.Capital.SqlUtility)
# contruct the sc_da object and call solvas_model_run
sc_connection_string = "Driver={Sql Server};server=(local);trusted_connection=True;database=Internal_Capital_DEV;"
sc_event_id = NULL
sc_fs_id = 1
sc_da <- DataAccess(connection_string_param = sc_connection_string, fs_id_param = sc_fs_id, event_id_param = sc_event_id)
sc_da_connection_status <- DataAccess.ConnectionStatus(sc_da)
solvas_model_run(sc_da)

