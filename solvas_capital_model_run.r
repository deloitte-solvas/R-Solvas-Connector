###################################################################################################
# START OF SOLVAS|CAPITAL HEADER - DO NOT MODIFY 
###################################################################################################
#
# Define a function called solvas_capital_model_run that takes one paramater - sc_da - this is the 
# DataAccess Object that will be populated with the current scenario id, event id and other
# internal data. This function must be defined AND there should be no other scripting code
# not inclosed in a function in the model file. Having code that is not part in a function
# will cause potential problems when the script is loaded from the server. When the script is
# loaded from the server the contents of this file will be injected into a larger script
# that creates the DataAccess object and calls the solvas_capital_model_run function.
#
# Example:
#   solvas_capital_model_run <- function(sc_da) {
#     # model code here....
#   }
#
# Important: 
# If additional packages are needed to run the model be sure those packages are installed 
# on the database server before attempting to run the model.
#
###################################################################################################
# END OF SOLVAS|CAPITAL HEADER - DO NOT MODIFY 
###################################################################################################
library(Solvas.Capital.SqlUtility)
solvas_capital_model_run <- function(sc_da) {
  # do model development here...
  
  # example: get inital account balances by account number
  print("getting account balances")
  account_balances = DataAccess.FsInitialAccountBalanceGet(sc_da, FALSE)
  print(account_balances["2000",])
  
  ACNT_ACCRETION_OF_ACQ_DISCOUNT = "Accretion of Acquisition Discount"
  account_balances_by_name = DataAccess.FsInitialAccountBalanceGet(sc_da, TRUE)
  print(account_balances_by_name[ACNT_ACCRETION_OF_ACQ_DISCOUNT,])
  
  #AP_BBB_CORPORATE_YIELD_BY_RELATIVE = "BBB_CORPORATE_YIELD_BY_RELATIVE"
  # example: get and print out all assumptions
  #print("getting scenario assumptions")
  #assumptions = DataAccess.FsAssumptionsGet(sc_da, NULL, FALSE)
  #print(assumptions[BBB_CORPORATE_YIELD_BY_RELATIVE,1])
  #print(assumptions["HISTORIC_PRINCIPAL_BALANCE_BB_BY_DATE",1])
  
  
  # example: get and print out interest_rate_effective value for all loans for period 1
  
  instruments = DataAccess.FiInstrumentGet(sc_da,NULL,1)
  print(instruments["interest_rate_effective"])
  
  # for (period in 1:9) {
  #    instruments = DataAccess.FiInstrumentGet(sc_da,NULL,period)
  #    #print(instruments["interest_rate_effective"])
  #    print(period)
  #  }
  
  # change some balance amounts
  account_balances["2000",] = 2000 # example setting all account balances for account #2000 = 2000
  account_balances["2100",] = 2100 # example setting all account balances for account #2100 = 2100
  account_balances["3000",] = 3000 # example setting all account balances for account #3000 = 3000
  account_balances["1000",1] = 99  # example set 99 to first relative period to 1
  
  # save the updated balances back to the database 
  print("updating account balances")
  DataAccess.FsAccountBalancePut(sc_da, account_balances)
  
  # example: get and print out interest_rate_effective value for all loans for effective date 9/30/2014
  #  instruments = DataAccess.FiInstrumentGet(sc_da,"9/30/2014",NULL)
  #  print(instruments["interest_rate_effective"])
  print("example processing complete")
} 


