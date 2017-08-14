###################################################################################################
#
# Define a function called solvas_capital_model_run that takes one parameter - sc_da - this is the 
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

library(Solvas.Capital.SqlUtility)

solvas_capital_model_run <- function(sc_da) {
  # do model development here...
  #test
  # example get scenario info
  print("getting scenario info")
  scenario_info <- DataAccess.FsScenarioInfoGet(sc_da)
  print (scenario_info$functional_currency)
  # example get initial account balances by account number
  print("getting account balances")
  account_balances = DataAccess.FsInitialAccountBalanceGet(sc_da, FALSE)
  # example printing account_balances$ACCNT_2000
  print(account_balances$ACCNT_2000)
  # change some balance amounts
  account_balances$ACCNT_2000 = 2000  # example setting all account balances for account #2000 = 2000
  account_balances$ACCNT_2100 = 2100  # example setting all account balances for account #2100 = 2100
  account_balances$ACCNT_1040 = 1040  # example setting all account balances for account #1040 = 1040
  account_balances$ACCNT_1000[1] = 99 # example setting first period of account #1000 to 99
  print("updating account balances")
  # save the updated balances back to the database 
  DataAccess.FsAccountBalancePut(sc_da, account_balances)
  
  # example: get initial account balances by account name 
  account_balances_by_name = DataAccess.FsInitialAccountBalanceGet(sc_da, TRUE)
  # example printing balances for Accrued Fee Payable
  print(account_balances_by_name$`Accrued Fee Payable`)
  account_balances_by_name$`Accrued Fee Payable` = 2000 # updating Accrued Fee Payable
  account_balances_by_name$`Cash Receivable/Payable` = 1090 # updating Cash Receivable/Payable
  print("updating account balances")
  # save the updated balances back to the database 
  # NOTE: this will OVERWRITE the account balances from the prior example!
  DataAccess.FsAccountBalancePut(sc_da, account_balances_by_name)
  
  # example getting scalar data (commented out - not seeded data)
  # assumptions_scalar = DataAccess.FsAssumptionsScalarGet(sc_da,'model scalars')
  # start_date = as.Date(assumptions_scalar$ACQUISITION_DATE[1])
  # print(start_date)
  # risk_rating = as.double(assumptions_scalar$RISK_RATING[1])
  # print(risk_rating)
  
  # example: get and print out all assumptions
  print("getting scenario assumptions")
  assumptions <- DataAccess.FsAssumptionsGet(sc_da, NULL, FALSE)
  #View(assumptions)
  sc_assumptions = assumptions;
  print(assumptions$BBB_CORPORATE_YIELD_BY_RELATIVE)

  

  # example: get and print out interest_rate_effective value for all loans for period 1
  
  instruments = DataAccess.FiInstrumentGet(sc_da,NULL,1)
  print(instruments["interest_rate_effective"])
  
  # for (period in 1:9) {
  #    instruments = DataAccess.FiInstrumentGet(sc_da,NULL,period)
  #    #print(instruments["interest_rate_effective"])
  #    print(period)
  #  }
  
  
  # example: get and print out interest_rate_effective value for all loans for effective date 9/30/2014
  #  instruments = DataAccess.FiInstrumentGet(sc_da,"9/30/2014",NULL)
  #  print(instruments["interest_rate_effective"])
  print("example processing complete")
} 


