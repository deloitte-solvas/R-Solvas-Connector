% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/DataAccess.R
\name{DataAccess.FsInitialAccountBalanceGet}
\alias{DataAccess.FsInitialAccountBalanceGet}
\title{Retrieve initial account balances from Solvas|Capital}
\usage{
DataAccess.FsInitialAccountBalanceGet(da_obj, use_account_name = TRUE)
}
\arguments{
\item{da_obj}{- current instance of Solvas|Capital's DataAccess class.}

\item{use_account_name}{- if TRUE, will use account_name, false will use account_number.}
}
\value{
dataframe
}
\description{
Retrieves initial account balances for the scenario. The initial account balance is a direct copy of the entity's 
account balance. Returns a data frame of all the initial account balances. The data frame contains all accounts and 
dates for the reporting period. NA is used when no account balance exists.

If use_account_name is TRUE then variables will be named by the actual account name  (i.e. 
'Net/Gain' would be account_balances$'Net/Gain').

If use_account_name is FALSE then variables will be named using the account number prefixed
by ACCNT_ (i.e. account number '1000' would be account_balances$ACCNT_1000).

NOTE: It is important not change the structure or order of the columns in the dataframe as this
is used when updating the account balances.
}

