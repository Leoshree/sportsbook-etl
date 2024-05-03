# sportsbook-etl
NLO Assignment
## 1. Combining legs with markets:

In sportsbetting it's possible to bet on a combination of events. Information on these events can be found in table `bets_v1`. However per bet the source provides data in two separate arrays. The odds are received in a column called 'legs' and the type of bet and the name of the sports-event can be found in the 'markets' column. Your goal is to read `bets_v1` and add a column 'outcomes' which combines these two arrays, such that it becomes clear what are the odds for a specific market (i.e. a single list of outcomes in which all the information of an outcome is available). 

## 2. Adding transactions to a bet

A player has wagered on these bets and may have received payment from winning. To further process the bets, also read `trans_v1`. With the `sportsbook_id` can be determined to which bet the transactions belong. Add a column named `transactions` to the DataFrame created in assignment 1 and add a column which contains per bet an array of the related transactions. 

## 3. Writing the results

Select the `sportsbook_id`, `account_id`, `outcomes`, and `transactions` and write these to a parquet file called `bets_interview_completed` and include this
in the repository. 
