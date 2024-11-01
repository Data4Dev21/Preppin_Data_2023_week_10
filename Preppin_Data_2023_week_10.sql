--Data Source Bank's customers are thrilled with the developments from last week's challenge.
--However, they're not always the smartest... 
--If a transaction isn't made on a particular day, how can the customer find out their balance? 
--They filter the data and no values appear. 
--Looks like we'll need to use a technique called scaffolding to ensure we have a row for each date in the dataset.

--Requirements
--Aggregate the data so we have a single balance for each day already in the dataset, for each account
--Scaffold the data so each account has a row between 31st Jan and 14th Feb (hint)
--Make sure new rows have a null in the Transaction Value field
--Create a parameter so a particular date can be selected
--Filter to just this date
--Output the data - making it clear which date is being filtered to

SET SELECTED_DATE = '2023-02-01'; --setting a variable

WITH CTE AS
(
SELECT D.TRANSACTION_DATE
      ,P.ACCOUNT_TO AS ACCOUNT_ID    --Receiving Account
      ,D.VALUE
      ,I.BALANCE
FROM
TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_DETAIL D 
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_PATH P ON P.transaction_id=D.transaction_id -- this join is to bring in the Receiving account from path
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_ACCOUNT_INFORMATION I ON ACCOUNT_ID=I.ACCOUNT_NUMBER
WHERE I.BALANCE_DATE ='2023-01-31' AND CANCELLED_='N' 

UNION ALL

SELECT D.TRANSACTION_DATE
      ,P.ACCOUNT_FROM AS ACCOUNT_ID  --Payee Account
      ,D.VALUE*-1  -- Needs to be neagive since money is going out
      ,I.BALANCE
FROM
TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_DETAIL D 
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_TRANSACTION_PATH P ON P.transaction_id=D.transaction_id --this join is to bring in the Receiving account from path
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_ACCOUNT_INFORMATION I ON ACCOUNT_ID=I.ACCOUNT_NUMBER
WHERE I.BALANCE_DATE ='2023-01-31' AND CANCELLED_='N'

UNION ALL

SELECT BALANCE_DATE AS TRANSACTION_DATE
      ,ACCOUNT_NUMBER AS ACCOUNT_ID
      ,NULL AS VALUE
      ,BALANCE
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK07_ACCOUNT_INFORMATION
)
,WEEK_9 AS
(
SELECT ACCOUNT_ID
      ,TRANSACTION_DATE
      ,VALUE
    ,SUM(IFNULL(VALUE,0))OVER(PARTITION BY ACCOUNT_ID ORDER BY TRANSACTION_DATE, VALUE DESC) + BALANCE AS BALANCE --value descending caters for the assumption biggest transaction happening first when there are multiple transactions on a day.
FROM CTE
ORDER BY ACCOUNT_ID, TRANSACTION_DATE, VALUE DESC
)
, daily_transaction as
(
SELECT ACCOUNT_ID
      ,TRANSACTION_DATE
      ,SUM(VALUE) AS TRANSACTION_VALUE
      --,SUM(BALANCE) AS BALANCE (summing the balances will be erroneous)
FROM WEEK_9
GROUP BY 1,2
)
, DAILY_BALANCE AS
(
SELECT ACCOUNT_ID
      ,TRANSACTION_DATE
      ,VALUE    
      --this value is just serving as a place holder and will be swapped with the transaction value for DT since that is the sum of all transactions in a day.
      --while this reresent the value of the latest transaction in a day
      ,BALANCE
      ,row_number() OVER (PARTITION BY ACCOUNT_ID, TRANSACTION_DATE ORDER BY  VALUE ASC) as rn 
      -- we set a row number to assign figures to all balances.. idea is to pick up the latest balance on a day if there are multiple transactions. Since there is an assumption of bigger 
      -- transaction preceding smaller transactions on days of multiple transactions, sorting the value ascending will assign 1 to the smallest value which will pick up the latest daily balance 
      -- when we use qualify rn=1
      -- Partition is being done on transcation date too since we want a daily balance
FROM WEEK_9
QUALIFY RN=1   --ACCOUNT_ID='39744047' AND 
) 
, DAILY_SUMMARY AS
(
SELECT DB.ACCOUNT_ID
      ,DB.TRANSACTION_DATE
      ,DT.TRANSACTION_VALUE
      ,DB.BALANCE
      --,row_number() OVER (PARTITION BY ACCOUNT_ID, TRANSACTION_DATE ORDER BY  VALUE ASC) as rn
FROM DAILY_BALANCE DB
JOIN DAILY_TRANSACTION DT ON DB.ACCOUNT_ID=DT.ACCOUNT_ID AND DB.TRANSACTION_DATE=DT.TRANSACTION_DATE
ORDER BY 1,2
)
, DISTINCT_IDS AS
(
SELECT DISTINCT ACCOUNT_ID
FROM DAILY_SUMMARY
)
,NUMBERS AS
(
SELECT '2023-01-31'::date AS n 
      ,ACCOUNT_ID
FROM DISTINCT_IDS
UNION ALL
SELECT DATEADD('day',1,n)  --Date scaffolding
      ,ACCOUNT_ID
from NUMBERS
WHERE n < '2023-02-14'::date
)
,
FINAL_TABLE AS
(
SELECT N.n  
      ,N.ACCOUNT_ID
      ,DS.TRANSACTION_VALUE
      ,DS.BALANCE AS BAL
      ,DB.TRANSACTION_DATE
      ,DB.BALANCE AS BALANCE
      ,DATEDIFF('day',DB.TRANSACTION_DATE,N.n ) as closest
      ,row_number() OVER (PARTITION BY N.ACCOUNT_ID, N ORDER BY CLOSEST) as new_rn   
FROM NUMBERS N
LEFT JOIN DAILY_SUMMARY DS ON N.ACCOUNT_ID=DS.ACCOUNT_ID AND N.n=DS.TRANSACTION_DATE
JOIN DAILY_BALANCE DB ON N.ACCOUNT_ID=DB.ACCOUNT_ID AND DB.TRANSACTION_DATE<=N.n --this clause on the transacetion date allows duplicates from the DB table 
--For example: For this account_id, on 2023-01-31 there was no transaction! As a result, the DB table brings on date 2023-01-31 = N
--It doesnt bring on any date < N since 2023-01-31 is the least date we can have in the dataset per our clauses. N=2023-02-01 is same as initial example
--However with N=2023-02-02, there is a transaction on that date which is capture as -1097.6, but we observe another transcation date from DB showing 2023-01-31
--This is because of the <= clause. This allows duplicates so the need to remove duplicates using datediff to give us the closest day to N which will be min(closest)
--WHERE N.ACCOUNT_ID=10005367
)
SELECT
      ACCOUNT_ID
     ,N AS TRANSACTION_DATE
     ,TRANSACTION_VALUE
     --,DB.BALANCE AS 
     ,BALANCE
FROM FINAL_TABLE
WHERE n = $SELECTED_DATE
QUALIFY NEW_RN = 1
;


