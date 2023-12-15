

# !pip install pymysql    #we  use it for access mysql database 
# !pip install mysqlclient #used to send commands or queries to the server 
# !pip install emails ##library for managing email messages. 
# !pip install pretty-html-table  #for formating 
# !pip install flask_sqlalchemy # provides ways to interact with several database engines such as SQLite, MySQL, and PostgreSQ


import pandas as pd
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formatdate
import pymysql.cursors
import sqlalchemy as sa
import MySQLdb
from pretty_html_table import build_table
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
from datetime import date, datetime, timedelta
import numpy as np
from decouple import config #to read .env file 



# Retrieve sensitive information from environment variables
DB_HOST = config("DB_HOST")
DB_USER = config("DB_USER")
DB_PASSWORD = config("DB_PASSWORD")
DB_NAME = config("DB_NAME")
email_sender = config('EMAIL_USER')
email_password = config('EMAIL_PASSWORD')

mydb = pymysql.connect(
    host= DB_HOST,
    user= DB_USER,
    password= DB_PASSWORD,
    database= DB_NAME
)



query_1 = """ 

WITH cte AS
(SELECT 

SUM(CASE 
WHEN Order_Date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) THEN B_GMV END) AS Order_received_today,
SUM(CASE 
WHEN Shipped_Date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) THEN B_GMV END) AS Shipped_today,
SUM(CASE 
WHEN cancellation_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) THEN B_GMV END) AS Cancelled_today,
SUM(CASE WHEN Order_Date < CURDATE()
AND line_status IN ('awaiting procurement','Packed', 'Confirmed','allocated','picked')
THEN B_GMV END) AS Total_Pending
FROM
 (SELECT CASE WHEN b.sku IS NULL THEN 'False' ELSE 'True' END liquidation_Flag,
CASE  
WHEN utm_source='Dealer' AND allocation_mode='MP' THEN 'MarketPlace SKU -Dealer'
WHEN utm_source='Dealer' AND a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM -Dealer'
WHEN utm_source='Dealer' THEN CONCAT(a.sku_type,' -Dealer')
WHEN a.allocation_mode='NONE' THEN a.sku_type
WHEN a.allocation_mode IS NULL THEN a.sku_type
WHEN a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM'
WHEN a.sku_type='Planned Inventory SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
WHEN a.sku_type='Private Label SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
ELSE a.sku_type END 'Mode',
a.*,
ops_spoc_email,CASE WHEN payment_mode='cod' THEN 'cod' ELSE 'prepaid' END payment_type
FROM analytics_writedb.ff_report_daily a
LEFT JOIN vikreta.dealers_newvendor d ON a.allocated_vendor_code = d.vendor_code
LEFT JOIN
(SELECT * FROM analytics_writedb.liquidation WHERE 
report_date=SUBDATE(CURDATE(),1)) b ON a.sku = b.sku
) a
WHERE a.mode IN ('MarketPlace SKU','MarketPlace SKU -Dealer')
)
SELECT 
SUM(Total_pending-Order_received_today+Shipped_today+Cancelled_today) AS Total_Pendency_till_yesterday,cte.* FROM cte

"""




query_2="""


(SELECT line_status AS `Status`,
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
THEN B_GMV END) AS 1_2_Days,
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND DATE_SUB(CURDATE(), INTERVAL 4 DAY)
THEN B_GMV END) AS 3_5_Days,
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 9 DAY) AND DATE_SUB(CURDATE(), INTERVAL 7 DAY)
THEN B_GMV END) AS 6_8_Days,
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 11 DAY) AND DATE_SUB(CURDATE(), INTERVAL 10 DAY)
THEN B_GMV END) AS 9_10_Days,

SUM(CASE WHEN Order_Date  < DATE_SUB(CURDATE(), INTERVAL 11 DAY)
THEN B_GMV END) AS Above_10_Days, 
SUM(B_GMV) AS Total_pending
FROM
(WITH cte AS (SELECT CASE WHEN b.sku IS NULL THEN 'False' ELSE 'True' END liquidation_Flag,
CASE  
WHEN utm_source='Dealer' AND allocation_mode='MP' THEN 'MarketPlace SKU -Dealer'
WHEN utm_source='Dealer' AND a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM -Dealer'
WHEN utm_source='Dealer' THEN CONCAT(a.sku_type,' -Dealer')
WHEN a.allocation_mode='NONE' THEN a.sku_type
WHEN a.allocation_mode IS NULL THEN a.sku_type
WHEN a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM'
WHEN a.sku_type='Planned Inventory SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
WHEN a.sku_type='Private Label SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
ELSE a.sku_type END 'Mode',
a.*,
ops_spoc_email,CASE WHEN payment_mode='cod' THEN 'cod' ELSE 'prepaid' END payment_type
FROM analytics_writedb.ff_report_daily a
LEFT JOIN vikreta.dealers_newvendor d ON a.allocated_vendor_code = d.vendor_code
LEFT JOIN
(SELECT * FROM analytics_writedb.liquidation WHERE 
report_date=SUBDATE(CURDATE(),1)) b ON a.sku = b.sku)
SELECT * FROM cte 
WHERE `mode` IN ('MarketPlace SKU','MarketPlace SKU -Dealer')

)b
WHERE line_status IN ('awaiting procurement','Packed', 'Confirmed')
GROUP BY 1)
UNION ALL
(SELECT "Total",
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
THEN B_GMV END) AS 1_2_Days,
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND DATE_SUB(CURDATE(), INTERVAL 4 DAY)
THEN B_GMV END) AS 3_5_Days,
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 9 DAY) AND DATE_SUB(CURDATE(), INTERVAL 7 DAY)
THEN B_GMV END) AS 6_8_Days,
SUM(CASE WHEN Order_Date  BETWEEN DATE_SUB(CURDATE(), INTERVAL 11 DAY) AND DATE_SUB(CURDATE(), INTERVAL 10 DAY)
THEN B_GMV END) AS 9_10_Days,

SUM(CASE WHEN Order_Date  < DATE_SUB(CURDATE(), INTERVAL 11 DAY)
THEN B_GMV END) AS Above_10_Days, 
SUM(B_GMV) AS Total_pending
FROM
(WITH cte AS (SELECT CASE WHEN b.sku IS NULL THEN 'False' ELSE 'True' END liquidation_Flag,
CASE  
WHEN utm_source='Dealer' AND allocation_mode='MP' THEN 'MarketPlace SKU -Dealer'
WHEN utm_source='Dealer' AND a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM -Dealer'
WHEN utm_source='Dealer' THEN CONCAT(a.sku_type,' -Dealer')
WHEN a.allocation_mode='NONE' THEN a.sku_type
WHEN a.allocation_mode IS NULL THEN a.sku_type
WHEN a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM'
WHEN a.sku_type='Planned Inventory SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
WHEN a.sku_type='Private Label SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
ELSE a.sku_type END 'Mode',
a.*,
ops_spoc_email,CASE WHEN payment_mode='cod' THEN 'cod' ELSE 'prepaid' END payment_type
FROM analytics_writedb.ff_report_daily a
LEFT JOIN vikreta.dealers_newvendor d ON a.allocated_vendor_code = d.vendor_code
LEFT JOIN
(SELECT * FROM analytics_writedb.liquidation WHERE 
report_date=SUBDATE(CURDATE(),1)) b ON a.sku = b.sku)
SELECT * FROM cte 
WHERE `mode` IN ('MarketPlace SKU','MarketPlace SKU -Dealer')


)c


WHERE line_status IN ('awaiting procurement','Packed', 'Confirmed'))


"""


query_3="""
WITH cte2 AS 

(WITH cte AS 


(SELECT EXTRACT(MONTH FROM order_date) Months, #line_status,
SUM(B_GMV)booked_GMV,

SUM(CASE WHEN line_status LIKE '%ship%' OR line_status LIKE '%pick%' OR line_status LIKE '%deliver%' 
OR line_status LIKE 'dispatch%' OR line_status LIKE '%RTO%' OR line_status LIKE '%return%' 
THEN  B_GMV END) shipped_GMV,
SUM(CASE WHEN line_status IN ('cancelled') THEN B_GMV END ) cancelled_GMV,




CASE	
WHEN ops_spoc_email = 'gulshan.mudgal@industrybuying.com' THEN 'Gulshan'
WHEN ops_spoc_email = 'azad@industrybuying.com' THEN 'Azad'
WHEN ops_spoc_email = 'sameerbatra@industrybuying.com' THEN 'Sameer'
WHEN ops_spoc_email = 'aayush.kumar@industrybuying.com' THEN 'Aayush'
WHEN ops_spoc_email = 'deepakib@industrybuying.com' THEN 'Deepak'
WHEN ops_spoc_email = 'akshay.pathania@industrybuying.com' THEN 'Akshay'
WHEN ops_spoc_email = 'himanshu.k@industrybuying.com' THEN 'Himanshu'
WHEN ops_spoc_email = 'vishal.sikarwar@industrybuying.com' THEN 'Vishal'
WHEN ops_spoc_email = 'navneet.kumar@industrybuying.com' THEN 'Navneet'
WHEN ops_spoc_email = 'sachin.kaushik@industrybuying.com' THEN 'Sachin'
WHEN ops_spoc_email = 'sumit.jindal@industrybuying.com' THEN 'Sumit'
END AS spoc

FROM

(SELECT CASE WHEN b.sku IS NULL THEN 'False' ELSE 'True' END liquidation_Flag,
CASE  
WHEN utm_source='Dealer' AND allocation_mode='MP' THEN 'MarketPlace SKU -Dealer'
WHEN utm_source='Dealer' AND a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM -Dealer'
WHEN utm_source='Dealer' THEN CONCAT(a.sku_type,' -Dealer')
WHEN a.allocation_mode='NONE' THEN a.sku_type
WHEN a.allocation_mode IS NULL THEN a.sku_type
WHEN a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM'
WHEN a.sku_type='Planned Inventory SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
WHEN a.sku_type='Private Label SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
ELSE a.sku_type END 'Mode',
a.*,
ops_spoc_email,CASE WHEN payment_mode='cod' THEN 'cod' ELSE 'prepaid' END payment_type
FROM analytics_writedb.ff_report_daily a
LEFT JOIN vikreta.dealers_newvendor d ON a.allocated_vendor_code = d.vendor_code
LEFT JOIN
(SELECT * FROM analytics_writedb.liquidation WHERE 
report_date=SUBDATE(CURDATE(),1)) b ON a.sku = b.sku
)tab



WHERE tab.mode IN ('MarketPlace SKU','MarketPlace SKU -Dealer') AND 
tab.ops_spoc_email IN (
'gulshan.mudgal@industrybuying.com',
'azad@industrybuying.com',
'sameerbatra@industrybuying.com',
'aayush.kumar@industrybuying.com',
'deepakib@industrybuying.com',
'akshay.pathania@industrybuying.com',
'himanshu.k@industrybuying.com',
'vishal.sikarwar@industrybuying.com',
'navneet.kumar@industrybuying.com',
'sachin.kaushik@industrybuying.com',
'sumit.jindal@industrybuying.com'
)
GROUP BY 1,5
)
SELECT spoc,
SUM(CASE WHEN months=12 THEN shipped_GMV END) AS dec_ship,
SUM(CASE WHEN months=12 THEN booked_GMV END) AS dec_booked,
SUM(CASE WHEN months=12 THEN cancelled_GMV END) AS dec_cancelled,


SUM(CASE WHEN months=11 THEN shipped_GMV END) AS nov_ship,
SUM(CASE WHEN months=11 THEN booked_GMV END) AS nov_booked,
SUM(CASE WHEN months=11 THEN cancelled_GMV END) AS nov_cancelled,

SUM(CASE WHEN months=10 THEN shipped_GMV END) AS oct_ship,
SUM(CASE WHEN months=10 THEN booked_GMV END) AS oct_booked,
SUM(CASE WHEN months=10 THEN cancelled_GMV END) AS oct_cancelled




FROM cte

GROUP BY spoc)

SELECT spoc,
ROUND(dec_ship/dec_booked*100,0) AS dec_shipped_rate,
ROUND(((dec_cancelled)/dec_booked)*100,0) dec_cancelled_rate,
ROUND((dec_booked-dec_ship-dec_cancelled)/(dec_booked)*100,0) AS dec_pending_rate,



ROUND(oct_ship/oct_booked*100,0) AS Oct_shipped_rate,
ROUND(((oct_booked-oct_ship)/oct_booked)*100,0) Oct_cancelled_rate,
ROUND((oct_booked-oct_ship-oct_cancelled)/(oct_booked)*100,0) AS oct_pending_rate,


ROUND((nov_ship/nov_booked)*100,0) AS Nov_shipped_rate,
ROUND(((nov_booked-nov_ship)/nov_booked)*100,0) Nov_cancelled_rate,
ROUND((nov_booked-nov_ship-nov_cancelled)/(nov_booked)*100,0) AS nov_pending_rate


FROM cte2

"""



query_4="""
WITH cte3 AS 
(WITH cte2 AS 
(WITH cte AS 
(SELECT MONTH(order_date) AS or_month,

allocated_vendor_name,
SUM(B_GMV) AS Booked_GMV,
 #extract (month 
SUM(CASE WHEN line_status LIKE '%ship%' OR line_status LIKE '%pick%' OR t1.line_status LIKE '%deliver%' 
OR t1.line_status LIKE 'dispatch%' OR t1.line_status LIKE '%RTO%' OR t1.line_status LIKE '%return%' 
THEN  B_GMV END)
AS Shipped_GMV

FROM  

(WITH cte AS (SELECT CASE WHEN b.sku IS NULL THEN 'False' ELSE 'True' END liquidation_Flag,

CASE  
WHEN utm_source='Dealer' AND allocation_mode='MP' THEN 'MarketPlace SKU -Dealer'
WHEN utm_source='Dealer' AND a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM -Dealer'
WHEN utm_source='Dealer' THEN CONCAT(a.sku_type,' -Dealer')
WHEN a.allocation_mode='NONE' THEN a.sku_type
WHEN a.allocation_mode IS NULL THEN a.sku_type
WHEN a.sku_type='MarketPlace SKU' AND allocation_mode='JIT' THEN 'JIT/OEM'
WHEN a.sku_type='Planned Inventory SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
WHEN a.sku_type='Private Label SKU' AND a.allocation_mode='MP' THEN 'MarketPlace SKU'
ELSE a.sku_type END 'Mode',
a.*,
ops_spoc_email,CASE WHEN payment_mode='cod' THEN 'cod' ELSE 'prepaid' END payment_type
FROM analytics_writedb.ff_report_daily a
LEFT JOIN vikreta.dealers_newvendor d ON a.allocated_vendor_code = d.vendor_code
LEFT JOIN
(SELECT * FROM analytics_writedb.liquidation WHERE 
report_date=SUBDATE(CURDATE(),1)) b ON a.sku = b.sku)
SELECT * FROM cte 
WHERE `mode` IN ('MarketPlace SKU','MarketPlace SKU -Dealer')

) t1
 WHERE allocated_vendor_name IS NOT NULL 
 AND 
 order_date 
 BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 3 MONTH),"%y-%M-01") AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 0 MONTH))  
 GROUP BY 1,2
 ORDER BY 2 DESC)
 
 
SELECT d.*,b.*,c.*,a.* FROM 

 (SELECT allocated_vendor_name avn,SUM(Booked_GMV)Booked_GMV,SUM(Shipped_GMV)Shipped_GMV 
 FROM cte GROUP BY 1 ORDER BY 2 DESC LIMIT 50) 
 a INNER JOIN	
(SELECT allocated_vendor_name savn,
SUM(CASE WHEN or_month=10 THEN Booked_GMV END) Oct_Booked_GMV,
SUM(CASE WHEN or_month=10 THEN Shipped_GMV END) Oct_Shipped_GMV 
 FROM cte GROUP BY 1) b ON a.avn=b.savn
 
 LEFT JOIN
 (SELECT allocated_vendor_name oavn,SUM(CASE WHEN or_month=11 THEN Booked_GMV END) Nov_Booked_GMV,
SUM(CASE WHEN or_month=11 THEN Shipped_GMV END)Nov_Shipped_GMV 
 FROM cte GROUP BY 1) c ON a.avn=c.oavn
 LEFT JOIN
 (SELECT allocated_vendor_name aavn,SUM(CASE WHEN or_month=12 THEN Booked_GMV END) Dec_Booked_GMV,
SUM(CASE WHEN or_month=12 THEN Shipped_GMV END)Dec_Shipped_GMV 
 FROM cte GROUP BY 1) d ON a.avn=d.aavn
 )
 SELECT aavn AS Vendor_Name, 
 ROUND(Dec_Booked_GMV,0)Dec_Booked_GMV,
 ROUND(Dec_Shipped_GMV,0)Dec_Shipped_GMV,
ROUND(Dec_Shipped_GMV/Dec_Booked_GMV*100,0) ff_Rate_Dec,

 ROUND(NOV_Booked_GMV,0)NOV_Booked_GMV,ROUND(NOV_Shipped_GMV,0)NOV_Shipped_GMV,
ROUND(NOV_Shipped_GMV/NOV_Booked_GMV*100,0) ff_Rate_Nov,
  
 ROUND(OCT_Booked_GMV,0)OCT_Booked_GMV,
 ROUND(OCT_Shipped_GMV,0)OCT_Shipped_GMV,
ROUND(OCT_Shipped_GMV/OCT_Booked_GMV*100,0) ff_Rate_Oct,
 

 ROUND(Booked_GMV,0) AS TOtal_GMV,ROUND(Shipped_GMV,0) AS Total_Shipped_GMV FROM cte2)
  SELECT row_number()
 over(ORDER BY Total_GMV DESC )Top_Seller ,cte3.* FROM cte3










"""
query_5= """

SELECT MAX(report_date)  AS Report_Date FROM `ff_report_daily`

"""


f1 = pd.read_sql(query_1,mydb)
f2=pd.read_sql(query_2,mydb)
f3=pd.read_sql(query_3,mydb)
f4=pd.read_sql(query_4,mydb)
f5=pd.read_sql(query_5,mydb)



#Formating 

f1['Total_Pendency_till_yesterday'] = f1['Total_Pendency_till_yesterday'].map('₹{:,.0f}'.format)
f1['Order_received_today'] = f1['Order_received_today'].map('₹{:,.0f}'.format)
f1['Shipped_today'] = f1['Shipped_today'].map('₹{:,.0f}'.format)
f1['Cancelled_today'] = f1['Cancelled_today'].map('₹{:,.0f}'.format)
f1['Total_Pending'] = f1['Total_Pending'].map('₹{:,.0f}'.format)


#2

f2['1_2_Days'] = f2['1_2_Days'].map('₹{:,.0f}'.format)
f2['3_5_Days'] = f2['3_5_Days'].map('₹{:,.0f}'.format)
f2['6_8_Days'] = f2['6_8_Days'].map('₹{:,.0f}'.format)
f2['9_10_Days'] = f2['9_10_Days'].map('₹{:,.0f}'.format)
f2['Above_10_Days'] = f2['Above_10_Days'].map('₹{:,.0f}'.format)
f2['Total_pending'] = f2['Total_pending'].map('₹{:,.0f}'.format)

#3

f3['dec_shipped_rate'] = f3['dec_shipped_rate'].map('{:,.0f}%'.format)
f3['dec_cancelled_rate'] = f3['dec_cancelled_rate'].map('{:.0f}%'.format)
f3['dec_pending_rate'] = f3['dec_pending_rate'].map('{:.0f}%'.format)


f3['Oct_shipped_rate'] = f3['Oct_shipped_rate'].map('{:.0f}%'.format)
f3['Oct_cancelled_rate'] = f3['Oct_cancelled_rate'].map('{:.0f}%'.format)
f3['oct_pending_rate'] = f3['oct_pending_rate'].map('{:.0f}%'.format)

f3['Nov_shipped_rate'] = f3['Nov_shipped_rate'].map('{:.0f}%'.format)
f3['Nov_cancelled_rate'] = f3['Nov_cancelled_rate'].map('{:.0f}%'.format)
f3['nov_pending_rate'] = f3['nov_pending_rate'].map('{:.0f}%'.format)

#4

f4 = f4.replace(np.nan, 0)

f4['Dec_Booked_GMV'] = f4['Dec_Booked_GMV'].map('₹{:,.0f}'.format)
f4['Dec_Shipped_GMV'] = f4['Dec_Shipped_GMV'].map('₹{:,.0f}'.format)

f4['ff_Rate_Dec'] = f4['ff_Rate_Dec'].map('{:,.0f}%'.format)

f4['NOV_Booked_GMV'] = f4['NOV_Booked_GMV'].map('₹{:,.0f}'.format)
f4['NOV_Shipped_GMV'] = f4['NOV_Shipped_GMV'].map('₹{:,.0f}'.format)
f4['ff_Rate_Nov'] = f4['ff_Rate_Nov'].map('{:,.0f}%'.format)


f4['OCT_Booked_GMV'] = f4['OCT_Booked_GMV'].map('₹{:,.0f}'.format)
f4['OCT_Shipped_GMV'] = f4['OCT_Shipped_GMV'].map('₹{:,.0f}'.format)
f4['ff_Rate_Oct'] = f4['ff_Rate_Oct'].map('{:,.0f}%'.format)

f4['TOtal_GMV'] = f4['TOtal_GMV'].map('₹{:,.0f}'.format)
f4['Total_Shipped_GMV'] = f4['Total_Shipped_GMV'].map('₹{:,.0f}'.format)




mydb.close()


f5=f5.to_string(index=False,header=False)



body1 = '''<html><head>
<b>Dear All,</b><br>
<b>Please find below the MP FF summary.</b><br>
</head>

<body>
        {0}
</body>

</html>'''.format(build_table(f1, 'yellow_light',font_size='medium'))

body2 = '''<html>
<head>
</head>

<body>
        {0}
</body>

</html>'''.format(build_table(f2, 'blue_light'))

body3= '''<html>
<head>
</head>

<body>
        {0}
</body>

</html>'''.format(build_table(f3, 'green_light'))


body4='''<html>
<head>
</head>

<body>
        {0}
</body>

</html>'''.format(build_table(f4, 'green_light'))




HTMLBody =  body1 + "<br/><br/>" + body2 + "<br/><br/>" + body3+ "<br/><br/>" + body4

#text= " Dear All,\n Please find the below MP FF summary. "

# Define email sender and receiver
email_receiver = ['harsh.singh@industrybuying.com']
#,'ashwani.kumar@industrybuying.com','shubh.birla@industrybuying.com','puneet.adlakha@industrybuying.com']
subject = 'SCM DAILY REPORT-1|PENDENCY|' + f5

#html_management and body

em = MIMEMultipart('multipart') 
em['From'] = ', '.join(email_sender)
em['To'] = ', '.join(email_receiver)
#em['Date'] = formatdate(localtime = True)
em['Subject'] = subject
part2 = MIMEText(HTMLBody, 'html')
#part1 = MIMEText(text, 'plain')


#em.attach(part1)
em.attach(part2)




# Add SSL (layer of security)
context = ssl.create_default_context()


# Log in and send the email
with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
    smtp.login(email_sender, email_password)
    smtp.sendmail(email_sender, email_receiver, em.as_string())
    smtp.quit()



print("SCM1 Done...!!!")
