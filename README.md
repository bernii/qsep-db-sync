qsep-db-sync
===========================
**A Java updater/sync tool for QSEP by QBS (http://www.qbs.com.pl/)**

## I. What is it?

QSEP database server integration lib. Allows the retrieval of database data from custom QSEP database used in products of QBS company (http://www.qbs.com.pl/). This lib was created using tech docs delivered by QBS (sorry but can not share them). Took some time to implement in Java ;)

This lib is being used to sync data between QSEP database and internet e-commerce site. The main and most interesting part can be found in **QSepIntegration** file.

The main challenge was during development was that there were lacks in the docs that I recieved and that you have to communicate with DB with 32-bit integers (pretty low level stuff).

Features:
 * get all the tables in the database (db structure)
 * read whole table data 
 * **bulk read** whole table data (during development it turned out that reading 70k+ rows with standard method was really ineficcent)

Currently there are no write operations supported. 
Code is a bit quick & dirty but if there will be a need to implement some new features is will be refactored. If you'll need to create some sync tool or read something from QBS DB it will be a huge help anyway.


## II. Want to know more?
Feel free to [contact me](mailto:berni+githubqsep@extensa.pl) if you have some thoughts, problems or you just want to say hi ;)
