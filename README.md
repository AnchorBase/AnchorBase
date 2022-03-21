<p align="center">
<img width="400" src="https://user-images.githubusercontent.com/57846789/146906105-443f8e0d-b9a9-49c3-b96e-12dc814c983f.png"/>
</p>

---

# ANCHORBASE
AnchorBase is a framework which allows to develop an analytical data warehouse with a flexible data model and with less costs

- [x] Automatic data warehouse deployment based on the given parameters using Sixth normal form (DDL generation + Anchor modeling)
- [x] Integration with relative data source systems (PostgreSQL, MSSQL, MySQL) and data loading
- [x] ETL generation, data loading from source systems, and logging
- [x] SQL-query designer that works with anchor modeling and helps to write less code
- [x] Data warehouse object’s documentation

## PROBLEMS WE SOLVE
Data warehouse development is usually associated with several common problems:
* It is a long and expensive work for high-paid professionals
* It is necessary to consider all business requirements (lots of them) to avoid complicated remakes
* Frequent business model change requires many improvements (even more expensive work)  
* The documentation is often irrelevant or doesn’t even exist

## SOLUTION
AnchorBase makes designing data warehouses much easier:
* AnchorBase generates DDL and ETL automatically once the user provides all the data warehouse parameters they need.
* AnchorBase develops a data warehouse using flexible methodology called Anchor modeling. The last one allows to add or alter data model objects without any influence on existing objects. In cause of that, it is possible to develop a data warehouse iteratively
* AnchorBase generates documentation automatically with the description of every object, the lists of columns, the object’s relations and the link of attributes and source

## DATA WAREHOUSE

AnchorBase develops a data warehouse with the following characteristics:
* An agile database modeling technique - Anchor modeling
* The database management system - PostgreSQL
* The data layers: STG (a source data AS IS), DDS (normalized objects)
* The DWH objects are historical by default (saving the history of the attributes changes)

## SOURCE INTEGRATION
AnchorBase can load data from the following database management systems:
* PostgreSQL
* MSSQL
* MySQL
	
Soon we will also be working with other sources: Oracle, Excel, CSV, Hadoop, MongoDB, etc.

AnchorBase saves the history of every attribute change even if a source system does not do it.

## ETL
Data processing inside DWH:
* An ETL-procedure is generated on every object using SQL
* AnchorBase determines source attribute changes and adds a new row
* A user can specify an object or an attribute in order to update it. Other ETL-procedures will not start
* AnchorBase logs every object procession

## SQL-QUERY DESIGNER
The key disadvantage of the anchor modeling is the large amount of objects and heavy SQL-queries.

AnchorBase solves this problem by creating **SQL-functions**.

![Снимок экрана 2022-02-19 в 20 44 00](https://user-images.githubusercontent.com/57846789/159370705-37b83447-f0b5-45fe-8279-dd20c293b504.png)


## DOCUMENTATION
For full documentation click this [link](https://supabase.com/docs)

<i>Only russian version (English version is comming soon)</i>

## Community & Support
* [Community Forum](https://github.com/AnchorBase/AnchorBase/discussions) - For help with questions, discuss new ideas, view announcements 
* [GitHub Issues](https://github.com/AnchorBase/AnchorBase/issues) - For bags and errors
* Our Email - hello@anchorbase.tech
* [Our Telegram](https://t.me/AnchorBase_official)

## Status
MVP was developped. Now we are looking for customers in order to test AnchorBase for enterprise use-cases

If you want to participate and develop your DWH using AnchorBase, please contact us via Email.
