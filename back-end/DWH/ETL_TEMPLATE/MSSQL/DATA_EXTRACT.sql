
/*
@@list_of_attributes - list of source attributes which need to be extracted
EXAMPLE: "attribute 1","attribute 2","attribute 3"...
@@schema - name of source schema 
@@table - name of source table
@@increment_sql - sql statement of filtering the source table by increment
EXAMPLE: AND "increment_attribute">CAST(COALESCE('the last increment value','1900-01-01 00:00:00') AS DATETIME)
*/



--data extract from the source 
	SELECT 
	@@list_of_attributes
	FROM "@@schema"."@@table" 
	WHERE 1=1
		@@increment_sql
	;