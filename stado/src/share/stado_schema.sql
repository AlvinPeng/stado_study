CREATE SCHEMA __stado;

CREATE VIEW __stado.pg_database AS
SELECT DISTINCT (string_to_array(datname, '__'))[2] datname, datdba, encoding, 
       datcollate, datctype, datistemplate, datallowconn, datconnlimit,
       datlastsysoid, datfrozenxid, dattablespace, datacl, MIN(OID)
  FROM pg_catalog.pg_database
 WHERE datname like '\_\_%\_\_N%'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12


/*
EXECUTE DIRECT ON ALL 'CREATE VIEW __stado.pg_database AS
SELECT DISTINCT (string_to_array(datname, ''__''))[2] datname, datdba, encoding, 
       datcollate, datctype, datistemplate, datallowconn, datconnlimit,
       datlastsysoid, datfrozenxid, dattablespace, datacl, MIN(oid)
  FROM pg_catalog.pg_database
 WHERE datname like ''\_\_%\_\_N%''
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12'

*/
