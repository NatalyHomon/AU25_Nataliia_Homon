/*	RESULT
 * Space consumption of ‘table_to_delete’ table BEFORE ANY OPERATOION
 * total_bytes 602480640, toast_bytes 8192, table_bytes 602472448, total 575mb, toast 8192 bytes table 575mb
 * toast_bytes 8192 => 8 KB is the minimum amount of space that PostgreSQL allocates for a table or a TOAST structure.
 * OCCUPIED SAPCE 575 MB
 * 
 * Space consumption of ‘table_to_delete’ table AFTER DELETE (1/3 rows were deleted)
 *  total_bytes 602611712, table_bytes 602603520, total 575mb, toast 8192 bytes table 575mb
 * OCCUPIED SAPCE 575 MB => space wasn't cleaned, left the same
 * 
 * Space consumption of ‘table_to_delete’ table AFTER VACUUM FULL VERBOSE
 * total_bytes 401580032, toast_bytes 8192, table_bytes 401571840, total 383 MB, toast 8192 bytes table 383 MB
 * OCCUPIED SAPCE 383 MB => Memory has been released, the data has been physically removed from the database.
 * 
 * Space consumption of ‘table_to_delete’ table AFTER TRUNCATE
 * total_bytes 0, toast_bytes 8192, table_bytes 0, total 0mb, toast 8192 bytes table 0mb
 * CCUPIED SAPCE 0 MB => means that the table is essentially empty — it contains no indexes or data — and the 8 KB is the minimum amount of space that PostgreSQL allocates for a table or a TOAST structure
 * 
 * Duration of each operation
 * DELETE (1/3 rows were deleted)- 10s
 * VACUUM FULL VERBOSE -9.8s
 * TRUNCATE -0s
 * 
 * DELETE (all rows)-55s
 */



CREATE TABLE table_to_delete AS
               SELECT 'veeeeeeery_long_string' || x AS col
               FROM generate_series(1,(10^7)::int) x; -- generate_series() creates 10^7 rows of sequential numbers from 1 to 10000000 (10^7)
               
               --execute time 21s
               
  --check table             
 SELECT *
 FROM table_to_delete;
  
--  reviewed space              
SELECT *, pg_size_pretty(total_bytes) AS total,
                                    pg_size_pretty(index_bytes) AS INDEX,
                                    pg_size_pretty(toast_bytes) AS toast,
                                    pg_size_pretty(table_bytes) AS TABLE
               FROM ( SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
                               FROM (SELECT c.oid,nspname AS table_schema,
                                                               relname AS TABLE_NAME,
                                                              c.reltuples AS row_estimate,
                                                              pg_total_relation_size(c.oid) AS total_bytes,
                                                              pg_indexes_size(c.oid) AS index_bytes,
                                                              pg_total_relation_size(reltoastrelid) AS toast_bytes
                                              FROM pg_class c
                                              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                                              WHERE relkind = 'r'
                                              ) a
                                    ) a
               WHERE table_name LIKE '%table_to_delete%';
--total_bytes 602480640, toast_bytes 8192, table_bytes 602472448, total 575mb, toast 8192 bytes table 575mb 


--DELETE operation on ‘table_to_delete’
DELETE FROM table_to_delete
               WHERE REPLACE(col, 'veeeeeeery_long_string','')::int % 3 = 0; -- removes 1/3 of all rows
               
/*1. 10s took time to delete (3333333-rows were deleted)
  2. total_bytes 602611712, toast_bytes 8192, table_bytes 602603520, total 575mb, toast 8192 bytes table 575mb 
  */
       
VACUUM FULL VERBOSE table_to_delete;       --execute time 9.8s
/*   Checked space consumption of the table once again  - space of table was decreased;   
   total_bytes 401580032, toast_bytes 8192, table_bytes 401571840, total 383 MB, toast 8192 bytes table 383 MB           
*/
 TRUNCATE table_to_delete;           
  --executed time 0 
   --total_bytes 0, toast_bytes 8192, table_bytes 0, total 0mb, toast 8192 bytes table 0mb            