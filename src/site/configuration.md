# Configuration

For simple cases (e.g. table-level settings) as-db can be run from the [command-line](usage.html).

For more complex cases as-db can be configured using an XML file. The XML schema is available at [db.xsd](https://github.com/TIBCOSoftware/as-db/blob/master/src/main/resources/db.xsd)

Sample configuration:
 
~~~xml
<database>
  <table name="TABLE1" />
  <table name="TABLE2" schema="SCHEMA1">
    <column name="COLUMN1" type="varchar" size="255" keySequence="1" />
    <column name="COLUMN2" type="timestamp" keySequence="2" />
    <column name="COLUMN3" type="numeric" size="16" decimals="2" />
  </table>
  <table name="TABLE3" space="Space2" catalog="CATALOG1" schema="SCHEMA1">
    <selectSQL>SELECT * FROM TABLE3 WHERE COLUMN3 > 10</selectSQL>
    <countSQL>SELECT COUNT(*) FROM TABLE3 WHERE COLUMN3 > 10</countSQL>
    <insertSQL>INSERT INTO TABLE3 (COLUMN1, COLUMN2, COLUMN3) VALUES
        (?, ?, ?)</insertSQL>
    <column name="COLUMN1" field="Field1" />
    <column name="COLUMN2" field="Field2" />
    <column name="COLUMN3" field="Field3" />
  </table>
</database>
~~~


## Import

If the space name is not set (using the `-space` option on the command line or the `space` attribute on the `table` element in the configuration XML) `as-db` uses the table name.

If the space does not exist it is derived from the table schema: for each column in the table, a field def is created using the following type mapping:

| SQL Type  | Field Type |
|-----------|------------|
| CHAR      | STRING     |
| CLOB      | STRING     |
| VARCHAR   | STRING     |
| SQLXML    | STRING     |
| NUMERIC   | DOUBLE     |
| DECIMAL   | DOUBLE     |
| BIT       | BOOLEAN    |
| BOOLEAN   | BOOLEAN    |
| TINYINT   | INTEGER    |
| SMALLINT  | INTEGER    |
| INTEGER   | INTEGER    |
| BIGINT    | LONG       |
| REAL      | FLOAT      |
| FLOAT     | DOUBLE     |
| DOUBLE    | DOUBLE     |
| BINARY    | BLOB       |
| BLOB      | BLOB       |
| VARBINARY | BLOB       |
| DATE      | DATETIME   |
| TIME      | DATETIME   |
| TIMESTAMP | DATETIME   |


If the table is not configured with a SQL select statement (`selectSQL` attribute in `table` element in configuration XML) it is generated from the table meta-data. 

A query is then executed with that SQL, and each row in the result set is converted into a tuple which is put into the space.


## Export

If the table name is not set (using the `-table` option on the command line or the `name` attribute on the `table` element in the configuration XML) `as-db` uses the space name.

If the table does not exist it is derived from the space definition: for each field definition in the space definition, a column is created using the following type mapping:

| Field Type | SQL Type |
|------------|----------|
| BLOB       | BLOB     |
| BOOLEAN    | NUMERIC  |
| CHAR       | CHAR     |
| DATETIME   | TIMESTAMP|
| DOUBLE     | DOUBLE   |
| FLOAT      | REAL     |
| INTEGER    | INTEGER  |
| LONG       | NUMERIC  |
| SHORT      | SMALLINT |
| STRING     | VARCHAR  |

If the table is not configured with a SQL insert statement (`insertSQL` attribute in `table` element in configuration XML) it is generated from the table's columns.

A prepared statement is then executed with that SQL, and each tuple in the space is inserted into the table. Use `-insert_batch_size` option to enable and set the size of the batch inserts. 