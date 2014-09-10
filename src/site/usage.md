# Usage

~~~bash
Usage: as-db [options] [command] [command options]
  Options:
    -data_store
       Directory path for data store
    -discovery
       Discovery URL
    -driver
       Database driver class name
    -?, -help
       Print this help message
    -jar
       Path to driver JAR file
    -listen
       Listen URL
    -member_name
       Member name
    -metaspace
       Metaspace name
    -no_exit
       Do not shut down after the Eclipse application has ended
    -password
       Database password
    -rx_buffer_size
       Receive buffer size
    -security_token
       Security token path
    -url
       Database URL
    -user
       Database user
    -worker_thread_count
       Worker thread count
  Commands:
    import      Import tables
      Usage: import [options] The list of tables to import
        Options:
          -batch_size
             Transfer output batch size
          -catalog
             Catalog name
          -distribution_role
             Distribution role (none, leech, seeder)
          -fetch-size
             Fetch size
          -operation
             Space operation (get, load, none, partial, put, take)
          -schema
             Schema name
          -sql
             SQL query
          -type
             Table type
          -wait_for_ready_timeout
             Wait for ready timeout
          -writer_thread_count
             Number of writer threads

    export      Export tables
      Usage: export [options] The list of spaces to export
        Options:
          -batch_size
             Transfer output batch size
          -catalog
             Catalog name
          -distribution_scope
             Browser distribution scope
          -filter
             Browser filter
          -prefetch
             Browser prefetch
          -query_limit
             Browser query limit
          -schema
             Schema name
          -time_scope
             Browser time scope
          -timeout
             Browser timeout
          -writer_thread_count
             Number of writer threads
~~~
