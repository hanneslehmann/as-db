# Usage

~~~bash
Usage: as-db [options] [command] [command options]
  Options:
    -config
       Database configuration XML file
    -data_store
       Directory path for the shared-nothing persistence data store
    -debug
       Log level (ERROR, WARNING, INFO, DEBUG or VERBOSE)
       Default: INFO
    -discovery
       URL to be used to discover the metaspace
    -driver
       Database driver class name
    -format_blob
       Blob format ("base64" or "hex")
    -format_boolean_false
       Boolean format for 'false' value e.g. "false"
    -format_boolean_true
       Boolean format for 'true' value e.g. "true"
    -format_datetime
       Date/time format e.g. "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    -format_number
       Number format e.g. "###,###.###"
    -?, -help
       Print this help message
    -identity_password
       Identity password
    -jar
       Path to driver JAR file
    -listen
       URL for the application
    -log
       Log file path
    -log_append
       Append logs onto any existing files
       Default: false
    -log_count
       Number of log files to cycle through
       Default: 1
    -log_debug
       File log level (ERROR, WARNING, INFO, DEBUG or VERBOSE)
       Default: INFO
    -log_limit
       Max number of bytes to write to any log file
    -member_name
       Unique member name
    -metaspace
       Name of the metaspace that the application is to join
    -password
       Database password
    -rx_buffer_size
       TCP buffer size for receiving data
    -security_token
       Security token path
    -time_zone
       Time zone ID e.g. "GMT"
    -url
       Database URL
    -user
       Database user
    -version
       Print this application's version
    -worker_thread_count
       Number of threads that can be used for program invocation
  Commands:
    import      Import tables
      Usage: import [options] The list of tables to import
        Options:
          -batch_size
             Transfer output batch size
          -catalog
             Catalog name
          -count_sql
             Select count query
          -distribution_role
             Distribution role (none, leech, seeder)
          -fetch_size
             Fetch size
          -fields
             Names of specific fields to transfer, e.g. field1 field2
          -limit
             Max number of entries to read from input
          -operation
             Space operation (get, load, none, partial, put, take)
          -schema
             Schema name
          -select_sql
             Select query
          -space
             Space name
          -transfer_thread_count
             Number of worker threads to use for transfer
          -type
             Table type
          -wait_for_ready_timeout
             Wait for ready timeout

    export      Export tables
      Usage: export [options] The list of spaces to export
        Options:
          -batch_size
             Transfer output batch size
          -browser_type
             Browser type
          -catalog
             Catalog name
          -distribution_scope
             Browser distribution scope
          -fields
             Names of specific fields to transfer, e.g. field1 field2
          -filter
             Browser filter
          -insert_sql
             Insert SQL statement
          -limit
             Max number of entries to read from input
          -prefetch
             Browser prefetch
          -query_limit
             Browser query limit
          -schema
             Schema name
          -table
             Table name
          -time_scope
             Browser time scope
          -timeout
             Browser timeout
          -transfer_thread_count
             Number of worker threads to use for transfer
~~~
