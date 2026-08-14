#!/bin/sh

cat > /work/url_shortener/releases/0.0.1/sys.config <<EOF2
[{url_shortener,[]},
{kernel, [
  {logger_level, debug},
  {logger, [
    {handler, debug_log, logger_std_h,
      #{level => debug,
        filters => [{debug_filter, {fun logger_filters:level/2, {stop, neq, debug}}}],
        formatter => {logger_formatter,
                      #{single_line => false,
                        template => [time, " [", level, "] ", pid, "@", mfa, ":", line, "\n    ", msg, "\n"]
                       }
                     },
        config => #{type => {file, "data/log/debug.log"},
                    max_no_bytes => 104857600, %100 Mb
                    max_no_files => 10
                   }
      }},
    {handler, info_log, logger_std_h,
      #{level => info,
        filters => [{info_filter, {fun logger_filters:level/2, {stop, neq, info}}}],
        formatter => {logger_formatter,
                      #{single_line => false,
                        template => [time, " [", level, "] ", pid, "@", mfa, ":", line, "\n    ", msg, "\n"]
                       }
                     },
        config => #{type => {file, "data/log/info.log"},
                    max_no_bytes => 104857600, %100 Mb
                    max_no_files => 10
                   }
      }},
    {handler, warning_log, logger_std_h,
      #{level => warning,
        filters => [{warning_filter, {fun logger_filters:level/2, {stop, neq, warning}}}],
        formatter => {logger_formatter,
                      #{single_line => false,
                        template => [time, " [", level, "] ", pid, "@", mfa, ":", line, "\n ", file, "\n ", msg, "\n"]
                       }
                     },
        config => #{type => {file, "data/log/warning.log"},
                    max_no_bytes => 104857600, %100 Mb
                    max_no_files => 10
                   }
      }},
    {handler, error_log, logger_std_h,
      #{level => error,
        filters => [{error_filter, {fun logger_filters:level/2, {stop, neq, error}}}],
        formatter => {logger_formatter,
                      #{single_line => false,
                        template => [time, " [", level, "] ", pid, "@", mfa, ":", line, "\n ", file, "\n ", msg, "\n"]
                       }
                     },
        config => #{type => {file, "data/log/error.log"},
                    max_no_bytes => 104857600, %100 Mb
                    max_no_files => 10
                   }
      }}
  ]}
]},
{brod,
  [
    {clients,
      [
        {shorturl,
          [ { endpoints, [{"${KAFKA1}", ${KAFKA_PORT}}, {"${KAFKA2}", ${KAFKA_PORT}}, {"${KAFKA3}", ${KAFKA_PORT}}] },
            { query_api_versions, false }
          ]
        },
        {clicker,
          [ { endpoints, [{"${KAFKA1}", ${KAFKA_PORT}}, {"${KAFKA2}", ${KAFKA_PORT}}, {"${KAFKA3}", ${KAFKA_PORT}}] },
            { query_api_versions, false }
          ]
        }
      ]
    }
  ]
},
{grpcbox, [
    {client, #{
        channels => [
            {rocksdb_shard1,  [{http, "${ROCKSDB_SHARD1}", 8098, []}], #{}},
            {rocksdb_shard2,  [{http, "${ROCKSDB_SHARD2}", 8098, []}], #{}},
            {rocksdb_shard3,  [{http, "${ROCKSDB_SHARD3}",  8098, []}], #{}},
            {rocksdb_backup1, [{http, "${ROCKSDB_BACKUP1}",  8098, []}], #{}}
        ]
    }}
]}
].

EOF2

exec /work/url_shortener/bin/url_shortener foreground
