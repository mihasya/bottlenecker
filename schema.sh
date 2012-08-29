#!/bin/bash
DEFAULT_RF=3

RF=${RF:-$DEFAULT_RF}

cat <<EOF
CREATE KEYSPACE bottlenecker WITH strategy_class = 'SimpleStrategy' AND strategy_options: replication_factor = ${RF};

USE bottlenecker

CREATE TABLE random_data ("id" varchar PRIMARY KEY, "name" varchar);

EOF
