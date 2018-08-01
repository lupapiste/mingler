#!/usr/bin/env bash

docker exec -i mingler-db-0 mongo << EOF
  rs.initiate( {
   _id : "rs0",
   members: [
      { _id: 0, host: "mingler-db-0:27017" },
      { _id: 1, host: "mingler-db-1:27017" },
      { _id: 2, host: "mingler-db-2:27017" }
   ]
  })
EOF
