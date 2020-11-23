CREATE TABLE device_stats(
    device INTEGER REFERENCES device (_id) NOT NULL PRIMARY KEY,
    last_connected_on TIMESTAMPTZ NOT NULL,
    block_size INTEGER NOT NULL,
    block_count INTEGER NOT NULL,
    vlob_size INTEGER NOT NULL,
    vlob_count INTEGER NOT NULL
);
