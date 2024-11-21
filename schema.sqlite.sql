CREATE TABLE IF NOT EXISTS remap_keys (
    id       INTEGER NOT NULL,
    -- 1 string, 2 object, 3 array
    maptype  TINYINT NOT NULL,
    mapkey   TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (mapkey)
);

CREATE TABLE IF NOT EXISTS remap_values (
    id       INTEGER NOT NULL,
    mapvalue TEXT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (id) REFERENCES remap_keys (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS remap_objects (
    id       INTEGER NOT NULL,
    field    TEXT NOT NULL,
    mapvalue TEXT,
    PRIMARY KEY (id, field),
    FOREIGN KEY (id) REFERENCES remap_keys (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS remap_arrays (
    id          INTEGER NOT NULL,
    array_index INTEGER NOT NULL,
    mapvalue    TEXT,
    PRIMARY KEY (id, array_index),
    FOREIGN KEY (id) REFERENCES remap_keys(id)
        ON DELETE CASCADE
);

