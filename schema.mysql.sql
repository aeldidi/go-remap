CREATE TABLE IF NOT EXISTS remap_keys (
    id       INTEGER NOT NULL AUTO_INCREMENT,
    -- 1 string, 2 integer, 3 float64, 4 boolean, 5 object, 6 array
    maptype  TINYINT NOT NULL,
    mapkey   TEXT NOT NULL CHARACTER SET utf8mb4,
    PRIMARY KEY (id),
    UNIQUE (mapkey)
);

CREATE TABLE IF NOT EXISTS remap_values (
    id       INTEGER NOT NULL,
    mapvalue TEXT NOT NULL CHARACTER SET utf8mb4,
    PRIMARY KEY (id),
    FOREIGN KEY (id) REFERENCES remap_keys (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS remap_objects (
    id       INTEGER NOT NULL,
    field    TEXT NOT NULL CHARACTER SET utf8mb4,
    mapvalue TEXT CHARACTER SET utf8mb4,
    PRIMARY KEY (id, field),
    FOREIGN KEY (id) REFERENCES remap_keys (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS remap_arrays (
    id          INTEGER NOT NULL,
    array_index INTEGER NOT NULL,
    mapvalue    TEXT CHARACTER SET utf8mb4,
    PRIMARY KEY (id, array_index),
    FOREIGN KEY (id) REFERENCES remap_keys(id)
        ON DELETE CASCADE
);

