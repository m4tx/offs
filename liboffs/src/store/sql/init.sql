CREATE TABLE IF NOT EXISTS file
(
    -- ID and name
    id                VARCHAR(64) PRIMARY KEY NOT NULL,
    parent            VARCHAR(64),
    name              VARCHAR(512)            NOT NULL,

    -- File version
    dirent_version    INTEGER                 NOT NULL,
    content_version   INTEGER                 NOT NULL,
    retrieved_version INTEGER                 NOT NULL DEFAULT 0,

    -- Type and permissions
    file_type         INTEGER                 NOT NULL,
    mode              INTEGER                 NOT NULL,
    dev               INTEGER                 NOT NULL,
    size              INTEGER                 NOT NULL,

    -- Timestamps
    atim              INTEGER                 NOT NULL,
    atimns            INTEGER                 NOT NULL,
    mtim              INTEGER                 NOT NULL,
    mtimns            INTEGER                 NOT NULL,
    ctim              INTEGER                 NOT NULL,
    ctimns            INTEGER                 NOT NULL,

    FOREIGN KEY (parent) REFERENCES file (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_path ON file (parent, name);

CREATE TABLE IF NOT EXISTS blob
(
    id      VARCHAR(64) PRIMARY KEY NOT NULL,
    content BLOB                    NOT NULL
);

CREATE TABLE IF NOT EXISTS chunk
(
    file    VARCHAR(64) NOT NULL,
    blob    VARCHAR(64) NOT NULL,
    "index" INT         NOT NULL,

    FOREIGN KEY (file) REFERENCES file (id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (file, "index")
);
