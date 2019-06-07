CREATE TABLE IF NOT EXISTS journal
(
    id        INTEGER PRIMARY KEY,
    file      VARCHAR(64) NOT NULL,

    operation BLOB        NOT NULL
);
