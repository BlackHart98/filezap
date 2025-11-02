CREATE TABLE IF NOT EXISTS filezap_chunks(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
    chunk_checksum INTEGER NOT NULL,
    cutpoint INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    file_path TEXT NOT NULL
);
