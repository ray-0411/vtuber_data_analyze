CREATE TABLE "main" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    channel TEXT NOT NULL,
    youtube INTEGER NOT NULL,
    twitch INTEGER NOT NULL,
    yt_number INTEGER DEFAULT 0, tw_number INTEGER DEFAULT 0
);

CREATE TABLE stream(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_name TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    url TEXT DEFAULT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL
);

CREATE TABLE streamer(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL UNIQUE,
    channel_name TEXT NOT NULL,
    yt_url TEXT DEFAULT NULL,
    tw_url TEXT DEFAULT NULL, 
    `group` TEXT
);

CREATE TABLE working(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time TEXT NOT NULL UNIQUE,
    finish TEXT NOT NULL,
    timer REAL DEFAULT NULL, 
    kind TEXT DEFAULT NULL, 
    "create" TEXT DEFAULT NULL
);

CREATE TABLE same_stream (
    from_id INTEGER PRIMARY KEY,
    to_id INTEGER NOT NULL,
    time TEXT DEFAULT NULL
);