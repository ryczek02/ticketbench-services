-- ticketbench: PostgreSQL schema, 12 tables.
-- Foreign keys are deliberately left out: MariaDB/InnoDB creates an index for
-- every foreign key, which would break the "no secondary indexes" baseline.
-- Referential integrity is kept by the application and checked by tests.

DROP TABLE IF EXISTS platnosci, bilety, rezerwacje, kody_rabatowe, klienci, cenniki,
    wydarzenia, miejsca, sektory, obiekty, organizatorzy, kategorie CASCADE;

CREATE TABLE kategorie (
    id    BIGINT PRIMARY KEY,
    nazwa TEXT NOT NULL
);

CREATE TABLE organizatorzy (
    id    BIGINT PRIMARY KEY,
    nazwa TEXT NOT NULL,
    nip   VARCHAR(10) NOT NULL,
    email TEXT NOT NULL
);

CREATE TABLE obiekty (
    id        BIGINT PRIMARY KEY,
    nazwa     TEXT NOT NULL,
    miasto    TEXT NOT NULL,
    adres     TEXT NOT NULL,
    pojemnosc INT NOT NULL,
    metadane  JSONB NOT NULL
);

CREATE TABLE sektory (
    id               BIGINT PRIMARY KEY,
    obiekt_id        BIGINT NOT NULL,
    nazwa            TEXT NOT NULL,
    typ              TEXT NOT NULL,
    rzedy            INT NOT NULL,
    miejsc_w_rzedzie INT NOT NULL
);

CREATE TABLE miejsca (
    id        BIGINT PRIMARY KEY,
    sektor_id BIGINT NOT NULL,
    rzad      INT NOT NULL,
    numer     INT NOT NULL
);

CREATE TABLE wydarzenia (
    id               BIGINT PRIMARY KEY,
    organizator_id   BIGINT NOT NULL,
    kategoria_id     BIGINT NOT NULL,
    obiekt_id        BIGINT NOT NULL,
    nazwa            TEXT NOT NULL,
    data_rozpoczecia TIMESTAMPTZ NOT NULL,
    status           VARCHAR(16) NOT NULL CHECK (status IN ('planowane', 'odwolane')),
    metadane         JSONB NOT NULL
);

CREATE TABLE cenniki (
    id            BIGINT PRIMARY KEY,
    wydarzenie_id BIGINT NOT NULL,
    sektor_id     BIGINT NOT NULL,
    cena          NUMERIC(10, 2) NOT NULL,
    waluta        CHAR(3) NOT NULL
);

CREATE TABLE klienci (
    id          BIGINT PRIMARY KEY,
    imie        TEXT NOT NULL,
    nazwisko    TEXT NOT NULL,
    email       TEXT NOT NULL,
    telefon     TEXT NOT NULL,
    miasto      TEXT NOT NULL,
    utworzono   TIMESTAMPTZ NOT NULL,
    preferencje JSONB NOT NULL
);

CREATE TABLE kody_rabatowe (
    id         BIGINT PRIMARY KEY,
    kod        VARCHAR(32) NOT NULL,
    procent    INT NOT NULL,
    wazny_od   TIMESTAMPTZ NOT NULL,
    wazny_do   TIMESTAMPTZ NOT NULL,
    limit_uzyc INT NOT NULL
);

CREATE TABLE rezerwacje (
    id              BIGINT PRIMARY KEY,
    klient_id       BIGINT NOT NULL,
    utworzono       TIMESTAMPTZ NOT NULL,
    status          VARCHAR(16) NOT NULL CHECK (status IN ('oczekujaca', 'oplacona', 'anulowana', 'wygasla')),
    wygasa_o        TIMESTAMPTZ,
    kwota           NUMERIC(12, 2) NOT NULL,
    kod_rabatowy_id BIGINT,
    metadane        JSONB NOT NULL
);

CREATE TABLE bilety (
    id             BIGINT PRIMARY KEY,
    wydarzenie_id  BIGINT NOT NULL,
    miejsce_id     BIGINT NOT NULL,
    cena           NUMERIC(10, 2) NOT NULL,
    status         VARCHAR(16) NOT NULL CHECK (status IN ('wolny', 'zablokowany', 'zarezerwowany', 'sprzedany')),
    rezerwacja_id  BIGINT,
    zablokowany_do TIMESTAMPTZ,
    metadane       JSONB NOT NULL
);

CREATE TABLE platnosci (
    id            BIGINT PRIMARY KEY,
    rezerwacja_id BIGINT NOT NULL,
    kwota         NUMERIC(12, 2) NOT NULL,
    metoda        VARCHAR(16) NOT NULL,
    status        VARCHAR(16) NOT NULL,
    zaplacono     TIMESTAMPTZ NOT NULL,
    metadane      JSONB NOT NULL
);
