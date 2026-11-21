-- ticketbench: MariaDB schema, 12 tables, InnoDB. Same columns as PostgreSQL.
-- JSON is an alias of LONGTEXT with a JSON_VALID check. wydarzenia.gatunek is
-- a virtual column over metadane so the genre can be indexed.
-- No foreign keys: InnoDB would index every FK column automatically.

DROP TABLE IF EXISTS platnosci, bilety, rezerwacje, kody_rabatowe, klienci, cenniki,
    wydarzenia, miejsca, sektory, obiekty, organizatorzy, kategorie;

CREATE TABLE kategorie (
    id    BIGINT PRIMARY KEY,
    nazwa VARCHAR(64) NOT NULL
) ENGINE = InnoDB;

CREATE TABLE organizatorzy (
    id    BIGINT PRIMARY KEY,
    nazwa VARCHAR(128) NOT NULL,
    nip   VARCHAR(10) NOT NULL,
    email VARCHAR(128) NOT NULL
) ENGINE = InnoDB;

CREATE TABLE obiekty (
    id        BIGINT PRIMARY KEY,
    nazwa     VARCHAR(128) NOT NULL,
    miasto    VARCHAR(64) NOT NULL,
    adres     VARCHAR(128) NOT NULL,
    pojemnosc INT NOT NULL,
    metadane  JSON NOT NULL
) ENGINE = InnoDB;

CREATE TABLE sektory (
    id               BIGINT PRIMARY KEY,
    obiekt_id        BIGINT NOT NULL,
    nazwa            VARCHAR(16) NOT NULL,
    typ              VARCHAR(16) NOT NULL,
    rzedy            INT NOT NULL,
    miejsc_w_rzedzie INT NOT NULL
) ENGINE = InnoDB;

CREATE TABLE miejsca (
    id        BIGINT PRIMARY KEY,
    sektor_id BIGINT NOT NULL,
    rzad      INT NOT NULL,
    numer     INT NOT NULL
) ENGINE = InnoDB;

CREATE TABLE wydarzenia (
    id               BIGINT PRIMARY KEY,
    organizator_id   BIGINT NOT NULL,
    kategoria_id     BIGINT NOT NULL,
    obiekt_id        BIGINT NOT NULL,
    nazwa            VARCHAR(160) NOT NULL,
    data_rozpoczecia DATETIME(6) NOT NULL,
    status           VARCHAR(16) NOT NULL CHECK (status IN ('planowane', 'odwolane')),
    metadane         JSON NOT NULL,
    gatunek          VARCHAR(64) AS (JSON_UNQUOTE(JSON_EXTRACT(metadane, '$.gatunek'))) VIRTUAL
) ENGINE = InnoDB;

CREATE TABLE cenniki (
    id            BIGINT PRIMARY KEY,
    wydarzenie_id BIGINT NOT NULL,
    sektor_id     BIGINT NOT NULL,
    cena          DECIMAL(10, 2) NOT NULL,
    waluta        CHAR(3) NOT NULL
) ENGINE = InnoDB;

CREATE TABLE klienci (
    id          BIGINT PRIMARY KEY,
    imie        VARCHAR(64) NOT NULL,
    nazwisko    VARCHAR(64) NOT NULL,
    email       VARCHAR(128) NOT NULL,
    telefon     VARCHAR(16) NOT NULL,
    miasto      VARCHAR(64) NOT NULL,
    utworzono   DATETIME(6) NOT NULL,
    preferencje JSON NOT NULL
) ENGINE = InnoDB;

CREATE TABLE kody_rabatowe (
    id         BIGINT PRIMARY KEY,
    kod        VARCHAR(32) NOT NULL,
    procent    INT NOT NULL,
    wazny_od   DATETIME(6) NOT NULL,
    wazny_do   DATETIME(6) NOT NULL,
    limit_uzyc INT NOT NULL
) ENGINE = InnoDB;

CREATE TABLE rezerwacje (
    id              BIGINT PRIMARY KEY,
    klient_id       BIGINT NOT NULL,
    utworzono       DATETIME(6) NOT NULL,
    status          VARCHAR(16) NOT NULL CHECK (status IN ('oczekujaca', 'oplacona', 'anulowana', 'wygasla')),
    wygasa_o        DATETIME(6) NULL,
    kwota           DECIMAL(12, 2) NOT NULL,
    kod_rabatowy_id BIGINT NULL,
    metadane        JSON NOT NULL
) ENGINE = InnoDB;

CREATE TABLE bilety (
    id             BIGINT PRIMARY KEY,
    wydarzenie_id  BIGINT NOT NULL,
    miejsce_id     BIGINT NOT NULL,
    cena           DECIMAL(10, 2) NOT NULL,
    status         VARCHAR(16) NOT NULL CHECK (status IN ('wolny', 'zablokowany', 'zarezerwowany', 'sprzedany')),
    rezerwacja_id  BIGINT NULL,
    zablokowany_do DATETIME(6) NULL,
    metadane       JSON NOT NULL
) ENGINE = InnoDB;

CREATE TABLE platnosci (
    id            BIGINT PRIMARY KEY,
    rezerwacja_id BIGINT NOT NULL,
    kwota         DECIMAL(12, 2) NOT NULL,
    metoda        VARCHAR(16) NOT NULL,
    status        VARCHAR(16) NOT NULL,
    zaplacono     DATETIME(6) NOT NULL,
    metadane      JSON NOT NULL
) ENGINE = InnoDB;
