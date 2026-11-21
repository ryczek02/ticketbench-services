-- Secondary indexes. Each statement is executed separately; DROP runs in
-- reverse order when indexes are switched off.
CREATE INDEX idx_bilety_wydarzenie_status ON bilety (wydarzenie_id, status);
CREATE INDEX idx_bilety_rezerwacja ON bilety (rezerwacja_id);
CREATE INDEX idx_rezerwacje_klient ON rezerwacje (klient_id, utworzono);
CREATE INDEX idx_rezerwacje_status_wygasa ON rezerwacje (status, wygasa_o);
CREATE INDEX idx_rezerwacje_utworzono ON rezerwacje (utworzono);
CREATE INDEX idx_platnosci_rezerwacja ON platnosci (rezerwacja_id);
CREATE INDEX idx_wydarzenia_obiekt_data ON wydarzenia (obiekt_id, data_rozpoczecia);
CREATE INDEX idx_wydarzenia_organizator ON wydarzenia (organizator_id);
CREATE INDEX idx_wydarzenia_data ON wydarzenia (data_rozpoczecia);
CREATE INDEX idx_obiekty_miasto ON obiekty (miasto);
CREATE INDEX idx_miejsca_sektor ON miejsca (sektor_id);
CREATE INDEX idx_cenniki_wydarzenie ON cenniki (wydarzenie_id, sektor_id);
CREATE INDEX idx_sektory_obiekt ON sektory (obiekt_id);
CREATE INDEX idx_wydarzenia_gatunek ON wydarzenia (gatunek);
