BEGIN;
CREATE SCHEMA evidence;
CREATE ROLE bracc_evidence_journal NOLOGIN;
CREATE TABLE evidence.event_lock (
    event_identity text PRIMARY KEY CHECK (event_identity ~ '^[0-9a-f]{64}$')
);
CREATE TABLE evidence.journal (
    event_identity text NOT NULL CHECK (event_identity ~ '^[0-9a-f]{64}$'),
    source_id text NOT NULL,
    dataset_id text NOT NULL,
    record_locator text NOT NULL,
    revision_no integer NOT NULL CHECK (revision_no > 0),
    observation_id text NOT NULL CHECK (observation_id ~ '^[0-9a-f]{64}$'),
    event_id uuid NOT NULL,
    artifact_id uuid NOT NULL,
    artifact_sha256 text NOT NULL CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$'),
    canonical_bytes bytea NOT NULL,
    PRIMARY KEY (event_identity,revision_no),
    UNIQUE (event_identity,observation_id),
    FOREIGN KEY (artifact_id) REFERENCES control.artifact(id)
);
CREATE INDEX journal_record_history ON evidence.journal(source_id,dataset_id,record_locator,revision_no);
CREATE FUNCTION evidence.reject_journal_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,evidence AS $$
BEGIN
    RAISE EXCEPTION 'evidence journal is append-only';
END $$;
CREATE TRIGGER journal_immutable BEFORE UPDATE OR DELETE ON evidence.journal
FOR EACH ROW EXECUTE FUNCTION evidence.reject_journal_mutation();
CREATE FUNCTION evidence.reject_journal_truncate() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,evidence AS $$
BEGIN
    RAISE EXCEPTION 'evidence journal is append-only';
END $$;
CREATE TRIGGER journal_no_truncate BEFORE TRUNCATE ON evidence.journal
FOR EACH STATEMENT EXECUTE FUNCTION evidence.reject_journal_truncate();
GRANT USAGE ON SCHEMA evidence TO bracc_evidence_journal;
GRANT SELECT, INSERT, UPDATE ON evidence.event_lock TO bracc_evidence_journal;
GRANT SELECT, INSERT ON evidence.journal TO bracc_evidence_journal;
GRANT USAGE ON SCHEMA control TO bracc_evidence_journal;
GRANT SELECT ON control.source, control.dataset, control.partition, control.artifact TO bracc_evidence_journal;
GRANT EXECUTE ON FUNCTION evidence.reject_journal_mutation(), evidence.reject_journal_truncate() TO bracc_evidence_journal;
INSERT INTO control.schema_migration(version) VALUES (6);
COMMIT;
