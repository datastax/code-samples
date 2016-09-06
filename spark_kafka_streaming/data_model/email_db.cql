CREATE KEYSPACE IF NOT EXISTS email_db WITH replication = {'class':'NetworkTopologyStrategy', 'Analytics':1};

CREATE TABLE IF NOT EXISTS email_db.email_msg_tracker (
    msg_id text,
    tenant_id uuid,
    mailbox_id uuid,
    time_delivered timestamp,
    time_forwarded timestamp,
    time_read timestamp,
    time_replied timestamp,
    PRIMARY KEY ((msg_id, tenant_id), mailbox_id)
) WITH CLUSTERING ORDER BY (mailbox_id ASC);
