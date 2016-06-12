SET DR=ACTIVE;

-- partitioned table
CREATE TABLE xdcr_partitioned
(
  clusterid  bigint             NOT NULL
, txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, key        varbinary(16)      NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_p PRIMARY KEY
  (
    cid, rid
  )
);
PARTITION TABLE xdcr_partitioned ON COLUMN cid;
CREATE INDEX P_CIDINDEX ON xdcr_partitioned (cid);
CREATE ASSUMEUNIQUE INDEX P_KEYINDEX ON xdcr_partitioned (key);

-- replicated table
CREATE TABLE xdcr_replicated
(
  clusterid  bigint             NOT NULL
, txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, key        varbinary(16)       NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_r PRIMARY KEY
  (
    cid, rid
  )
);
CREATE INDEX R_CIDINDEX ON xdcr_replicated (cid);
CREATE UNIQUE INDEX R_KEYINDEX ON xdcr_replicated (key);

DR TABLE xdcr_partitioned;
DR TABLE xdcr_replicated;

-- base procedures you shouldn't call
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertBaseProc;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.ReplicatedInsertBaseProc;

-- real procedures
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertPartitionedSP;
PARTITION PROCEDURE InsertPartitionedSP ON TABLE xdcr_partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.UpdatePartitionedSP;
PARTITION PROCEDURE InsertPartitionedSP ON TABLE xdcr_partitioned COLUMN cid;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertReplicatedMP;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.UpdateReplicatedMP;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.ReadSP;
PARTITION PROCEDURE ReadSP ON TABLE xdcr_partitioned COLUMN cid;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.ReadMP;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.Summarize;