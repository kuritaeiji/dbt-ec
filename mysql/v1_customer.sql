-- 1) 顧客テーブルの作成（べき等）
CREATE TABLE IF NOT EXISTS customer (
  id   VARCHAR(255)  NOT NULL,
  name VARCHAR(255)  NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2) 最初の3件を投入（存在すれば上書き）→ べき等
INSERT INTO customer (id, name)
VALUES
  ('c001', 'Alice'),
  ('c002', 'Bob'),
  ('c003', 'Carol')
AS new
ON DUPLICATE KEY UPDATE
  name = new.name;