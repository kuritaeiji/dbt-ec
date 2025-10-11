-- c002 を更新（同じ値に再実行しても同じなのでべき等）
UPDATE customer
  SET name = 'Bob Prime'
WHERE id = 'c002';

-- c003 を削除（存在しなくてもエラーにならず、べき等）
DELETE FROM customer
WHERE id = 'c003';

-- 新規 c004 を追加（存在すれば同名で上書き）→ べき等
INSERT INTO customer (id, name)
VALUES
  ('c004', 'Dave')
AS new
ON DUPLICATE KEY UPDATE
  name = new.name;