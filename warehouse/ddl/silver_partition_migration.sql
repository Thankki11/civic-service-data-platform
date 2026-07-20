-- Chay MOT LAN tren Spark SQL neu lakehouse.silver.application_history da
-- duoc tao bang phien ban cu khong partition. Bang moi duoc ETL tao san voi
-- spec nay; lenh nay chi danh cho bang da ton tai.
--
-- Iceberg se tao partition spec moi. File cu van doc duoc, nhung muon rewrite
-- file cu de nhan partition pruning toi da thi chay compact/rewrite sau.
ALTER TABLE lakehouse.silver.application_history
    ADD PARTITION FIELD days(action_time);
