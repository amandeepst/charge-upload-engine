select /*+ :hints */
trim(source_id) as sourceId,
trim(frequency_id) as frequencyIdentifier,
cutoff_dt as cutoffDate,
ora_hash(source_id, :partitions) AS partitionId
from
vw_rec_idfr
WHERE ilm_dt < CAST(:high as DATE) -- hit required partition
AND ilm_dt >= CAST (:low as DATE)
AND ilm_dt < :high
AND ilm_dt >= :low