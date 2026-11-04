-- Requêtes d'exemple sur le lac. La vue `rc` est créée par `wikistream sql` :
--   create view rc as select * from read_parquet('data/lake/recentchange/date=*/hour=*/part-*.parquet', hive_partitioning = true);
-- Lancer : .venv/bin/python -m wikistream sql --file sql/requetes.sql

-- Volume par heure (partitions Hive date/hour, temps d'événement)
select date, hour, count(*) as evenements from rc group by all order by all;

-- Wikis les plus actifs, part de bots, octets ajoutés et retirés
select wiki,
       count(*) as modifs,
       round(avg(bot::int), 3) as part_bots,
       sum(greatest(len_new - coalesce(len_old, 0), 0)) as octets_ajoutes,
       sum(greatest(coalesce(len_old, 0) - len_new, 0)) as octets_retires,
       sum((type = 'new')::int) as nouvelles_pages
from rc where type in ('edit', 'new')
group by wiki order by modifs desc limit 10;

-- Articles qui ont attiré le plus de contributeurs humains distincts
select wiki, title, count(*) as modifs, count(distinct "user") as contributeurs
from rc where type in ('edit', 'new') and not bot and ns = 0
group by all order by contributeurs desc, modifs desc limit 10;

-- Latence : émission Wikimedia -> réception, et -> écriture Parquet (secondes)
select quantile_cont(epoch(recv_time) - epoch(meta_dt), [0.5, 0.99]) as emission_reception,
       quantile_cont(epoch(write_time) - epoch(meta_dt), [0.5, 0.99]) as emission_ecriture
from rc where meta_dt is not null;

-- Contrôle d'idempotence : doit renvoyer 0
select count(*) - count(distinct id) as doublons from rc;
