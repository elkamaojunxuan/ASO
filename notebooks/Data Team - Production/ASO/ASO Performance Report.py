# Databricks notebook source
# MAGIC %run "Frequently Used/tp_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC clear cache

# COMMAND ----------

sql_data('''

with last_month as
(select product_name, product_id, market, country, keyword, current_date as date,
avg(rank) as rank, avg(traffic_share) as traffic_share, avg(search_volume) as search_volume, avg(difficulty) as difficulty
from ds_aso_keyword_lastmonth 
where substr(date,1,7) in (substr(add_months(current_date,-1),1,7),substr(add_months(current_date,-2),1,7),substr(add_months(current_date,-3),1,7)) 
and search_volume!='NaN'
group by 1,2,3,4,5,6
union 
select product_name, product_id, market, country, keyword, date, rank, traffic_share, search_volume, difficulty
from ds_aso_keyword_lastmonth where search_volume!='NaN')

select a.account_name, a.product_name, a.product_id, a.market, a.country, a.keyword, a.date, a.rank, a.traffic_share, b.search_volume, b.difficulty
from market.appannie_keywords a left join last_month b
on a.product_id=b.product_id and a.market=b.market and a.country=b.country and a.keyword=b.keyword and substr(a.date,1,7)=substr(b.date,1,7)
where a.rank<=20 and b.search_volume>=10 or substr(a.date,1,7)=substr(current_date,1,7)
''','aso',1).write.mode("overwrite").format('delta').saveAsTable('ds_aso_keyword_performance')


# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct country_play_store from market.google_acquisition_buyers_7d_play_country

# COMMAND ----------

sql_data('''
select a.game, a.country_play_store, a.date, a.store_listing_visitors, a.installers, c.payer as d10_payer, b.purchase as d10_purchase, b.revenue as d10_revenue
from
(select  
case when lower(package_name) like '%dino%' then 'dino bash'
     when lower(package_name) like '%derby%' then 'photo finish'
     when lower(package_name) like '%terra%' then 'terra genesis'
     when lower(package_name) like '%food%' then 'food truck chef'
     when lower(package_name) like '%siege%' then 'siege'
     when lower(package_name) like '%languinis%' then 'languinis'
     when lower(package_name) like '%darkzone%' then 'operation new earth'
     when lower(package_name) like '%startrek%' then 'star trek timelines'
     when lower(package_name) like '%diesel%' then 'diesel drag racing'
     when lower(package_name) like '%tap%' then 'tap busters'
     when lower(package_name) like '%photofinish%' then 'horse racing manager' 
     when lower(package_name) like '%luckyfish%' then 'jackpot empire slots'
     when lower(package_name) like '%nascar%' then 'nascar heat'
     when lower(package_name) like '%catchidle%' then 'catch idle'
     when lower(package_name) like '%arcana%' then 'the arcana'
     end as game, 
date, country_play_store, store_listing_visitors, installers
from market.google_acquisition_buyers_7d_play_country) a left join 

(select game, upper(country) as country, install_date, sum(purchase) as purchase, sum(net_revenue+total_ad_rev) as revenue
from ds_summary_ua_revenue
where platform='android' and network='organic' and days_installed<=10 and install_date>='2018-07-15'
group by 1,2,3 order by 1,2,3) b on a.game=b.game and a.date=b.install_date and a.country_play_store=b.country left join

(select 
case when game_id_abbr='pf2' then 'horse racing manager'
     when game_id_abbr='lg' then 'languinis'
     when game_id_abbr='tg' then 'terra genesis'
     when game_id_abbr='tb' then 'tap busters' end as game,
locale_country, install_date, 
count(distinct case when total_iap_revenue>0 then device_tpdid end) as payer
from ds_summary_sessions_day
where platform='Android' and network='organic' and datediff(session_date, install_date)<=10 and install_date>='2018-07-15'
group by 1,2,3 order by 1,2,3) c on a.game=c.game and a.date=c.install_date and a.country_play_store=c.locale_country

''','aso_android_performance',1).write.mode('OverWrite').saveAsTable('ds_aso_android_performance')

# COMMAND ----------

sql_data('''
with app_store_prep as 
(select 
case when lower(a.appname) like '%rowing%' then 'championship rowing'
     when lower(a.appname) like '%dino%' then 'dino bash'
     when lower(a.appname) like '%horse%racing%manager%' then 'horse racing manager'
     when lower(a.appname) like '%languinis%' then 'languinis'
     when lower(a.appname) like '%operation%' then 'operation new earch'
     when lower(a.appname) like '%photo%finish%' then 'photo finish'
     when lower(a.appname) like '%rerunners%' then 'rerunners'
     when lower(a.appname) like '%siege%' then 'siege'
     when lower(a.appname) like '%tap%' then 'tap busters'
     when lower(a.appname) like '%terra%' then 'terra genesis' 
     when lower(a.appname) like '%food%' then 'food truck chef'
     when lower(a.appname) like '%leo%' or lower(a.appname) like '%里奥%' then 'leo fortune'
     when lower(a.appname) like '%almost%' then 'almost a hero'       
     when lower(a.appname) like '%star%trek%' then 'star trek timelines'    
     when lower(a.appname) like '%nascar%' then 'nascar heat'
     when lower(a.appname) like '%jackpot%empire%' then 'jackpot empire slots' 
     when lower(a.appname) like '%catch%idle%' then 'catch idle'
     when lower(a.appname) like '%the%arcana%' then 'the arcana'
     end as game, a.country, a.date,
a.impressions, a.clicks, a.installs, b.ratio
from
(select appname, country, date,
sum(impressionsTotalUnique) as impressions, sum(pageViewUnique) as clicks, sum(units) as installs
from market.ios_impressions where lower(sourcetype)='app store search' and impressionsTotalUnique>0
group by 1,2,3) a left join
(select appname, country, date,  
sum(case when sourcetype='App Store Search' then units end)/sum(case when sourcetype in ('App Store Search','App Store Browse') then units end) as ratio
from market.ios_impressions
group by 1,2,3) b on a.appname=b.appname and a.country=b.country and a.date=b.date),

app_store as
(select p.*, payer*ratio as d10_payer, purchase*ratio as d10_purchase, revenue*ratio as d10_revenue
from app_store_prep p left join
(select game, upper(country) as country, install_date, sum(purchase) as purchase, sum(net_revenue+total_ad_rev) as revenue
from ds_summary_ua_revenue
where platform='ios' and network='organic' and days_installed<=10 and install_date>='2018-07-15'
group by 1,2,3 order by 1,2,3) q on p.game=q.game and p.date=q.install_date and p.country=q.country left join
(select 
case when game_id_abbr='pf2' then 'horse racing manager'
     when game_id_abbr='lg' then 'languinis'
     when game_id_abbr='tg' then 'terra genesis'
     when game_id_abbr='tb' then 'tap busters' end as game,
locale_country, install_date, 
count(distinct case when total_iap_revenue>0 then device_tpdid end) as payer
from ds_summary_sessions_day
where lower(platform) like '%os%' and network='organic' and datediff(session_date, install_date)<=10 and install_date>='2018-07-15'
group by 1,2,3 order by 1,2,3) r on p.game=r.game and p.date=r.install_date and p.country=r.locale_country),

apple_search_ads as
(select m.*, o.payer, n.purchase, n.revenue
from
(select game, install_date, upper(country) as country, sum(impressions) as impressions, sum(clicks) as clicks, sum(installs) as installs
from ds_summary_ua_spend where network='apple search ads' and install_date>='2018-07-01'
group by 1,2,3) m left join
(select game, install_date, upper(country) as country, sum(purchase) as purchase, sum(net_revenue+total_ad_rev) as revenue
from ds_summary_ua_revenue where network='apple search ads' and days_installed<=10 and install_date>='2018-07-15'
group by 1,2,3) n on m.game=n.game and m.install_date=n.install_date and m.country=n.country left join
(select 
case when game_id_abbr='pf2' then 'horse racing manager'
     when game_id_abbr='lg' then 'languinis'
     when game_id_abbr='tg' then 'terra genesis'
     when game_id_abbr='tb' then 'tap busters' end as game,
locale_country, install_date,
count(distinct case when total_iap_revenue>0 then device_tpdid end) as payer
from ds_summary_sessions_day where network='apple search ads' and datediff(session_date, install_date)<=10 and install_date>='2018-07-15'
group by 1,2,3 order by 1,2,3) o on m.game=o.game and m.install_date=o.install_date and m.country=o.locale_country)

select x.game, x.date, x.country, 
x.impressions-coalesce(y.impressions,0) as organic_impressions, x.clicks-coalesce(y.clicks,0) as organic_clicks, x.installs-coalesce(y.installs,0) as organic_installs, 
x.d10_payer-coalesce(y.payer,0) as d10_payer, x.d10_purchase-coalesce(y.purchase,0) as d10_purchase, x.d10_revenue-coalesce(y.revenue,0) as d10_revenue 
from app_store x left join apple_search_ads y on x.game=y.game and x.country=y.country and x.date=y.install_date

''','aso_ios_performance',1).write.mode('OverWrite').saveAsTable('ds_aso_ios_performance')

# COMMAND ----------

# tableau_refresh_new('7d1667d6-4247-4559-8104-ab095998c556')

# COMMAND ----------

