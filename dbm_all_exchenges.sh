#!/bin/bash

# function dbm_study_dremel() {
#
#   tee /dev/stderr << EOF | dremel
#   SET accounting_group urchin-processing-qa;
#   SET min_completion_ratio 1;
#   SET io_timeout 1200;
#   SET nest_join_schema TRUE;
#   SET runtime_name dremel;
#   SET materialize_overwrite true;
#   SET materialize_owner_group materialize-a-dremel;
#   SET enable_gdrive true;
#
#   DEFINE TABLE audience_stats <<EOF
#     bigtable2:
#       bigtable_name: "/bigtable-replicated/analytics/urchin-processing.audience_stats"
#     default_timestamp_mode: 0
#   EOF;
#
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/ga_remarket_list_ids/data@1' AS
#   SELECT inga_xadvertiser_id
#   FROM
#     (SELECT data.action.dbm_list.metadata.user_list_id AS user_list_id,
#       data.action.dbm_list.backing_service_data.denormalized_xbid_advertiser_id
#       AS inga_xadvertiser_id
#     FROM analytics_configstore.prod.TriggerActions
#     GROUP@50 BY 1,2) a
#   JOIN@50
#    (SELECT list_info.column.cell.Value.list_info.list_id AS list_id
#     FROM FLATTEN(audience_stats, list_info.column.cell.Value.list_info.list_id)
#     WHERE RIGHT(rowkey,1) IN ("X", "M")
#     AND ads_stats.column.name > '20160101'
#     GROUP@50 BY 1) b
#   ON user_list_id = list_id
#   GROUP@50 BY 1;
#
#   # filter for "NVL(xbid_discard_reason, 0) == 0", otherwise you may
#   # doublecount stats.
#   # If xbid_partner_id == 0 then it is a DCM.
#   # xbid_mobile_page_layout
#   # (1,2) THEN "MOBILE_WEB"
#   # (3) THEN "MOBILE_APP"
#   # (6) THEN "MOBILE_APP_INTERSTITIAL"
#   # (7) THEN "VIDEO_MOBILE_APP_INTERSTITIAL"
#   # (0,5) THEN "DESKTOP_WEB"
#   # (4,8,9) THEN "DESKTOP_WEB_VIDEO"
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_revenue_xbid/data@5000' AS
#   SELECT
#     xbid_dimensions.xbid_advertiser_id AS advertiser_id,
#     CASE
#       WHEN xbid_dimensions.system.xbid_mobile_type IN ('COMPUTER','TABLET') THEN 'COMPUTER_TABLET'
#       WHEN xbid_dimensions.system.xbid_mobile_type = 'SMARTPHONE' THEN 'SMARTPHONE'
#       ELSE ''
#     END AS mobile_type,
#     IF(xbid_dimensions.system.xbid_mobile_page_layout IN (3,6,7), 'APP', 'NON_APP') AS page_layout,
#     SUM(xbid_metrics.cost.xbid_revenue_usd_nanos) / 1000000000 AS revenue
#   FROM xbid_reporting.abv.2016
#   WHERE NVL(xbid_dimensions.xbid_discard_reason, 0) = 0
#     AND xbid_dimensions.xbid_partner_id > 0
#     AND xbid_dimensions.xbid_exchange_id != 'xbid-adx'
#     AND xbid_dimensions.xbid_exchange_id != 'xbid-trueview'
#   GROUP@5000 BY 1,2,3;
#
#   DEFINE TABLE advertisers_revenue_xbid /cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_revenue_xbid/data*;
#   DEFINE TABLE ga_remarket_list_ids /cns/ig-d/home/aredakov/david_dbm_all_exchanges/ga_remarket_list_ids/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_in_ga_nonga/data@1' AS
#   SELECT advertiser_id, mobile_type, page_layout, inga_xadvertiser_id,
#     ANY(revenue) AS revenue
#   FROM  advertisers_revenue_xbid a
#   LEFT JOIN@50 ga_remarket_list_ids
#   ON advertiser_id = inga_xadvertiser_id
#   GROUP@50 BY 1,2,3,4;
#
#   DEFINE TABLE advertisers_in_ga_nonga /cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_in_ga_nonga/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_revenue_flags/data@50' AS
#   SELECT a.advertiser_id AS advertiser_id, inga_xadvertiser_id,
#     service_channel, service_country_code, vertical, mobile_type,
#     page_layout, ANY(revenue) AS revenue
#   FROM
#     (SELECT INT64(uaid_source_account_id_string) AS advertiser_id,
#       service_channel,
#       service_country_code,
#       vertical
#     FROM XP_DailyCurrentStats_F
#     WHERE product_group = "DBM"
#       AND service_channel != 'AUXILIARY'
#        AND date_id >= 5844
#     GROUP BY 1,2,3,4) a
#   JOIN@50 advertisers_in_ga_nonga b
#   ON a.advertiser_id = b.advertiser_id
#   GROUP@50 BY 1,2,3,4,5,6,7;
#
#   # VIDEO non VIDEO
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/creativetype/data@50' AS
#   SELECT creativeId.advertiser_id AS advertiser_id,
#     IF(creative.creative_type = 'VIDEO', 'VIDEO','NON_VIDEO') AS creative_type,
#   FROM xbid_fe_ui.Creatives
#   GROUP@50 BY 1,2;
#
#   DEFINE TABLE advertisers_revenue_flags /cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_revenue_flags/data*;
#   DEFINE TABLE creativetype /cns/ig-d/home/aredakov/david_dbm_all_exchanges/creativetype/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_revenue_flags_creativetype/data@50' AS
#   SELECT a.advertiser_id AS advertiser_id, creative_type,
#     inga_xadvertiser_id, service_channel, service_country_code,
#     vertical, mobile_type, page_layout, ANY(revenue) AS revenue
#   FROM creativetype a
#   JOIN@50 advertisers_revenue_flags b
#   ON a.advertiser_id = b.advertiser_id
#   GROUP@50 BY 1,2,3,4,5,6,7,8;
#
#   # Using remarketing non-GA
#   MATERIALIZE '/cns/pa-d/home/aredakov/david_dbm_all_exchanges/ga_dbm_user_list_id/data@1' AS
#   SELECT user_list_id AS ga_dbm_user_list_id
#   FROM
#     (SELECT data.action.dbm_list.metadata.user_list_id AS user_list_id,
#     data.action.dbm_list.backing_service_data.denormalized_xbid_advertiser_id
#     AS inga_xadvertiser_id
#     FROM analytics_configstore.prod.TriggerActions
#     GROUP@50 BY 1,2) a
#   JOIN@50
#     (SELECT list_info.column.cell.Value.list_info.list_id AS list_id
#     FROM FLATTEN(audience_stats, list_info.column.cell.Value.list_info.list_id)
#     WHERE RIGHT(rowkey,1) IN ("X", "M")
#     AND ads_stats.column.name > '20160101'
#     GROUP@50 BY 1) b
#   ON user_list_id = list_id
#   GROUP@50 BY 1;
#
#   DEFINE TABLE list_ids_ingadbm /cns/pa-d/home/aredakov/david_dbm/ga_dbm_user_list_id/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/remarketing_line_items/data@50' AS
#   SELECT line_item_id
#   FROM
#     FLATTEN ((SELECT lineItemId.line_item_id AS line_item_id,
#       targeting.user_list_expr.enable_similar_user_expansion
#         AS enable_similar_user_expansion,
#       targeting.user_list_expr.include.item.criteria_id AS user_list_id
#     FROM FLATTEN(xbid_fe_ui.XbidLineItems,
#       targeting.user_list_expr.include.item.criteria_id)), user_list_id) lis
#   LEFT JOIN@50
#     (SELECT userList.adsdb_user_list_id AS user_list_id
#     FROM xbid_fe_ui.FirstPartyRemarketingLists
#     GROUP BY 1) fp
#     ON lis.user_list_id = fp.user_list_id
#   LEFT JOIN@50
#     (SELECT userList.adsdb_user_list_id AS user_list_id
#     FROM xbid_fe_ui.ThirdPartyRemarketingLists
#     GROUP BY 1) tp
#     ON lis.user_list_id = tp.user_list_id
#   LEFT JOIN@50
#     (SELECT ga_dbm_user_list_id
#     FROM list_ids_ingadbm) ga
#     ON lis.user_list_id = ga.ga_dbm_user_list_id
#   WHERE ga.ga_dbm_user_list_id IS NULL
#     AND (lis.enable_similar_user_expansion IS NOT NULL
#     OR lis.user_list_id IS NOT NULL)
#   GROUP@50 BY 1;
#
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/xbid_lineitems/data@5000' AS
#   SELECT xbid_dimensions.xbid_advertiser_id AS advertiser_id_in_remarketing,
#     xbid_dimensions.xbid_line_item_id AS xbid_line_item_id
#   FROM xbid_reporting.abv.2016
#   WHERE NVL(xbid_dimensions.xbid_discard_reason,0) = 0
#     AND xbid_dimensions.xbid_partner_id > 0
#     AND xbid_dimensions.xbid_exchange_id != 'xbid-adx'
#     AND xbid_dimensions.xbid_exchange_id != 'xbid-trueview'
#   GROUP@5000 BY 1,2;
#
#   DEFINE TABLE remarketing_line_items /cns/ig-d/home/aredakov/david_dbm_all_exchanges/remarketing_line_items/data*;
#   DEFINE TABLE xbid_lineitems /cns/ig-d/home/aredakov/david_dbm_all_exchanges/xbid_lineitems/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertiser_id_in_remarketing/data@50' AS
#   SELECT advertiser_id_in_remarketing
#   FROM xbid_lineitems a
#   JOIN@50 remarketing_line_items b
#   ON xbid_line_item_id = line_item_id
#   GROUP@50 BY 1;
#
#   DEFINE TABLE advertisers_revenue_flags_creativetype /cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertisers_revenue_flags_creativetype/data*;
#   DEFINE TABLE advertiser_id_in_remarketing /cns/ig-d/home/aredakov/david_dbm_all_exchanges/advertiser_id_in_remarketing/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/data_for_r/data@50' AS
#   SELECT advertiser_id, creative_type, inga_xadvertiser_id, service_channel,
#     service_country_code, vertical, mobile_type, page_layout,
#     IF(advertiser_id_in_remarketing > 0, 'using_nonga_remark',
#       'notusing_nonga_remark') AS non_ga_remarketing, ANY(revenue) AS revenue
#   FROM advertisers_revenue_flags_creativetype a
#   LEFT JOIN@50 advertiser_id_in_remarketing b
#   ON a.advertiser_id = b.advertiser_id_in_remarketing
#   GROUP BY 1,2,3,4,5,6,7,8,9;
#
# EOF
#
# }
#
# dbm_study_dremel

function dbm_study_propencities() {

  local svg="$( tempfile --prefix model --suffix .svg )"

  tee /dev/stderr << EOF | R --vanilla
  library(ginstall)
  library(gfile)
  library(namespacefs)
  library(rglib)
  library(cfs)
  library(dremel)
  library(gbm)
  library(Hmisc)
  library(ggplot2)
  InitGoogle()
  options("scipen"=100, "digits"=6)

  myConn <- DremelConnect()
  DremelSetMinCompletionRatio(myConn, 1.0)
  DremelSetAccountingGroup(myConn,'urchin-processing-qa')
  DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
  DremelSetMaterializeOverwrite(myConn, TRUE)
  DremelSetIOTimeout(myConn, 7200)

  DremelAddTableDef('using_ga', '/cns/ig-d/home/aredakov/david_dbm_all_exchanges/data_for_r/data*', myConn, verbose=FALSE)
  t <- DremelExecuteQuery("
    SELECT page_layout, creative_type, mobile_type, advertiser_id,
      non_ga_remarketing, service_country_code,
      vertical, service_channel, revenue, 1 AS treat
    FROM using_ga
    WHERE inga_xadvertiser_id > 0
  ;", myConn)
  t[which(t\$revenue==0),] <- NA
  t <- na.omit(t)

  c <- DremelExecuteQuery("
    SELECT page_layout, creative_type, mobile_type, advertiser_id,
      non_ga_remarketing, service_country_code,
      vertical, service_channel, revenue, 0 AS treat
    FROM using_ga
    WHERE inga_xadvertiser_id IS NULL
  ;", myConn)
  c[which(c\$revenue==0),] <- NA
  c <- na.omit(c)

  t.test(t\$revenue, c\$revenue)
  describe(t\$revenue)
  describe(c\$revenue)

  # Merging.
  f <- merge(t, c, all=TRUE)
  head(f)
  f\$mobile_type  <- factor(f\$mobile_type )
  f\$creative_type <- factor(f\$creative_type)
  f\$page_layout <- factor(f\$page_layout)
  f\$non_ga_remarketing <- factor(f\$non_ga_remarketing)
  f\$service_country_code <- factor(f\$service_country_code)
  f\$vertical <- factor(f\$vertical)
  f\$service_channel <- factor(f\$service_channel)

  # Bernoulli:
  # Estimate propensity score with Generalized Boosted Model.
  # Package GBM uses interaction.depth parameter as a number of splits it
  # has to perform on a tree.
  # 3,000 to 10,000 iterations with shrinkage rates between 0.01 and 0.001.
  # "bernoulli" (logistic regression for 0-1 outcomes),
  # "adaboost" (the AdaBoost exponential loss for 0-1 outcomes).
  gps <- gbm(treat ~ mobile_type + creative_type + page_layout + non_ga_remarketing +
    vertical + service_country_code + service_channel, data = f,
    n.trees=500, train.fraction=0.5, interaction.depth=4,
    distribution="bernoulli", shrinkage=0.001)
  # If type="response" then gbm converts back to the same scale as the outcome.
  f\$gpsvalue <- predict(gps, type="response", n.trees=500)
  f\$weight <- ifelse(f\$treat == 1, 1/f\$gpsvalue, 1/(1-f\$gpsvalue))
  wlm <- lm(revenue ~ treat, data = f, weights= (f\$weight))
  summary(wlm)
  str(wlm)
  names <- c("bernoulli_mean", "p-value", "CILow", "CIup")
  data <- c(
    round(summary(wlm)\$coefficients[2],4),
    round(summary(wlm)\$coefficients[8],7),
    round(summary(wlm)\$coefficients[2] - summary(wlm)\$coefficients[4]*2,4),
    round(summary(wlm)\$coefficients[2] + summary(wlm)\$coefficients[4]*2,4)
    )
   data.frame(names,data)

   # AdaBoost.
  agps <- gbm(treat ~ mobile_type + creative_type + page_layout + non_ga_remarketing +
    vertical + service_country_code + service_channel, data = f,
    n.trees=500, train.fraction=0.5, interaction.depth=4,
    distribution="adaboost", shrinkage=0.001)
  # If type="response" then gbm converts back to the same scale as the outcome.
  f\$agpsvalue <- predict(agps, type="response", n.trees=500)
  f\$aweight <- ifelse(f\$treat == 1, 1/f\$agpsvalue, 1/(1-f\$agpsvalue))
  awlm <- lm(revenue ~ treat, data = f, weights= (f\$aweight))
  summary(awlm)

  f\$treat  <- factor(f\$treat)
  f\$revenue <- as.numeric(f\$revenue)

  tse <- sd(t\$revenue/1000)/sqrt(length(t\$revenue))
  cse <- sd(c\$revenue/1000)/sqrt(length(c\$revenue))

  names <- c("Using GA Remarketing", "Not Using GA Remarketing")
  means <- c(mean(t\$revenue/1000), mean(c\$revenue/1000))
  standardErrors <- c(tse, cse)

  # 95% chance that μ lies between m ± 2SE
  svg("${svg}")
  plotTop <- max(means+standardErrors*2)
  barCenters <- barplot(means,
    names.arg=names,
    main="Average Spend on DBM per Advertiser,
      \n Before Controling Factors",
    ylab="Revenue USD with CI95%, K",
    col=c("red","darkblue"), las=1, ylim=c(0,plotTop))
  segments(barCenters, means-standardErrors*2, barCenters, means+standardErrors*2, lwd=2)
  arrows(barCenters, means-standardErrors*2, barCenters, means+standardErrors*2, lwd=2, angle=90, code=3)
  dev.off()

EOF

  echo ''| sendgmr --subject="All Exchages" --attachment_files="${svg}" \
  --to=aredakov

}

dbm_study_propencities

