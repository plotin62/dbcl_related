#
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
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm/ga_remarket_list_ids/data@1' AS
#   SELECT user_list_id, inga_xadvertiser_id
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
#     GROUP@50 BY 1) b
#   ON user_list_id = list_id
#   GROUP@50 BY 1,2;
#
#   DEFINE TABLE ga_remarket_list_ids /cns/ig-d/home/aredakov/david_dbm/ga_remarket_list_ids/data*;
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
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm/advertisers_revenue/data@1000' AS
#   SELECT
#     xbid_dimensions.xbid_advertiser_id AS advertiser_id,
#     xbid_dimensions.xbid_exchange_id AS exchange_id,
#     xbid_dimensions.system.xbid_mobile_type AS mobile_type,
#     xbid_dimensions.system.xbid_mobile_page_layout AS page_layout,
#     inga_xadvertiser_id,
#     SUM(xbid_metrics.cost.xbid_revenue_usd_nanos) / 1000000000 AS revenue
#   FROM xbid_reporting.abv.20160529
#   LEFT JOIN@1000 ga_remarket_list_ids
#   ON xbid_dimensions.xbid_advertiser_id = inga_xadvertiser_id
#   WHERE NVL(xbid_dimensions.xbid_discard_reason,0) = 0
#     AND xbid_dimensions.xbid_partner_id > 0
#   GROUP@1000 BY 1,2,3,4,5;
#
#   DEFINE TABLE advertisers_revenue /cns/ig-d/home/aredakov/david_dbm/advertisers_revenue/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm/advertisers_revenue_flags/data@50' AS
#   SELECT a.advertiser_id AS advertiser_id, exchange_id, inga_xadvertiser_id, revenue,
#     service_channel, service_country_code, vertical, mobile_type, page_layout
#   FROM
#     (SELECT INT64(uaid_source_account_id_string) AS advertiser_id,
#       service_channel,
#       service_country_code,
#       vertical
#     FROM XP_DailyCurrentStats_F
#     WHERE product_group = "DBM"
#       AND service_channel != 'AUXILIARY'
#       AND service_country_code = 'US'
#     GROUP BY 1,2,3,4) a
#   JOIN@50 advertisers_revenue b
#   ON a.advertiser_id = b.advertiser_id;
#
#   # VIDEO non VIDEO
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm/advertisers_revenue_flags_creativetype/data@50' AS
#   SELECT creativeId.advertiser_id AS advertiser_id, creative.creative_type AS creative_type
#   FROM xbid_fe_ui.Creatives
#   GROUP@50 BY 1,2;
#
#   DEFINE TABLE advertisers_revenue_flags /cns/ig-d/home/aredakov/david_dbm/advertisers_revenue_flags/data*;
#   DEFINE TABLE creativetype /cns/ig-d/home/aredakov/david_dbm/advertisers_revenue_flags_creativetype/data*;
#   MATERIALIZE '/cns/ig-d/home/aredakov/david_dbm/advertisers_revenue_flags_creativetype/data@50' AS
#   SELECT creative_type, b.advertiser_id AS advertiser_id, b.exchange_id AS exchange_id,
#     inga_xadvertiser_id, service_channel, service_country_code,
#     vertical, mobile_type, page_layout, SUM(revenue) AS revenue
#   FROM creativetype a
#   JOIN@ 50 advertisers_revenue_flags b
#   ON a.advertiser_id = b.advertiser_id
#   GROUP@50 BY 1,2,3,4,5,6,7,8,9;
#
# EOF
#
# }
#
# dbm_study_dremel

function dbm_study_r() {

  local svg="$( tempfile --prefix model --suffix .svg )"
  local svga="$( tempfile --prefix model --suffix .svg )"
  local cond=$1

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

  # Comparing exchanges, IN ('COMPUTER','TABLET')
  DremelAddTableDef('using_ga', '/cns/ig-d/home/aredakov/david_dbm/advertisers_revenue_flags_creativetype/data*', myConn, verbose=FALSE)
  t <- DremelExecuteQuery("
    SELECT exchange_id, ROUND(revenue/total_revenue,5) AS share_revenue
      FROM
      (SELECT advertiser_id, exchange_id, revenue
        FROM using_ga
        WHERE inga_xadvertiser_id > 0
        ${cond}) a
      JOIN@50
     (SELECT advertiser_id, SUM(revenue) AS total_revenue
      FROM using_ga
      WHERE inga_xadvertiser_id > 0
        ${cond}
      GROUP@50 BY 1) b
      ON a.advertiser_id = b.advertiser_id
  ;", myConn)
  t <- na.omit(t)

  c <- DremelExecuteQuery("
    SELECT exchange_id, ROUND(revenue/total_revenue,5) AS share_revenue
      FROM
      (SELECT advertiser_id, exchange_id, revenue
        FROM using_ga
        WHERE inga_xadvertiser_id IS NULL
        ${cond}) a
      JOIN@50
     (SELECT advertiser_id, SUM(revenue) AS total_revenue
      FROM using_ga
      WHERE inga_xadvertiser_id IS NULL
      ${cond}
      GROUP@50 BY 1) b
      ON a.advertiser_id = b.advertiser_id
  ;", myConn)
  c <- na.omit(c)

  # Boxplots
  t <- t[ which(t\$exchange_id == 'xbid-adx'),]
  c <- c[ which(c\$exchange_id == 'xbid-adx'),]

  svg("${svg}")
  layout(matrix(c(1,2), nrow = 1, ncol = 2, byrow = TRUE))
  boxplot(t\$share_revenue,
  main="% of Revenue Spent on AdX",
  sub='',
  ylab="Advertisers Using Google Analytics Remarketing",
  outline=FALSE,
  col="gold")

  boxplot(c\$share_revenue,
  main="% of Revenue Spent on AdX",
  ylab="Advertisers Not-Using Google Analytics Remarketing",
  sub='',
  outline=FALSE,
  col="gold")
  dev.off()

  # t-test
  sink("/tmp/ttest.txt")
  t.test(t\$share_revenue, c\$share_revenue)
  sink()

EOF

  echo ''| sendgmr --subject="${cond}" --attachment_files="${svg}","/tmp/ttest.txt" \
  --to=aredakov

}

for service_channel in LCS SBS; do
  for mobile_type in "('COMPUTER','TABLET')" "('SMARTPHONE')"; do
    for creative_cond in "creative_type != 'VIDEO'" "creative_type == 'VIDEO'"; do
      for page_layout_cond in "page_layout NOT IN (3,6,7)" "page_layout IN (3,6,7)"; do
        cond="
        AND service_channel = '$service_channel'
        AND mobile_type IN $mobile_type
        AND $creative_cond
        AND $page_layout_cond
        "
        dbm_study_r "$cond"
      done
    done
  done
done
