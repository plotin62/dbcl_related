#!/bin/bash

function dbm_cpm_dremel() {

  tee /dev/stderr << EOF | dremel
  SET accounting_group urchin-processing-qa;
  SET min_completion_ratio 1;
  SET io_timeout 1200;
  SET nest_join_schema TRUE;
  SET runtime_name dremel;
  SET materialize_overwrite true;
  SET materialize_owner_group materialize-a-dremel;
  SET enable_gdrive true;

  # DBM creatives: dbm_creatives
  # control group (FLASH_INPAGE and HTML5_BANNER)
  MATERIALIZE '/cns/ig-d/home/aredakov/dbm_cpm/creatives/data@50' AS
  SELECT
    creativeId.advertiser_id AS dbm_advertiser_id,
    creativeId.creative_id AS dbm_creative_id,
    creative.creative_type AS dbm_creative_type,
    creative.requires_flash AS dbm_flash_required,
    creative.requires_html5 AS dbm_html5_required,
    creative.hosting_source AS dbm_creative_serving
  FROM xbid_fe_ui.Creatives c
  WHERE creative.width = 300
    AND creative.height = 250
    AND STRFTIME_USEC(creative.creation_date_usec*1000, "%Y%m%d") >= '20160101'
  LIMIT 10;

  # DBM perfomance: dbm_perf
  SELECT
    STRFTIME_USEC(STRING(base_event.google_event_date), '%F') AS event_date,
    xbid_dimensions.xbid_partner_id AS dbm_partner_id,
    xbid_dimensions.xbid_advertiser_id AS dbm_advertiser_id,
    xbid_dimensions.xbid_line_item_id AS dbm_line_item_id,
    xbid_dimensions.xbid_creative_id AS dbm_creative_id,
    xbid_dimensions.auction_type AS dbm_auction_type,
    xbid_dimensions.xbid_exchange_id AS dbm_exchange_id,
    device.platform_type AS platform_type,
    device.rendering_environment_type AS rendering_environment,
    SUM(rich_media.time) AS rich_media_time,
    SUM(rich_media.count) AS rich_media_count,
    SUM(rich_media_metrics.rich_media_impressions) AS rich_media_impressions,
    SUM(rich_media_metrics.rich_media_clicks) AS rich_media_clicks,
    SUM(rich_media_metrics.rich_media_interactive_impressions) AS rich_media_interactive_impressions,
    SUM(rich_media_metrics.rich_media_expansions) AS rich_media_expansions,
    SUM(rich_media_metrics.rich_media_expansion_time) AS rich_media_expansion_time,
    SUM(rich_media_metrics.rich_media_full_screen_impressions) AS rich_media_full_screen_impressions,
    SUM(active_view_metrics.active_view_viewable_impressions) AS active_view_viewable_impressions,
    SUM(ad_metrics.clicks) AS clicks,
    SUM(ad_metrics.impressions) AS impressions,
    SUM(xbid_metrics.cost.xbid_revenue_usd_nanos) / POW(10, 9) AS dbm_revenue_usd,
    SUM(xbid_metrics.cost.xbid_profit_usd_nanos) / POW(10, 9) AS dbm_profit_usd,
    SUM(xbid_metrics.cost.xbid_media_cost_usd_nanos) / POW(10, 9) AS dbm_media_cost_usd
  FROM xfa_reporting.abv.{20160101..20160827}
  WHERE base_event.local_event_date >= 201600101
    AND base_event.local_event_date <= 20160827
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;

  # DBM account info
  SELECT
    p.event_date AS event_date,
    p.dbm_partner_id AS dbm_partner_id,
    d.dbm_partner_name AS dbm_partner_name,
    p.dbm_advertiser_id AS dbm_advertiser_id,
    p.dbm_line_item_id AS dbm_line_item_id,
    p.dbm_creative_id AS dbm_creative_id,
    d.dbm_creative_name AS dbm_creative_name,
    d.dbm_creative_type AS dbm_creative_type,
    p.dbm_auction_type AS dbm_auction_type,
    d.dbm_creative_serving AS dbm_creative_serving,
    CASE
      WHEN p.platform_type = 3000 then 'desktop'
      WHEN p.platform_type = 3001 then 'smart phone'
      WHEN p.platform_type = 3002 then 'tablet'
      WHEN p.platform_type = 3003 then 'feature phone'
      ELSE 'other'
    END AS platform_type,
    CASE
      WHEN p.platform_type in (3001, 3002, 3003) then 'm' + string(p.rendering_environment)
      ELSE string(p.rendering_environment)
    END AS rendering_environment,
    SUM(p.rich_media_time) AS rich_media_time,
    SUM(p.rich_media_count) AS rich_media_count,
    SUM(p.rich_media_impressions) AS rich_media_impressions,
    SUM(p.rich_media_clicks) AS rich_media_clicks,
    SUM(p.rich_media_interactive_impressions) AS rich_media_interactive_impressions,
    SUM(p.rich_media_expansions) AS rich_media_expansions,
    SUM(p.rich_media_full_screen_impressions) AS rich_media_full_screen_impressions,
    SUM(p.active_view_viewable_impressions) AS active_view_viewable_impressions,
    SUM(p.clicks) AS clicks,
    SUM(p.impressions) AS impressions,
    SUM(p.dbm_revenue_usd) AS dbm_revenue_usd,
    SUM(p.dbm_profit_usd) AS dbm_profit_usd,
    SUM(p.dbm_media_cost_usd) AS dbm_media_cost_usd
  FROM dbm_perf p
  LEFT JOIN@50 dbm_creatives d
  ON p.dbm_creative_id = d.dbm_creative_id
  GROUP@50 by

EOF

}

dbm_cpm_dremel



###################################
# R part in progress
###################################


function dbm_cpm() {

  local svg="$( tempfile --prefix model --suffix .svg )"
  local svga="$( tempfile --prefix model --suffix .svg )"
  local cond=$1

  tee /dev/stderr << EOF | R --vanilla
  library(ginstall)
  library(gfile)
  library(namespacefs)
  library(rglib)
  lbrary(cfs)
  library(dremel)
  library(gbm)
  library(Hmisc)
  library(ggplot2)
  InitGoogle()
  options("scipen"=100, "digits"=12)

  myConn <- DremelConnect()
  DremelSetMinCompletionRatio(myConn, 1.0)
  DremelSetAccountingGroup(myConn,'urchin-processing-qa')
  DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
  DremelSetMaterializeOverwrite(myConn, TRUE)
  DremelSetIOTimeout(myConn, 7200)

  DremelAddTableDef('using_ga', '/cns/ig-d/home/aredakov/david_dbm_propencities/data_for_r/data*', myConn, verbose=FALSE)
  t <- DremelExecuteQuery("
    SELECT ..... 1 AS treat
    FROM using_ga
    WHERE inga_xadvertiser_id > 0
  ;", myConn)
  t[which(t\$revenue==0),] <- NA
  t <- na.omit(t)

  c <- DremelExecuteQuery("
    SELECT ....... 0 AS treat
    FROM
    WHERE inga_xadvertiser_id IS NULL
  ;", myConn)
  c[which(c\$revenue==0),] <- NA
  c <- na.omit(c)

  t.test(t\$adx_share, c\$adx_share)
  describe(t\$adx_share)
  describe(c\$adx_share)

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
  f <- ddply(f, .(), transform, percentile = round(ecdf(adx_share)(adx_share),1))

  # Bernoulli:
  # Estimate propensity score with Generalized Boosted Model.
  # Package GBM uses interaction.depth parameter AS a number of splits it
  # has to perform on a tree.
  # 3,000 to 10,000 iterations with shrinkage rates between 0.01 and 0.001.
  # "bernoulli" (logistic regression for 0-1 outcomes),
  # "adaboost" (the AdaBoost exponential loss for 0-1 outcomes).
  gps <- gbm(treat ~ mobile_type + creative_type + page_layout + non_ga_remarketing +
    vertical + service_country_code + service_channel, data = f,
    n.trees=500, train.fraction=0.5, interaction.depth=4,
    distribution="bernoulli", shrinkage=0.001)
  # If type="response" then gbm converts back to the same scale AS the outcome.
  f\$gpsvalue <- predict(gps, type="response", n.trees=500)
  f\$weight <- ifelse(f\$treat == 1, 1/f\$gpsvalue, 1/(1-f\$gpsvalue))
  wlm <- lm(adx_share ~ treat, data = f, weights= (f\$weight))
  summary(wlm)
  str(wlm)
  names <- c("bernoulli_mean", "p-value", "CILow", "CIup")
  data <- c(
    round(summary(wlm)\$coefficients[2],4)*100,
    round(summary(wlm)\$coefficients[8],7),
    round(summary(wlm)\$coefficients[2] - summary(wlm)\$coefficients[4]*2,4)*100,
    round(summary(wlm)\$coefficients[2] + summary(wlm)\$coefficients[4]*2,4)*100
    )
   data.frame(names,data)

   # AdaBoost.
  agps <- gbm(treat ~ mobile_type + creative_type + page_layout + non_ga_remarketing +
    vertical + service_country_code + service_channel, data = f,
    n.trees=500, train.fraction=0.5, interaction.depth=4,
    distribution="adaboost", shrinkage=0.001)
  # If type="response" then gbm converts back to the same scale AS the outcome.
  f\$agpsvalue <- predict(agps, type="response", n.trees=500)
  f\$aweight <- ifelse(f\$treat == 1, 1/f\$agpsvalue, 1/(1-f\$agpsvalue))
  awlm <- lm(adx_share ~ treat, data = f, weights= (f\$aweight))
  summary(awlm)

  f\$treat  <- factor(f\$treat)
  f\$adx_share <- as.numeric(f\$adx_share)

  tse <- sd(t\$adx_share)/sqrt(length(t\$adx_share))
  cse <- sd(c\$adx_share)/sqrt(length(c\$adx_share))

  names <- c("Using GA Remarketing", "Not Using GA Remarketing")
  means <- c(mean(t\$adx_share), mean(c\$adx_share))
  standardErrors <- c(tse, cse)

  # 95% chance that μ lies between m ± 2SE
  svg("${svg}")
  plotTop <- max(means+standardErrors*5)
  barCenters <- barplot(means,
    names.arg=names,
    main="Average Share of AdX Spend per Advertiser,
      \n Before Controling Factors",
    ylab="Revenue Share with CI95%",
    col=c("red","darkblue"), las=1, ylim=c(0,plotTop))
  segments(barCenters, means-standardErrors*2, barCenters, means+standardErrors*2, lwd=2)
  arrows(barCenters, means-standardErrors*2, barCenters, means+standardErrors*2, lwd=2, angle=90, code=3)
  dev.off()

EOF

  echo ''| sendgmr --subject="AdX" --attachment_files="${svg}" \
  --to=aredakov

}

dbm_cpm
