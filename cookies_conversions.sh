#!/bin/bash
# go/caqstats
# go/analysis-of-conversion-definitions
# ConversionCategoryId
# https://cs.corp.google.com/piper///depot/google3/ads/conversion_tracking/conversion_properties.proto?rcl=136177735&l=146
# caq_stats is only filled in for search_type=1, to miss search (0) and viral (4) i

source gbash.sh || exit 1

function main() {

  tee /dev/stderr << EOF | dremel
  ${DREMEL_INIT}

  SET accounting_group analytics-internal-processing-dev;
  SET min_completion_ratio 1;
  SET io_timeout 2400;
  SET nest_join_schema true;
  SET runtime_name dremel;
  SET materialize_overwrite true;
  SET materialize_owner_group analytics-internal-processing-dev;
  SET run_as_mdb_account aredakov;

  # AdEvents contains query spam information, it does not contain all ad queries
  # and impressions. It only contains the ad queries corresponding to the
  # clicks and conversions in the logs.
  # All cookie ages.
  MATERIALIZE '/cns/ig-d/home/aredakov/cookie_deletion/cookie_age/data@5000' AS
  SELECT
    query.mobile_browser_class,
    FINGERPRINT2011(FORMAT("%d-%d-%d",
      query.query_id.time_usec, query.query_id.server_ip,
      query.query_id.process_id)) AS qid,
    UINT64((query.query_id.time_usec / 1000000 - query.cookie_init_time) / 86400)
      AS cookie_age_days,
  FROM processed_ads.SampledAdEventsQueries.all
  WHERE query.search_type = 1
    AND query.cookie_init_time > 0
    AND query.country = 'US'
  GROUP@5000 BY 1,2,3;

  MATERIALIZE '/cns/ig-d/home/aredakov/cookie_deletion/conversions/data@5000' AS
  SELECT
    FINGERPRINT2011(FORMAT("%d-%d-%d",
      query_id.time_usec, query_id.server_ip, query_id.process_id)) AS qid,
    SUM(impression.credible_conversions) AS credible_conversions,
    SUM(impression.impressions) AS impressions,
    SUM(impression.adv_cost_usd) AS cost
  FROM FLATTEN(caq.stats.last7days, impression)
  WHERE impression.impressions > 0
  GROUP@5000 BY 1;

  DEFINE TABLE cookie_age /cns/ig-d/home/aredakov/cookie_deletion/cookie_age/data*;
  DEFINE TABLE conversions /cns/ig-d/home/aredakov/cookie_deletion/conversions/data*;
  MATERIALIZE '/cns/ig-d/home/aredakov/cookie_deletion/conversions_by_cook_age/data' AS
  SELECT
    cookie_age_days,
    SUM(credible_conversions) AS credible_conversions,
    SUM(cost) AS cost,
    COUNT(a.qid) AS cnt_queries,
    SUM(impressions) AS sum_impressions
  FROM conversions a
  JOIN@5000 cookie_age b
  ON a.qid = b.qid
  WHERE cookie_age_days <= 600
  GROUP@5000 BY 1;

EOF

  R --vanilla << EOF
  library(ginstall)
  library(gfile)
  library(namespacefs)
  library(rglib)
  library(cfs)
  library(dremel)
  library(gbm)
  library(Hmisc)
  library(ggplot2)
  library(corrplot)
  InitGoogle()
  options("scipen"=100, "digits"=6)

  myConn <- DremelConnect()
  DremelSetMinCompletionRatio(myConn, 1.0)
  DremelSetAccountingGroup(myConn,'urchin-processing-qa')
  DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
  DremelSetMaterializeOverwrite(myConn, TRUE)
  DremelSetIOTimeout(myConn, 7200)

  # Cookies count.
  # go/content-ads-sessions
  # 1. Translate the logic of the go function I linked into dremel and call that
  # for the first event of each cookied session: http://google3/logs/lib/contentads/go/contentsession.go?l=175
  # 2. Get pseudonym access to ads, do the calculation on AdQueries, GROUP BY
  # userid and then aggregate to take, say, the minimum cookie age for that day
  # for that cookie.

  pd <- position_dodge(.1)
  ggplot(data = cc, aes(x = cookie_age_days, y = cnt_cookies/1000)) +
  geom_point(aes(x = cookie_age_days, color="red")) +
  coord_cartesian(ylim = c(0, 150),xlim = c(0, 600)) +
  stat_smooth(position=pd, method="glm", method.args = list(family = "poisson")) +
  theme(legend.position="none") +
  theme(axis.text.x=element_text(size=8, color="gray26", angle = 45, hjust = 1)) +
  ggtitle("Count of Cookies per Cookie Age Bucket ") +
  theme(plot.title = element_text(lineheight=.8)) +
  ylab("Count  of Cookies, Thousands") +
  xlab("Cookie Age in Days")

  DremelAddTableDef('conversions_by_cook_age', '/cns/ig-d/home/aredakov/cookie_deletion/conversions_by_cook_age/data*',
    myConn, verbose=FALSE)

  d <- DremelExecuteQuery("
  SELECT
    cookie_age_days,
    credible_conversions,
    cost,
    cost / credible_conversions AS conv_cost,
    credible_conversions / sum_impressions AS impr_conv_rate
  FROM conversions_by_cook_age
  ;", myConn)

  d[is.na(d)] <- 0
  d <- d[order(d$cookie_age_days), ]

  # Correlations.
  # Parametric
  cor.test(d$cookie_age_days,d$credible_conversions,method="pearson")
  cor.test(d$cookie_age_days,d$cost,method="pearson")
  cor.test(d$cookie_age_days,d$conv_cost,method="pearson")

  # Non-parametric
  # Spearman's correlation measures the strength and direction of
  # monotonic association between two variables.
  # Spearman's rho measures the strength of association of two variables.
  cor.test(d$cookie_age_days,d$credible_conversions,method="spearman")
  cor.test(d$cookie_age_days,d$cost,method="spearman")
  cor.test(d$cookie_age_days,d$conv_cost,method="spearman")
  cor.test(d$cookie_age_days,d$impr_conv_rate,method="spearman")


  # Poisson regression is applied to situations in which the response
  # variable is the number of events to occur in a given period of time.
  fit <- glm(d$credible_conversions ~ d$cookie_age_days, family=poisson())
  exp(coef(fit))

  pd <- position_dodge(.1)
  ggplot(data = d, aes(x = cookie_age_days, y = credible_conversions)) +
  geom_point(aes(x = cookie_age_days, color="red")) +
  coord_cartesian(ylim = c(0, 1000),xlim = c(0, 600)) +
  stat_smooth(position=pd, method="glm", method.args = list(family = "poisson")) +
  theme_bw() +
  theme(legend.position="none") +
  theme(strip.text=element_text(size=14,face="bold")) +
  theme(axis.text.x=element_text(size=14,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=14,face="bold", color="gray26")) +
  theme(axis.text.x=element_text(size=14, color="gray26", angle = 45, hjust = 1)) +
  ggtitle("Conversions Over Cookie Age. 1% Sample.") +
  theme(plot.title = element_text(lineheight=.8)) +
  ylab("Conversions per Cookie Age") +
  xlab("Cookie Age in Days")

  summary(lm(d$conv_cost ~ d$cookie_age_days))

  pd <- position_dodge(.1)
  ggplot(data = d, aes(x = cookie_age_days, y = conv_cost)) +
  geom_point(aes(x = cookie_age_days, color="red")) +
  # coord_cartesian(ylim = c(0, 2),xlim = c(0, 600)) +
  stat_smooth(position=pd, method="lm", fullrange=TRUE, size=1.2) +
   theme_bw() +
  theme(legend.position="none") +
  theme(strip.text=element_text(size=14,face="bold")) +
  theme(axis.text.x=element_text(size=14,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=14,face="bold", color="gray26")) +
  theme(axis.text.x=element_text(size=14, color="gray26", angle = 45, hjust = 1)) +
  scale_y_continuous(labels =  scales::dollar) +
  ggtitle("Conversions Cost Over Cookie Age") +
  theme(plot.title = element_text(lineheight=.8)) +
  ylab("Conversions Cost, USD") +
  xlab("Cookie Age in Days")

  fitr <- glm(d$cost ~ d$cookie_age_days, family=poisson())
  exp(coef(fitr))

  pd <- position_dodge(.1)
  ggplot(data = d, aes(x = cookie_age_days, y = cost)) +
  geom_point(aes(x = cookie_age_days, color="red")) +
  coord_cartesian(ylim = c(0, 10000),xlim = c(0, 600)) +
  stat_smooth(position=pd, method="glm", method.args = list(family = "poisson")) +
  theme(legend.position="none") +
  scale_y_continuous(labels =  scales::dollar) +
  theme(axis.text.x=element_text(size=8, color="gray26", angle = 45, hjust = 1)) +
  ggtitle("Total Cost Over Cookie Age") +
  theme(plot.title = element_text(lineheight=.8)) +
  ylab("Total Cost, USD") +
  xlab("Cookie Age in Days")

  # Conversions rate (Impressions).
  summary(lm(d$impr_conv_rate ~ d$cookie_age_days))

  pd <- position_dodge(.1)
  ggplot(data = d, aes(x = cookie_age_days, y = impr_conv_rate)) +
  geom_point(aes(x = cookie_age_days, color="red")) +
  coord_cartesian(ylim = c(0,0.001),xlim = c(0, 600)) +
  stat_smooth(position=pd, method="lm", fullrange=TRUE, size=1.2) +
  theme(legend.position="none") +
  scale_y_continuous(labels =  percent) +
  theme_bw() +
  theme(legend.position="none") +
  theme(strip.text=element_text(size=14,face="bold")) +
  theme(axis.text.x=element_text(size=14,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=14,face="bold", color="gray26")) +
  theme(axis.text.x=element_text(size=14, color="gray26", angle = 45, hjust = 1)) +
  ggtitle("Conversion Rate (by Impressions) Over Cookie Age") +
  theme(plot.title = element_text(lineheight=.8)) +
  ylab("Conversion Rate, by Impressions") +
  xlab("Cookie Age in Days")

EOF

}

gbash::main "$@"
