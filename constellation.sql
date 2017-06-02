#!/bin/bash
# Team page:
# https://sites.google.com/a/google.com/mobiledisplay/engineering/constellation/constellation-data-analysis
# Query Coverage of Constellation Graph (Xdevice + Mobius)
# Avacado is better gaia coverage.
# https://dasnav.corp.google.com/dnseli67/#dimensions=stats.link_stats.link_type:6&granularity=day&page=atomics&start=P7D&view=default
# Xdevice: GAIA based web/app/mobweb.
# Mobius: App to MobBrowser. (There is some non_Mobius source of same-device
# lines, but is very small)
# https://dasnav.corp.google.com/dnsq25o6/#dimensions=&granularity=day&page=atomics&start=P28D&view=default

# https://dasnav.corp.google.com/dnseli67/#dimensions=stats.link_stats.link_type:7&granularity=week&page=atomics&start=P91D&view=default
# revenue
# https://dasnav.corp.google.com/dnsndxk6/#label_id=lguz6&view=default

# Correlations.
# https://rasta.corp.google.com/#/metrics?qs=080a101be:eyIxIjoAMgMiY29udGULYWRzIiwiMwNbCAoMDhA6OnF1ZXJpZRETCQsNdA9zHWltcHKKRzaQqKUYioG4rDp2cG1jbGGK9lZBKLcaiqHDpyi9jFMmaMWKeMeKsdY3BjK40BmKmNM6bWF4jVjXi_jBcIw42YuIyIuo4StfH4oYo4to0Y24yY619wZ2N0co9o-Y540o9I1o7A-PmPuNZ0j-jzjqkBj8j6kMkIi5jUkLkGj7kDj6kPjpkRjXX4v2lja48Y2pEDqPWKuQ6KaSGRiSOQuSWO5yeZF43I3o4IvY44woxJJ46I3I1W2TCQBtj2j4kNkGk9kKk_kllDEzKTciMjaMUzBpOI_4um90aIoV-UeP2VKQmNRvdooWFnZZJoxpXY3WF0Y5V2SS2PCUQdbG-LNuZ5bopJcIu3CR2MCTaXmMtpl5dmFsigk1jllLjykiixlgbooJf2WY2KBfd1-Pp2X2luc4-mWMGXkNZ5YmVtipkblzdF91c5iZKI3JoZo5pQtfcpaWWXmNWbeVySJhlgm1ljX2R1m1dItG6PaVibCWObKQebuSlub2JjmMDZj5zo3J0p1Jjpt10TNBYFngARY0Ml19nnndIjWeICOpZouZmXOZliYV9ib2-TyUmeIGOjGegTFTowneAzB9LACe6fCOmfd1eZWG5ldHdvcmuM2f0DoAoKnjOhcCnpoCoHoGoEoJoLB5iqDmdwn2iucJlnmOWhqiCeGf-iih-iEiojFKJSKd-ieeqU6e8tMjAxNzAzpIMKTqT4xTcHpGpIpKpMOaT6UBM5B2KKSc0BMAedqdYBnuOTUTMZ_iKWJnixmiDIxTGgMipemcjNpSoElJOKPzOn2iefwElMpiqDoDoIoJpAA6KaOH0A
# cor.test(d$rmkt,d$mobius,method="pearson")
# cor.test(d$rmkt, d$mobius, method="spearman")

library(ginstall)
library(gfile)
library(namespacefs)
library(rglib)
library(cfs)
library(dremel)
library(gbm)
library(Hmisc)
library(ggplot2)
library(lubridate)
InitGoogle()
options("scipen"=100, "digits"=6)

myConn <- DremelConnect()
DremelSetMinCompletionRatio(myConn, 1.0)
DremelSetAccountingGroup(myConn,'urchin-processing-qa')
DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
DremelSetMaterializeOverwrite(myConn, TRUE)
DremelSetIOTimeout(myConn, 7200)

qr <- DremelExecuteQuery("
  SELECT
    date,
    SUM(stats.link_stats.value.query_with_link_count) AS mobius_queries
   FROM ads_constellation.ExpConstellationQueryCoverage
  WHERE date >= '2017-01-01'
    AND country = 'US'
    AND device_os_type IN ('DEVICE_OS_TYPE_ANDROID','DEVICE_OS_TYPE_IOS')
    AND traffic_type IN ('TRAFFIC_TYPE_YOUTUBE_APP',
     'TRAFFIC_TYPE_MGDN', 'TRAFFIC_TYPE_GMOB')
    AND stats.link_stats.link_type IN ('SAME_DEVICE_COVERAGE')
  GROUP BY 1
;", myConn)

qr$date <- ymd(qr$date)
pd <- position_dodge(.1)
ggplot(data = qr, aes(x = date, y = mobius_queries)) +
geom_point(aes(x = date, color="red")) +
stat_smooth(position=pd, method="loess", fullrange=TRUE, size=1.2) +
theme(legend.position="none") +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
ggtitle("Mobius Queries") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("Mobius Queries") +
xlab("")

rv <- DremelExecuteQuery("
  SELECT
    date,
    SUM(ad_spend) AS mobius_spend
  FROM zoom.dnsndxk6.10313
  WHERE date >= '2017-01-01'
    AND country = 'US'
    AND os_name IN ('Android','iOS')
    AND device_type IN ('Mobile','Tablet')
    AND mixer IN ('CAT2')
    AND repository IN ('adwords')
    AND targeting_type IN ('User Interest', 'User List')
  GROUP@50 BY 1
;", myConn)

rv$date <- ymd(rv$date)
pd <- position_dodge(.1)
ggplot(data = rv, aes(x = date, y = mobius_spend)) +
geom_point(aes(x = date, color="red")) +
stat_smooth(position=pd, method="loess", fullrange=TRUE, size=1.2) +
theme(legend.position="none") +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
ggtitle("Mobius Spend") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("Mobius Spend") +
xlab("")


mub <- merge(qr,rv,by=c("date"))
mub$date <- ymd(mub$date)
mub$ratio <- (mub$mobius_spend / (mub$mobius_queries))*1000000

t.test(mub$ratio)
# 189.943

# Per millions
pd <- position_dodge(.1)
ggplot(data = mub, aes(x = date, y = ratio)) +
geom_point(aes(x = date, color="red")) +
stat_smooth(position=pd, method="lm", fullrange=TRUE, size=1.2) +
theme(legend.position="none") +
scale_y_continuous(labels =  scales::dollar) +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
ggtitle("Mobius Spend per Million Queries") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("Mobius Spend per Million Queries, USD") +
xlab("")

# Revenue regressions.
qa <- DremelExecuteQuery("
  SELECT
    WEEKOFYEAR(date) AS week,
    SUM(ad_spend) AS mobius_spend
  FROM zoom.dnsndxk6.10313
  WHERE date >= '2016-01-01'
    AND date <= '2016-03-31'
    AND country = 'US'
    AND os_name IN ('Android','iOS')
    AND device_type IN ('Mobile','Tablet')
    AND mixer IN ('CAT2')
    AND repository IN ('adwords')
    AND targeting_type IN ('User Interest', 'User List')
  GROUP@50 BY 1
;", myConn)

t.test(qa$mobius_spend)
# 3.0M
summary(lm(qa$mobius_spend ~ qa$week))
# 0.14M

pd <- position_dodge(.1)
ggplot(data = qa, aes(x = week, y = mobius_spend)) +
geom_point(aes(x = week, color="red")) +
stat_smooth(position=pd, method="lm", fullrange=TRUE, size=1.2) +
theme_bw() +
theme(legend.position="none") +
scale_y_continuous(labels =  scales::dollar) +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
ggtitle("US. Q1 2016. Mobius Spend.") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("Mobius Spend, USD") +
xlab("Week from 1 Jan 2017")

qb <- DremelExecuteQuery("
  SELECT
    WEEKOFYEAR(date) AS week,
    SUM(ad_spend) AS mobius_spend
  FROM zoom.dnsndxk6.10313
  WHERE date >= '2017-01-01'
    AND date <= '2017-03-31'
    AND country = 'US'
    AND os_name IN ('Android','iOS')
    AND device_type IN ('Mobile','Tablet')
    AND mixer IN ('CAT2')
    AND repository IN ('adwords')
    AND targeting_type IN ('User Interest', 'User List')
  GROUP@50 BY 1
  HAVING week != 9
;", myConn)

t.test(qb$mobius_spend)
# 5.1M
summary(lm(qb$mobius_spend ~ qb$week))
# 0.25M

pd <- position_dodge(.1)
ggplot(data = qb, aes(x = week, y = mobius_spend)) +
geom_point(aes(x = week, color="red")) +
stat_smooth(position=pd, method="lm", fullrange=TRUE, size=1.2) +
theme_bw() +
theme(legend.position="none") +
scale_y_continuous(labels =  scales::dollar) +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
ggtitle("US. Q1 2017. Mobius Spend.") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("Mobius Spend, USD") +
xlab("Week")

# Coverage OS type.
os <- DremelExecuteQuery("
  SELECT
    REPLACE(device_os_type,'DEVICE_OS_TYPE_','') AS OS,
    REPLACE(traffic_type,'TRAFFIC_TYPE_','') AS traffic,
    IF(stats.link_stats.link_type ='SAME_DEVICE_COVERAGE','Mobius','Xdevice') AS LinkType,
    SUM(stats.link_stats.value.query_with_link_count) AS with_link_queries,
    SUM(total_query_count) AS total_queries,
    SUM(stats.link_stats.value.query_with_link_count) / SUM(total_query_count) AS Share
  FROM ads_constellation.ExpConstellationQueryCoverage
  WHERE date >= '2017-01-01'
    AND country = 'US'
    AND stats.link_stats.link_type IN ('SAME_DEVICE_COVERAGE',
      'CROSS_DEVICE_COVERAGE')
    AND device_os_type IN ('DEVICE_OS_TYPE_ANDROID','DEVICE_OS_TYPE_IOS')
    AND traffic_type IN ('TRAFFIC_TYPE_YOUTUBE_APP',
     'TRAFFIC_TYPE_MGDN', 'TRAFFIC_TYPE_GMOB')
  GROUP BY 1,2,3
;", myConn)

ggplot(os ,aes(x = OS, y = Share)) +
geom_bar(stat="identity", width=.5, position = "dodge", aes(fill = LinkType)) +
theme_bw() +
theme(legend.position="none") +
facet_grid(traffic~.) +
geom_text(aes(label=sprintf("%1.1f%%", Share*100)),vjust=+1.1,
size=5) +
theme(strip.text=element_text(size=14,face="bold")) +
theme(axis.text.x=element_text(size=14,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=14,face="bold", color="gray26")) +
theme(axis.text.x=element_text(size=14, color="gray26", hjust = 1)) +
theme(axis.ticks.y = element_blank()) +
theme(axis.text.y = element_blank()) +
theme(axis.line.y = element_blank()) +
scale_y_continuous(labels = percent) +
ggtitle("Country=US. % Constellation Coverage by Link/OS") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("% of Total") + xlab("")

