account_1AccountThree#' GetExpGroupAccount4
#' Gets history of experiments and groups for account_4 with salt
#' make it consistent across accounts
#'
#' @param
#' baseDate: date string in the format "YYYY-MM-DD"
#'
#' @return data.table with results
#'
#' @author  Lin Jia
#'
#' @import data.table
#'
#' @export
#'
GetExpGroupAccount4 <- function( # need to change to the start date of latest experiments
    end_date = format(Sys.Date(), "%F")){
    require(data.table, quietly = T)
    confFile <- "/shared/ppc/SHOP/trivago/config/masterconfig"
    txtConf  <- scan(file = confFile,what = "char",quiet = T,comment.char = "#")
    ## peruse file sequentially for each set of experiments and keep data.table with summary
    expGroups = data.table()
    numExp   <- grep("<experiments",txtConf)

    for (i_exp in 1:length(numExp)) {
        fromExp <- numExp[i_exp] + 1
        toExp   <- ifelse(i_exp == length(numExp), length(txtConf), numExp[i_exp + 1] - 1)
        txtExp  <- txtConf[fromExp : toExp]
        #placement = gsub(">","",txtExp[1])
        numExpID = grep("<id",txtExp)

        for (i_id in 1:length(numExpID)) { # i_exp = 2
            fromID <- numExpID[i_id]
            toID   <- ifelse(i_id == length(numExpID), length(txtExp), numExpID[i_id + 1] - 1)
            txtID  <- txtExp[fromID : toID]

            experimentID <- gsub("<|>","",txtID[1])
            salt         <- txtID[grep("salt", txtID) + 1]
            groupID      <- txtID[(grep("<group", txtID)):length(txtID)]
            groupSet     <- grep("<group",groupID)
            expTesting   <- txtID[grep("experiment_testing", txtID) + 1]
            participating_markets  <- txtID[grep("markets_participating", txtID) + 1]
            date_original <- txtID[grep("^date_original$", txtID) + 1]
            grp_list_init <- 0
            for (i_group in 1:length(groupSet)) { # i_group = 1
                fromGroup <- groupSet[i_group] + 1
                toGroup   <- ifelse(i_group == length(groupSet), length(groupID), groupSet[i_group + 1] - 1)
                txtGroup  <- groupID[fromGroup:toGroup]
                logic     <- txtGroup[grep("^logic$",txtGroup) + 1]
                # add exact matching of weight
                weight    <- as.numeric(txtGroup[grep("^weight$",txtGroup) + 1])
                grp_list  <- paste(seq(grp_list_init,grp_list_init+weight-1,1),collapse = ",")
                grp_name  <- txtGroup[grep("^name$",txtGroup) + 1]
                test_control_group <- txtGroup[grep("test_control_group",txtGroup) + 1]
                expGroups <- rbind(expGroups
                                   ,data.table(start_date = date_original
                                               ,end_date = end_date
                                               ,experiment_id = experimentID
                                               ,grp = i_group
                                               ,grp_name = grp_name
                                               ,weight, grp_list, logic, salt
                                               ,experiment_testing = expTesting
                                               ,participating_markets = participating_markets
                                               ,test_control_group =  test_control_group))
                grp_list_init <- grp_list_init + weight
            }
        }
    }
    return(expGroups)
}

#' getPerfExpAccount4
#' Gets performance summary at experiments level for each detail group
#'
#' @param
#' baseDate,endDate: date limits "YYYY-MM-DD"
#' experiment_id, salt
#' @return data.table with results
#'
#' @author Lin Jia
#'
#' @import data.table
#'
#' @export
#'

getPerfExpAccount4 <- function(baseDate = format(Sys.Date()-30, "%F")
                              ,endDate = format(Sys.Date(), "%F")
                              ,experiment_id
                              ,salt) {
    library(data.table)
    library(digest)
    # performance data
    perf <- getDataTableHive(sprintf("select pos, yyyy_mm_dd date, hotel_id,impressions ,clicks,bookings,
                                     commission_amount_euro, commission_expected_euro,  cost_euro,
                                     top_position_share, outbid_ratio, share_impr, roomnights, avg_nits,'does_not_apply' as placement
                                     from spmeta.trivago_performance where
                                     yyyy_mm_dd >= '%s'
                                     and yyyy_mm_dd < '%s'
                                     and pos not in ('')"
                                     , baseDate, endDate), dsn = 'hive-lhr4', uid='crontabs')
    # cancelllation data
    perf_canc <- getDataTableMS(sprintf("select r.date_cancelled date
                                        , a.distribution pos
                                        , coalesce(r.dest_id, r.hotel_id) hotel_id
                                        , sum(1) cancellations
                                        , sum(commission_amount_euro) comm_canc_asbooked
                                        from ppc_sp.SPReservation r force index (cancel)
                                        join bp_slice.FlatAffiliate a on (a.affiliate_id = r.affiliate_id
                                        and a.partner_id = 413084)
                                        where r.date_cancelled >= '%s' and r.date_cancelled < '%s'
                                        and r.status not in ('fraudulent','test','unknown')
                                        and r.partner_id = 413084
                                        group by 1,2,3;
                                        ", baseDate, endDate))

    if (nrow(perf_canc) <= 0 | nrow(perf) <= 0) stop("No data found for these dates")
    # assign placement hard coded
    perf_canc[, placement := 'does_not_apply']

    perfDetail <- merge(perf, perf_canc, by = c("date", "pos", "hotel_id", "placement"), all.x = T)

    # getting experiments info
    exp_summ <- getDataTableMS(sprintf("select experiment_id, salt, grp_name as logic, weight, grp_list, participating_markets
                                       from ppc_trivago.experiments_evaluation_overview
                                       where experiment_id = '%s' and salt = '%s'", experiment_id, salt))
    exp_summ[, sum_weight := sum(weight), by = list(experiment_id, salt)]
    # explode by pos
    exp_summ <- exp_summ[, list(pos = unlist(strsplit(participating_markets, ","))),
                         by=list(experiment_id, salt, logic, grp_list, weight, sum_weight)]
    ## explode by grp_list
    exp_summ <- exp_summ[, list(detail_group = as.numeric(unlist(strsplit(grp_list, ",")))),
                         by = list(experiment_id, salt, logic, weight, sum_weight, pos)]

    ## change country code, this only happens for the trivago configuration file where AA means SA, UK means GB,
    ## in spmeta.trivago_performance, the name is SA and GB
    exp_summ[, pos := gsub("AA", "SA", pos)]
    exp_summ[, pos := gsub("UK", "GB", pos)]

    ## merge with experiments data detail_group
    perfDetail <- merge(perfDetail, exp_summ[,.(salt=unique(salt), tot_weight=unique(sum_weight)),by="pos"], by=c("pos"), all.x = T)
    perfDetail[, detail_group := strtoi(substr(digest(paste0(hotel_id,salt),algo = "md5", serialize = F),29,32),16) %% tot_weight
               , by=c("hotel_id","salt","tot_weight")]

    perfDetail <- merge(perfDetail, exp_summ[, list(experiment_id, salt, logic, pos, detail_group)],
                        by = c("salt", "pos", "detail_group"), all.x = T)

    summPerf <- perfDetail[,list(cost=sum(cost_euro, na.rm=T)
                                 ,clicks=sum(clicks, na.rm=T)
                                 ,impressions=sum(impressions, na.rm=T)
                                 ,bookings=sum(bookings, na.rm=T)
                                 ,commission=sum(commission_amount_euro, na.rm=T)
                                 ,commission_expected=sum(commission_expected_euro, na.rm=T)
                                 ,bookings_expected=sum(1-avg_nits, na.rm=T)
                                 ,roomnights=sum(roomnights, na.rm=T)
                                 ,top_position_share=sum(top_position_share*impressions, na.rm=T)/sum(impressions, na.rm=T)
                                 ,outbid_ratio=sum(outbid_ratio*impressions, na.rm=T)/sum(impressions, na.rm=T)
                                 ,share_impr=sum(share_impr*impressions, na.rm=T)/sum(impressions, na.rm=T))
                           ,by=list(date, placement, experiment_id, detail_group, pos, salt, logic)]
    setkey(summPerf,date, placement, experiment_id, detail_group, pos, salt, logic)

    return(summPerf)
}

#' GetExpGroupKayak
#' Gets history of experiments and groups for Kayak with salt
#' make it consistent across accounts
#'
#' @param
#' baseDate: date string in the format "YYYY-MM-DD"
#'
#' @return data.table with results
#'
#' @author  Lin Jia
#'
#' @import data.table
#'
#' @export
#'

GetExpGroupKayak <- function(endDate = format(Sys.Date(), "%F")){
    require(data.table, quietly = T)
    confFile <- "/shared/ppc/ppc_conf/sp/kayak/config/main.conf"
    txtConf  <- scan(file = confFile,what = "char",quiet = T,comment.char = "#")
    ## peruse file sequentially for each set of experiments and keep data.table with summary
    expGroups = data.table()
    numExp   <- grep("<experiments",txtConf)

    for (i_exp in 1:length(numExp)) {
        fromExp <- numExp[i_exp] + 1
        toExp   <- ifelse(i_exp == length(numExp), length(txtConf), numExp[i_exp + 1] - 1)
        txtExp  <- txtConf[fromExp : toExp]
        placement <- txtExp[1]
        device <- gsub(">","",txtExp[2])
        numExpID = grep("<experiment",txtExp)

        for (i_id in 1:length(numExpID)) { # i_exp = 2
            fromID <- numExpID[i_id] + 1
            toID   <- ifelse(i_id == length(numExpID), length(txtExp), numExpID[i_id + 1] - 1)
            txtID  <- txtExp[fromID : toID]

            experimentID <- gsub(">","",txtID[+1])
            salt         <- txtID[grep("salt", txtID) + 1]
            locale       <- txtID[grep("locale", txtID) + 1]
            groupID      <- txtID[(grep("<groups", txtID) + 1):length(txtID)]
            groupSet     <- grep("<group",groupID)
            expTesting   <- txtID[grep("experiment_testing", txtID) + 1]
            startDate    <- txtID[grep("^date_original$", txtID) + 1]
            lsDates      <- format(seq(as.Date(startDate),as.Date(endDate),1),"%F")

            grp_list_init <- 0
            for (i_group in 1:length(groupSet)) { # i_group = 1
                fromGroup <- groupSet[i_group] + 1
                toGroup   <- ifelse(i_group == length(groupSet), length(groupID), groupSet[i_group + 1] - 1)
                txtGroup  <- groupID[fromGroup:toGroup]
                logic     <- txtGroup[grep("logic",txtGroup) + 1]
                # add exact matching of weight
                weight    <- as.numeric(txtGroup[grep("^weight$",txtGroup) + 1])
                grp_list  <- paste(seq(grp_list_init,grp_list_init+weight-1,1),collapse = ",")
                grp_name  <- paste0("group", i_group)
                test_control_group <- txtGroup[grep("test_control_group",txtGroup) + 1]
                expGroups <- rbind(expGroups
                                   ,data.table(date = lsDates
                                               ,experiment_id = experimentID
                                               ,grp = i_group
                                               ,grp_name = grp_name
                                               ,weight, grp_list, logic, salt, placement, device, locale
                                               ,experiment_testing = expTesting
                                               ,test_control_group))
                grp_list_init <- grp_list_init + weight
            }
        }
    }
    return(expGroups)
}

#' getPerfExpKayak
#' Gets performance summary at experiments level for each detail group for Kayak
#'
#' @param
#' startDate,endDate: date limits "YYYY-MM-DD"
#' experiment_id, salt
#' @return data.table with results
#'
#' @author Lin Jia
#'
#' @import data.table
#'
#' @export
#'

getPerfExpKayak <- function(startDate = format(Sys.Date()-30, "%F")
                            ,endDate = format(Sys.Date(), "%F")
                            ,experiment_id
                            ,salt) {
    require(digest, quietly = T)
    require(data.table, quietly = T)

    ## affliates
    aff_info <- getDataTableMS(
        "select id affiliate_id, name, partner_id, substring_index(aff.name,'_',1) brand,
        (case when aff.name rlike '[_]Flights'
        then substring_index(substring_index(substring_index(aff.name,'.',1),'_',-2),'_',1)
        when aff.name rlike '[_]Packages'
        then substring_index(substring_index(substring_index(aff.name,'.',1),'_',-2),'_',1)
        when aff.name rlike '[_]Mobile'
        then substring_index(substring_index(substring_index(aff.name,'.',1),'_',-2),'_',1)
        when aff.name rlike '[_]Pseudo_Responsive'
        then 'CM2'
        else substring_index(substring_index(aff.name,'.',1),'_',-1)
        end) placement_type,
        (case when aff.name like '%Mobile%'
        then 'Mobile'
        else 'Web'
        end) device
        from bp_slice.B_Affiliate aff
        where aff.partner_id in (496331,431843)")

    aff_info[, placement_type := tolower(placement_type)]
    aff_info[, placement      := ifelse(placement_type %in% c('core', 'whisky'), placement_type, 'ads')]

    ## reservation data
    reservation <- getDataTableMS(sprintf("
                                          SELECT substr(r.created,1,10) date
                                          , r.affiliate_id, r.status, r.checkin, r.checkout, r.label, coalesce(r.dest_id,r.hotel_id) hotel_id,
                                          (case when r.label rlike '[-]hcompareto'
                                          then substring_index(substring_index(r.label, '-city-', 1), '-hcompareto', -1)
                                          when r.label rlike '[-]hrcompareto'
                                          then substring_index(substring_index(r.label, '-city-', 1), '-hrcompareto', -1)
                                          when r.label rlike '[-]hinline'
                                          then substring_index(substring_index(r.label, '-city-', 1), '-hinline', -1)
                                          when r.label rlike '[-]hAfterclick'
                                          then substring_index(substring_index(r.label, '-city-', 1), '-hAfterclick', -1)
                                          when r.label rlike '[-]hsmartAdpt'
                                          then substring_index(substring_index(r.label, '-city-', 1), '-hsmartAdpt', -1)
                                          else substring_index(substring_index(r.label, '-hotel-', 1), '-core', -1)
                                          end) locale
                                          , sum(1) bookings
                                          , sum(r.commission_amount_euro) commission_amount_euro
                                          , sum(coalesce(ng.stay_probability_wnd, ng.stay_probability, ng.stay_probability_analytics)) expectBookings
                                          , sum(r.commission_amount_euro * coalesce(ng.stay_probability_wnd, ng.stay_probability, ng.stay_probability_analytics)) expectCommission
                                          FROM ppc_sp.SPReservation r
                                          left join ppc_sp.SPReservationIntentToStayNG ng on (ng.hotelreservation_id = r.hotelreservation_id
                                          and ng.date_created )
                                          where r.created >= '%1$s' and r.created < '%2$s'
                                          and r.status not in ('fraudulent','test','unknown')
                                          and r.affiliate_id in (select id from bp_slice.B_Affiliate where partner_id in (496331,431843))
                                          group by 1,2,3,4,5,6,7", startDate, endDate))

    setkey(reservation,date,affiliate_id,status,checkin,checkout,label,hotel_id,locale)

    reservation <- reservation[is.na(bookings), bookings := 0]
    reservation <- reservation[is.na(commission_amount_euro), commission_amount_euro := 0]
    reservation <- reservation[is.na(expectBookings), expectBookings := 0]
    reservation <- reservation[is.na(expectCommission), expectCommission := 0]

    reservation[, label := tolower(label)]
    reservation[which(locale ==""), locale := 'us']
    reservation[which(nchar(locale) > 2), locale :='us']
    reservation[which(locale == 'uk'), locale :='gb']
    reservation[which(locale=='vn'), locale :='us']

    reservation <- merge(reservation,aff_info,by="affiliate_id")

    reservation[, device := tolower(device)]
    reservation[, locale := toupper(locale)]

    # comment out this locale, as they have their own affiliate ids
    #reservation[which(locale=='gl'), locale :='us']##chaning GL to US as its a momondo unknow locale
    #reservation[which(locale=='ng'), locale :='us']
    #reservation[which(locale=='ph'), locale :='us']
    #reservation[which(locale=='RY'), locale :='us']
    # comment out brand as it is deprecated.
    #reservation[,brand:=tolower(brand)]

    ### TODO:: don't understand what it does here, what does aff mean, what does 0 mean for aff
    reservation[, aff := as.numeric(NA)]
    reservation[grepl("_aff-", label), aff := as.numeric(gsub(".*_aff-([0-1]+).*","\\1",label,perl = T))]
    ##if na consider direct
    reservation[, aff := ifelse(is.na(aff), 0, aff)]
    # filter out placements that are ads, what does this mean?
    reservation <- reservation[placement !='ads']

    # Newly ADDED Resolve experiment (internal) groups from experiments_overview, only filter for those that are considered to be experiments
    # TODO:: need to remove the startDate and end_date configuration and test the code all over again
    exp_overview <- getDataTableMS(sprintf("select distinct experiment_id, salt, logic, weight, grp_list,
                                           locale, placement, device from ppc_kayak.experiments_evaluation_overview where
                                           experiment_testing = True and experiment_id = '%s' and salt = '%s'",
                                           experiment_id, salt))

    exp_summ <- exp_overview[, list(locale = unlist(strsplit(locale, ","))), by=list(experiment_id, salt, logic, weight, placement, grp_list, device)]

    reservation <- merge(reservation, exp_summ[,.(salt= max(salt), tot_weight = sum(weight)),by= c("locale", "placement", "device", "experiment_id")]
                         ,by= c( "locale", "placement", "device"))
    # hash each dest_id to appropriate detailed groups
    reservation <- reservation[,detail_group:=strtoi(substr(digest(paste0(hotel_id,salt),algo = "md5", serialize = F),29,32),16) %% tot_weight
                               ,by=c("hotel_id","salt","tot_weight")]
    exp_logic <- exp_summ[, list(detail_group = as.numeric(grp_list)), by = list(experiment_id, salt, logic, device, placement, locale)]

    # merge to get the corresponding logic name for each detail group
    reservation <- merge(reservation, exp_logic, by = c("experiment_id", "salt", "locale", "device", "placement", "detail_group"), all.x = T)
    ## cost data ##################################
    kayakstats_core <- getDataTableMS(sprintf("select
                                              date, hotel_id, locale, brand, 'web' device, 'core' placement
                                              , days_to_arrival bw, length_of_stay los, includes_saturday_night sat
                                              , sum(1) as clicks
                                              , sum(total_cost) cost,cpc_core CPC_core
                                              from ppc_kayak.KayakStats_CoreWeb_Multiplier_2018
                                              where date >= '%s' and  date < '%s'
                                              and click_type <> 'None'
                                              group by date,hotel_id,locale,brand,days_to_arrival,length_of_stay,includes_saturday_night", startDate, endDate))
    kayakstats_mob <- getDataTableMS(sprintf("select
                                             date, hotel_id, locale, brand, 'mobile' device, 'core' placement
                                             , days_to_arrival bw, length_of_stay los, includes_saturday_night sat
                                             , sum(1) as clicks
                                             , sum(total_cost) cost,cpc_core CPC_core
                                             from ppc_kayak.KayakStats_CoreMobile_Multiplier_2018
                                             where date >= '%s' and  date < '%s'
                                             and click_type <> 'None'
                                             group by date,hotel_id,locale,brand,days_to_arrival,length_of_stay,includes_saturday_night", startDate, endDate))
    # whiskey
    kayakWhisky_web <- getDataTableMS(sprintf("select * from (select date, hotel_id, locale, 'kayak' brand, 'web' device,'whisky' placement, 0 as bw, 0 as los, 0 as sat
                                              , sum(clicks_whisky+clicks_bob) clicks
                                              , round(sum(cost_whisky+cost_bob),2) cost
                                              , cpc_whisky CPC_core
                                              from ppc_kayak.KayakStats_WhiskyWeb_2018
                                              where date >= '%1$s' and date < '%2$s'
                                              group by date,hotel_id,locale,brand,bw,los,sat)as tt where tt.clicks >=1", startDate, endDate))
    kayakWhisky_mob <- getDataTableMS(sprintf("select * from (select date, hotel_id, locale, 'kayak' brand, 'mobile' device,'whisky' placement, 0 as bw, 0 as los, 0 as sat
                                              , sum(clicks_whisky+clicks_bob) clicks
                                              , round(sum(cost_whisky+cost_bob),2) cost
                                              , cpc_whisky CPC_core
                                              from ppc_kayak.KayakStats_WhiskyMobile_2018
                                              where date >= '%1$s' and date < '%2$s'
                                              group by date,hotel_id,locale,brand,bw,los,sat) as tt where tt.clicks >=1", startDate, endDate))
    kayakstats<-rbind(kayakstats_core,kayakstats_mob,kayakWhisky_web,kayakWhisky_mob)

    # resolve exp groups and mult dimensions in clicks stats

    kayakstats <- merge(kayakstats,  exp_summ[,.(salt= max(salt), tot_weight = sum(weight)), by= c("locale", "placement", "device", "experiment_id")]
                        ,by= c("locale", "placement", "device"))
    kayakstats <- kayakstats[,detail_group:=strtoi(substr(digest(paste0(hotel_id,salt),algo = "md5", serialize = F),29,32),16) %% tot_weight
                             ,by=c("hotel_id","salt","tot_weight")]
    kayakstats <- merge(kayakstats, exp_logic, by = c("experiment_id", "salt", "locale", "device", "placement", "detail_group"), all.x = T)

    # get affiliate id information? WHY here twice?
    affs <- unique(getDataTableMS("select locale,brand,device,kayak_provider_id,affiliate_id from ppc_kayak.KayakMapping_CoreWhisky where active=1;"))
    affs[, placement := ifelse(kayak_provider_id == 'BOOKINGDOTCOM','core',
                               ifelse(kayak_provider_id == 'BOOKINGDOTCOMWHISKY','whisky','rentals'))]
    setkey(affs, locale, device, placement)
    kayakstats <- merge(kayakstats, affs, by = c("locale","brand","device","placement"), all = T) # original written as all = T

    ## momondo locales coming in as kayak, Question: where there is na affiliate id, that is momondo?
    momLoc    <- unique(kayakstats[is.na(affiliate_id),]$locale)
    affMomLoc <- affs[(brand=='momondo')&(locale %in% momLoc),]
    affMomLoc[, brand := 'kayak']
    kayakstats[is.na(affiliate_id), affiliate_id := ifelse(locale == 'AT'& device == 'mobile', 390364,
                                                           ifelse(locale == 'AT' & device == 'web', 390353, ''))]
    kayakstats[is.na(kayakstats$affiliate_id), affiliate_id := ifelse(device == 'mobile', 1367595, 1367594)]
    kayakstats[is.na(kayakstats$kayak_provider_id), kayak_provider_id:='BOOKINGDOTCOM']
    kayakstats <- kayakstats[!is.na(clicks)]
    kayakstats[is.na(hotel_id),hotel_id:=10003]

    ## check if the locale is present in the description of the table, only when the locale match, continue with reservation and cost stats
    reservation_locale <- unique(reservation$locale)
    kayakstat_locale   <- unique(kayakstats$locale)
    local_comp <- getDataTableMS("desc ppc_kayak.summ_kayak_perf_dim;")[Field =='locale', Type]
    local_comp <- sub(")", "", sub("enum[(]", "", local_comp))
    tlb_local  <- gsub("'", '', unlist(strsplit(local_comp, split = ',')))

    if(length(setdiff(kayakstat_locale,tlb_local)) + length(setdiff(reservation_locale,tlb_local)) != 0){
        message = paste0(sprintf("Current new locale kayak reports: %s",setdiff(kayakstat_locale,tlb_local)),
                         sprintf("Current new locale reservation: %s",setdiff(reservation_locale,tlb_local)))
        print(message)
    }
    # temporarily not stopping the function as new locale does not generally infuence performance
    key_ = c("date","locale","placement","device", "experiment_id", "logic", "salt", "detail_group")

    ddSummRes <- reservation[,list(grossBookings=sum(bookings,na.rm = T)
                                   ,grossCommission=sum(commission_amount_euro,na.rm = T)
                                   ,netBookings=sum(status=="ok")
                                   ,netCommission=sum(ifelse(status=="ok",commission_amount_euro,0),na.rm = T)
                                   ,expectBookings=sum(expectBookings,na.rm = T)
                                   ,expectCommission=sum(expectCommission,na.rm = T))
                             ,by = key_]
    setkeyv(ddSummRes,key_)

    ddSummClicks <- kayakstats[,list(clicks=sum(clicks,na.rm = T)
                                     ,cost=sum(cost,na.rm = T))
                               ,by= key_]
    setkeyv(ddSummClicks,key_)

    perfDetail <- rbind(ddSummRes[ddSummClicks]
                        ,cbind(ddSummRes[!ddSummClicks],clicks=0,cost=0))
    # nulls are zeros
    perfDetail[is.na(grossBookings),grossBookings:=0]
    perfDetail[is.na(grossCommission),grossCommission:=0]
    perfDetail[is.na(netBookings),netBookings:=0]
    perfDetail[is.na(netCommission),netCommission:=0]
    perfDetail[is.na(expectBookings),expectBookings:=0]
    perfDetail[is.na(expectCommission),expectCommission:=0]

    CPC_core <- kayakstats[, list(CPC_core=median(CPC_core)), by = list(locale,placement,device)]
    CPC_core[is.na(CPC_core), CPC_core := 0]
    perfDetail <- merge(perfDetail,CPC_core,by = c('locale','placement','device'))

    return(perfDetail)
}

#' GetExpGroupAccountThree
#' Gets history of experiments and groups for AccountThree with salt
#' make it consistent across accounts
#'
#' @param
#' baseDate: date string in the format "YYYY-MM-DD"
#'
#' @return data.table with results
#'
#' @author  Lin Jia
#'
#' @import data.table
#'
#' @export
#'

GetExpGroupAccountThree <- function(endDate = format(Sys.Date(), "%F")){
    require(data.table, quietly = T)
    confFile <- "/shared/ppc/ppc_conf/sp/AccountThree/config/temp.conf" # temporary test version
    txtConf  <- scan(file = confFile,what = "char",quiet = T,comment.char = "#")
    ## peruse file sequentially for each set of experiments and keep data.table with summary
    expGroups = data.table()
    numExp   <- grep("<experiments",txtConf)

    for (i_exp in 1:length(numExp)) {
        fromExp <- numExp[i_exp] + 1
        toExp   <- ifelse(i_exp == length(numExp), length(txtConf), numExp[i_exp + 1] - 1)
        txtExp  <- txtConf[fromExp : toExp]
        placement <- gsub(">","",txtExp[1])
        numExpID = grep("<experiment",txtExp)

        for (i_id in 1:length(numExpID)) { # i_exp = 2
            fromID <- numExpID[i_id] + 1
            toID   <- ifelse(i_id == length(numExpID), length(txtExp), numExpID[i_id + 1] - 1)
            txtID  <- txtExp[fromID : toID]

            experiment_id         <- sub('^[^_]*_', '' , gsub(">","", txtID[+1]))
            salt                  <- txtID[grep("salt", txtID) + 1]
            participating_markets <- txtID[grep("distributions", txtID) + 1]
            groupID               <- txtID[(grep("<groups", txtID) + 1):length(txtID)]
            groupSet              <- grep("<group",groupID)
            experiment_testing    <- txtID[grep("experiment_testing", txtID) + 1]
            startDate             <- txtID[grep("^date_original$", txtID) + 1]
            lsDates               <- format(seq(as.Date(startDate),as.Date(endDate),1),"%F")

            grp_list_init <- 0
            for (i_group in 1:length(groupSet)) { # i_group = 1
                fromGroup <- groupSet[i_group] + 1
                toGroup   <- ifelse(i_group == length(groupSet), length(groupID), groupSet[i_group + 1] - 1)
                txtGroup  <- groupID[fromGroup:toGroup]
                logic     <- txtGroup[grep("logic",txtGroup) + 1]
                # add exact matching of weight
                weight    <- as.numeric(txtGroup[grep("^weight$",txtGroup) + 1])
                grp_list  <- paste(seq(grp_list_init,grp_list_init+weight-1,1),collapse = ",")
                grp_name  <- txtGroup[grep("name", txtGroup) + 1]
                logic     <- paste0(logic, "_", grp_name)
                test_control_group <- txtGroup[grep("test_control_group",txtGroup) + 1]
                expGroups <- rbind(expGroups
                                   ,data.table(start_date = startDate
                                               ,end_date = endDate
                                               ,experiment_id
                                               ,grp = i_group
                                               ,experiment_testing
                                               ,grp_name, weight, grp_list, logic, salt
                                               ,placement, participating_markets, test_control_group))
                grp_list_init <- grp_list_init + weight
            }
        }
    }
    return(expGroups)
}

#' getPerfExpAccountThree
#' Gets performance for account_3 experiments, adaptations of get_perf_hotel_exp in accountThreeFuncs.R
#'
#' @param
#' startDate, endDate
#'
#' @return data.table with results
#'
#' @author Lin Jia
#'
#' @import data.table
#'
#' @export
#'

getPerfExpAccountThree <- function(startDate = format(Sys.Date()-7, "%F")
                                  ,endDate = format(Sys.Date(), "%F")
                                  ,lsDistribution = NULL
                                  ,lsPlacement = c('DMeta','Mobile')
                                  ,experiment_id, salt) {
    suppressWarnings(suppressMessages({
        require(zoo, quietly = T)
        require(digest,quietly = T)
        require(data.table,quietly = T)
    }))

    if (is.null(lsDistribution)) {
        dist_clause = ""
    } else dist_clause = sprintf("and replace(silo, case when siloname like '%%DMeta' then 'DMeta'
                     when siloname like '%%Mobile' then 'Mobile'
                                 end, '') in ('%s')",paste(lsDistribution, collapse = "','"))
    pla_clause <- sprintf("and (case when siloname like '%%DMeta' then 'DMeta'
                          when siloname like '%%Mobile' then 'Mobile'
                          end) in ('%s')",paste(lsPlacement, collapse = "','"))

    ## clicks and cost from account_three.account_three_API_cost_granular --> AccountThreeAPICostSiloHotel_YYYYMM
    ads <- data.table()
    for (ym in unique(format(seq(as.Date(startDate),as.Date(endDate),1),"%Y%m"))) {
        q1 = sprintf("select date
                     ,case when siloname like '%%DMeta' then 'DMeta'
                     when siloname like '%%Mobile' then 'Mobile'
                     end placement
                     ,replace(siloname, case when siloname like '%%DMeta' then 'DMeta'
                     when siloname like '%%Mobile' then 'Mobile'
                     end, '') distribution
                     ,h.id dest_id
                     ,length_of_stay los
                     ,sum(cost_EUR) cost
                     ,count(*) clicks
                     from account_3.AccountThreeAPIClickCost_CET_%1$s a
                     join bp_slice.B_Hotel h on h.id = a.partnerpropertyid
                     where date >= '%2$s' and date < '%3$s'
                     %4$s
                     %5$s
                     group by 1,2,3,4,5;",ym,startDate,endDate,pla_clause,dist_clause)
        tryCatch(ads <- rbind(ads, getDataTableMS(q1))
                 , error=function(e) return(NULL))
    }
    ## bookings, commission (plus net) from SPRes. and expected from nits
    if (is.null(lsDistribution)) {
        dist_clause = ""
    } else dist_clause = sprintf("and a.distribution in ('%s')",paste(lsDistribution, collapse = "','"))
    pla_clause <- sprintf("and a.placement in ('%s')",paste(lsPlacement, collapse = "','"))
    q2 = sprintf("select substr(r.created,1,10) date
                 , r.affiliate_id, r.status, r.checkin, r.checkout, r.label
                 , a.placement, a.distribution
                 , coalesce(r.dest_id, r.hotel_id) dest_id
                 , los length_of_stay
                 , sum(1) conversions
                 , sum(commission_amount_euro) commission
                 , sum(case when status = 'ok' then 1 else 0 end) net_conversions
                 , sum(case when status = 'ok' then commission_amount_euro else 0 end) net_commission
                 , sum(coalesce(ng.stay_probability_wnd, ng.stay_probability, ng.stay_probability_analytics)) expected_conversions
                 , sum(commission_amount_euro * coalesce(ng.stay_probability_wnd, ng.stay_probability, ng.stay_probability_analytics)) expected_commission
                 , sum(price_euro) total_value
                 from ppc_sp.SPReservation r
                 join bp_slice.FlatAffiliate a on (a.affiliate_id = r.affiliate_id
                 and a.partner_id = 404815 and a.placement in ('DMeta','Mobile'))
                 left join bp_slice.B_Hotel h on (h.id = coalesce(r.dest_id, r.hotel_id))
                 left join ppc_sp.SPReservationIntentToStayNG ng on (ng.hotelreservation_id = r.hotelreservation_id
                 and ng.date_created >= '2016-04-22')
                 where r.created >= '%1$s' and r.created < '%2$s'
                 and r.status not in ('fraudulent','test','unknown')
                 and r.partner_id = 404815
                 %3$s %4$s
                 group by 1,2,3,4,5,6,7,8,9,10",startDate,endDate,pla_clause,dist_clause)
    sale <- getDataTableMS(q2)
    q3 <- sprintf("select r.date_cancelled date
                  , r.affiliate_id, r.status, r.checkin, r.checkout, r.label
                  , a.placement, a.distribution
                  , coalesce(r.dest_id, r.hotel_id) dest_id
                  , los length_of_stay
                  , sum(1) cancellations
                  , sum(commission_amount_euro) comm_canc_asbooked
                  from ppc_sp.SPReservation r force index (cancel)
                  join bp_slice.FlatAffiliate a on (a.affiliate_id = r.affiliate_id
                  and a.partner_id = 404815 and a.placement in ('DMeta','Mobile'))
                  where r.date_cancelled >= '%1$s' and r.date_cancelled < '%2$s'
                  and r.status not in ('fraudulent','test','unknown')
                  and r.partner_id = 404815
                  %3$s %4$s
                  group by 1,2,3,4,5,6,7,8,9,10",startDate,endDate,pla_clause,dist_clause)
    cancels <- getDataTableMS(q3)
    setkey(sale,date,affiliate_id,status,checkin,checkout,label,placement,distribution,dest_id,length_of_stay)
    setkey(cancels,date,affiliate_id,status,checkin,checkout,label,placement,distribution,dest_id,length_of_stay)
    sale <- rbind(sale[cancels]
                  , cbind(sale[!cancels],cancellations=0, comm_canc_asbooked=0))
    q4 <- sprintf("select checkout date
                , r.affiliate_id, r.status, r.checkin, r.checkout, r.label
                , a.placement
                , a.distribution
                , coalesce(r.dest_id, r.hotel_id) dest_id
                , los length_of_stay
                , sum(1) stayed_conversions
                , sum(commission_amount_euro) stayed_commission
                from ppc_sp.SPReservation r
                join bp_slice.FlatAffiliate a on (a.affiliate_id = r.affiliate_id and a.partner_id = 404815 and a.placement in ('DMeta','Mobile'))
                where r.checkout >= '%1$s' and r.checkout < '%2$s'
                and r.status in ('ok')
                and r.partner_id = 404815
                and a.placement in ('DMeta','Mobile')
                %3$s %4$s
                group by 1,2,3,4,5,6,7,8,9,10;",startDate,endDate,pla_clause,dist_clause)
    stayed <- getDataTableMS(q4)
    setkey(stayed,date,affiliate_id,status,checkin,checkout,label,placement,distribution,dest_id,length_of_stay)
    setkey(sale,date,affiliate_id,status,checkin,checkout,label,placement,distribution,dest_id,length_of_stay)
    sale <- rbind(sale[stayed]
                  , cbind(sale[!stayed],stayed_conversions=0,stayed_commission=0))

    # resolve label parameters
    #sale <- merge(sale,affs,by="affiliate_id")
    sale[,label:=tolower(label)]
    sale[grepl("_los-",label),los:=as.numeric(gsub(".*_los-([0-9]+).*","\\1",label,perl = T))]
    sale <- sale[,los := ifelse(is.na(los),length_of_stay,los)]
    sale <- sale[,length_of_stay:= NULL]
    sale <- sale[,status:= NULL]
    sale <- sale[,checkin:= NULL]
    sale <- sale[,checkout:= NULL]
    sale <- sale[,label:= NULL]
    sale <- sale[,affiliate_id:= NULL]

    if (nrow(ads) > 0 & nrow(sale) > 0) {
        lastFullDate <- min(max(ads$date), max(sale$date))
        ads <- ads[date <= lastFullDate]
        sale <- sale[date <= lastFullDate]
        setkey(ads, date,placement,distribution,dest_id,los)
        setkey(sale, date,placement,distribution,dest_id,los)
        perfDD <- rbind(sale[ads] ## outer join
                        ,cbind(sale[!ads],cost=0,clicks=0)) ## bookings without clicks
        perfDD <- perfDD[is.na(conversions),conversions:=0]
        perfDD <- perfDD[is.na(commission),commission:=0]
        perfDD <- perfDD[is.na(net_conversions),net_conversions:=0]
        perfDD <- perfDD[is.na(net_commission),net_commission:=0]
        perfDD <- perfDD[is.na(expected_conversions),expected_conversions:=0]
        perfDD <- perfDD[is.na(expected_commission),expected_commission:=0]
        perfDD <- perfDD[is.na(total_value),total_value:=0]
        perfDD <- perfDD[is.na(cancellations),cancellations:=0]
        perfDD <- perfDD[is.na(comm_canc_asbooked),comm_canc_asbooked:=0]
        perfDD <- perfDD[is.na(stayed_conversions),stayed_conversions:=0]
        perfDD <- perfDD[is.na(stayed_commission),stayed_commission:=0]

        # get experiment information
        # Newly ADDED Resolve experiment (internal) groups from experiments_overview, only filter for those that are considered to be experiments
        exp_overview <- getDataTableMS(sprintf("select experiment_id, salt, logic, weight, grp_list,
                                           participating_markets, placement from account_3.experiments_evaluation_overview
                                           where experiment_testing = True and experiment_id = '%s' and salt = '%s'",
                                               experiment_id, salt))

        exp_summ <- exp_overview[, list(distribution = unlist(strsplit(participating_markets, ","))),
                                 by=list(experiment_id, salt, logic, weight, grp_list, placement)]

        perfDD <- merge(perfDD, exp_summ[,.(salt= max(salt), tot_weight = sum(weight)),by= c("distribution", "placement", "experiment_id")]
                             ,by= c( "distribution", "placement"))
        # hash each dest_id to appropriate detailed groups
        perfDD <- perfDD[,detail_group:=strtoi(substr(digest(paste0(dest_id,salt),algo = "md5", serialize = F),29,32),16) %% tot_weight
                                   ,by=c("dest_id","salt","tot_weight")]
        exp_logic <- exp_summ[, list(detail_group = as.numeric(unlist(strsplit(grp_list, ",")))),
                              by = list(experiment_id, placement, salt, logic, distribution)]

        # merge to get the corresponding logic name for each detail group
        perfDD <- merge(perfDD, exp_logic, by = c("experiment_id", "salt", "distribution", "placement", "detail_group"), all.x = T)

        key_ = c("date", "placement", "distribution", "los","experiment_id", "salt", "logic", "detail_group")

        perfDD <- perfDD[,list(clicks = sum(clicks, na.rm =T)
                               ,cost = sum(cost, na.rm = T)
                               ,grossBookings=sum(conversions,na.rm = T)
                               ,grossCommission=sum(commission,na.rm = T)
                               ,netBookings=sum(net_conversions, na.rm = T)
                               ,netCommission=sum(net_commission,na.rm = T)
                               ,expectBookings=sum(expected_conversions,na.rm = T)
                               ,expectCommission=sum(expected_commission,na.rm = T))
                         ,by = key_]
        setkeyv(perfDD,key_)

        return(perfDD)
    } else{
        print(sprintf("nrow in cost data is %s and nrow in reservation data is %s", nrow(ads), nrow(sale)))
    }
}

#' GetExpGroupAccountOne
#' Gets history of experiments and groups for account_1 with salt
#' make it consistent with account_3 and account_4
#'
#' @param
#' baseDate: date string in the format "YYYY-MM-DD"
#'
#' @return data.table with results
#'
#' @author  Lin Jia
#'
#' @import data.table
#'
#' @export
#'
GetExpGroupAccountOne <- function(startDate = format(Sys.Date()-30, "%F")
                           ,endDate = format(Sys.Date(), "%F")){
    require(data.table, quietly = T)
    lsDates  <- format(seq(as.Date(startDate),as.Date(endDate),1),"%F")
    confFile <- "/shared/ppc/ppc_conf/sp/account_1/experiment_config/main.conf"
    txtConf  <- scan(file = confFile,what = "char",quiet = T,comment.char = "#")
    ## peruse file sequentially for each set of experiments and keep data.table with summary
    expGroups = data.table()
    numExp   <- grep("<experiments",txtConf)

    for (i_exp in 1:length(numExp)) {
        fromExp <- numExp[i_exp] + 1
        toExp   <- ifelse(i_exp == length(numExp), length(txtConf), numExp[i_exp + 1] - 1)
        txtExp  <- txtConf[fromExp : toExp]
        #placement = gsub(">","",txtExp[1])
        numExpID = grep("<experiment",txtExp)

        for (i_id in 1:length(numExpID)) { # i_exp = 2
            fromID <- numExpID[i_id] + 1
            toID   <- ifelse(i_id == length(numExpID), length(txtExp), numExpID[i_id + 1] - 1)
            txtID  <- txtExp[fromID : toID]

            experimentID <- gsub(">","",txtID[+1])
            salt         <- txtID[grep("salt", txtID) + 1]
            campaigns    <- txtID[grep("campaigns", txtID) + 1]
            groupID      <- txtID[(grep("<groups", txtID) + 1):length(txtID)]
            groupSet     <- grep("<group",groupID)
            expTesting   <- txtID[grep("experiment_testing", txtID) + 1]

            grp_list_init <- 0
            for (i_group in 1:length(groupSet)) { # i_group = 1
                fromGroup <- groupSet[i_group] + 1
                toGroup   <- ifelse(i_group == length(groupSet), length(groupID), groupSet[i_group + 1] - 1)
                txtGroup  <- groupID[fromGroup:toGroup]
                logic     <- txtGroup[grep("logic",txtGroup) + 1]
                # add exact matching of weight
                weight    <- as.numeric(txtGroup[grep("^weight$",txtGroup) + 1])
                grp_list  <- paste(seq(grp_list_init,grp_list_init+weight-1,1),collapse = ",")
                grp_name  <- paste0("group", i_group)
                test_control_group <- txtGroup[grep("test_control_group",txtGroup) + 1]
                expGroups <- rbind(expGroups
                                   ,data.table(date = lsDates
                                               ,experiment_id = experimentID
                                               ,grp = i_group
                                               ,grp_name = grp_name
                                               ,weight, grp_list, logic, salt
                                               ,experiment_testing = expTesting
                                               ,participating_markets = campaigns
                                               ,test_control_group =  test_control_group))
                grp_list_init <- grp_list_init + weight
            }
        }
    }
    return(expGroups)
}

#' getPerfExpAccountOne
#' Gets performance summary at experiments level for each detail group
#'
#' @param
#' baseDate,endDate: date limits "YYYY-MM-DD"
#' dest_ids: limit query for specific hotel ids
#' include_hotel_country: if true returns aggregate by hotel country too
#' book_window_num: flag, if true returns booking window as integers (default False)
#' los/bw_cap: max values for length of stay and booking window. Used to be 14/180 and changed to 30/360
#'
#' @return data.table with results
#'
#' @author Lin Jia
#'
#' @import data.table
#'
#' @export
#'
#'
getPerfExpAccountOne <- function(baseDate = format(Sys.Date()-30, "%F")
                          ,endDate = format(Sys.Date(), "%F")
                          ,book_window_num = F
                          ,los_cap = 30
                          #,bw_cap = 360
                          ,include_hotel_country = F
                          ,experiment_id
                          ,salt) {
    suppressWarnings(suppressMessages({
        require(data.table, quietly = T)
        require(digest, quietly = T)
        require(reshape2, quietly = T)
        require(zoo, quietly = T)
    }))
    endDate <- min(endDate,format(Sys.Date(), "%F"))

    siteList <- c("localuniversal","mapresults")
    deviceList <- c("desktop","mobile","tablet")

    # update the booking window dimension calculation
    bookWindow <- function(bw) {
        return(ifelse(bw<3,"0"
                      ,ifelse(bw<7,"3"
                              ,ifelse(bw<14,"7"
                                      ,ifelse(bw<30,"14"
                                              ,ifelse(bw<60,"30"
                                                      ,ifelse(bw<90,"60"
                                                              ,ifelse(bw<180,"90", "180"))))))))
    }


    # get bookings and commissions from SPReservation and clicks and cost from AccountOneStats
    # for SPReservations get params from label
    ddValidLabels <- getDataTableMS(sprintf("
                                            SELECT substr(r.created,1,10) yyyy_mm_dd
                                            , r.affiliate_id, r.status, r.checkin, r.checkout, r.label, r.hotel_id
                                            , sum(1) bookings
                                            , sum(r.commission_amount_euro) commission_amount_euro
                                            , sum(coalesce(ng.stay_probability_wnd, ng.stay_probability, ng.stay_probability_analytics)) expectBookings
                                            , sum(r.commission_amount_euro * coalesce(ng.stay_probability_wnd, ng.stay_probability, ng.stay_probability_analytics)) expectCommission
                                            FROM ppc_sp.SPReservation r
                                            left join ppc_sp.SPReservationIntentToStayNG ng on (ng.hotelreservation_id = r.hotelreservation_id
                                            and ng.date_created >= '2016-04-22')
                                            where r.created >= '%s' and r.created < '%s'
                                            and r.status not in ('fraudulent','test','unknown')
                                            and r.partner_id = 423463
                                            group by 1,2,3,4,5,6,7",baseDate,endDate))

    setkey(ddValidLabels,yyyy_mm_dd, affiliate_id,status,checkin,checkout,label,hotel_id)
    # merge with cancellations to get as_booked
    cancels <- getDataTableMS(sprintf("select r.date_cancelled yyyy_mm_dd
                                      , r.affiliate_id, r.status, r.checkin, r.checkout, r.label, r.hotel_id
                                      , sum(1) cancellations
                                      , sum(r.commission_amount_euro) comm_canc_asbooked
                                      from ppc_sp.SPReservation r force index (cancel)
                                      where r.date_cancelled >= '%1$s' and r.date_cancelled < '%2$s'
                                      and r.status not in ('fraudulent','test','unknown')
                                      and r.partner_id = 423463
                                      group by 1,2,3,4,5,6,7",baseDate,endDate))
    setkey(cancels,yyyy_mm_dd, affiliate_id,status,checkin,checkout,label,hotel_id)
    ddValidLabels <- rbind(ddValidLabels[cancels]
                           , cbind(ddValidLabels[!cancels],cancellations=0, comm_canc_asbooked=0))
    ddValidLabels <- ddValidLabels[is.na(bookings), bookings := 0]
    ddValidLabels <- ddValidLabels[is.na(commission_amount_euro), commission_amount_euro := 0]
    ddValidLabels <- ddValidLabels[is.na(expectBookings), expectBookings := 0]
    ddValidLabels <- ddValidLabels[is.na(expectCommission), expectCommission := 0]

    # arbitrary condition to filter for missing NITS (nits per booking > X)
    dates_low_nits <- ddValidLabels[,.(nits_per_book=sum(expectCommission,na.rm = T)/sum(bookings, na.rm = T)), by=.(yyyy_mm_dd)][nits_per_book < 10]
    if (nrow(dates_low_nits) > 0) {
        print("The following dates have low NITS values and will be ignored")
        print(dates_low_nits)
        ddValidLabels <- ddValidLabels[! yyyy_mm_dd %in% dates_low_nits$yyyy_mm_dd]
        if (nrow(ddValidLabels) == 0) {
            print ("number of rows in bookings data is zero because of low nits value, stop the function")
            stop()
        }
        # change endDate to max in reservations (assuming bad NITS valules always happen for the most recent dates)
        endDate = format(as.Date(max(ddValidLabels$yyyy_mm_dd))+1, "%F")
    }
    affs <- getDataTableMS(sprintf("select affiliate_id, placement, distribution, bucket
                                   from bp_slice.FlatAffiliate WHERE partner_id = 423463"))
    # resolve label parameters
    ddValidLabels <- merge(ddValidLabels,affs,by="affiliate_id")
    ddValidLabels[,label:=tolower(label)]
    ddValidLabels[grepl("_site-",label),site:=gsub(".*site-([a-z]+)\\_(.*)","\\1",label,perl = T)]
    ddValidLabels[grepl("-link-",label),site:=gsub(".*link-([a-z]+)-(.*)","\\1",label,perl = T)]
    # for these labels get cc from "link-[a-z]" written first in "site"
    ddValidLabels[grepl("-link-",label),cc:=substr(site,nchar(site)-1,nchar(site))]
    ddValidLabels[grepl("-link-",label),site:=substr(site,1,nchar(site)-2)]
    ddValidLabels[is.na(site) & placement=="MR", site:="mapresults"]
    ddValidLabels[is.na(site) & placement=="LU", site:="localuniversal"]
    ddValidLabels <- ddValidLabels[site %in% siteList] # keep only relevant sites

    ddValidLabels[grepl("_ucc-",label),cc:=gsub(".*ucc-([a-z]+)\\_(.*)","\\1",label,perl = T)]
    ddValidLabels[,cc:=toupper(cc)]
    ddValidLabels[is.na(cc),cc:=distribution]

    # get campagin_id from merging with get_campaign_cc campaign_id, cc, and campaign name
    campaign_id_cc <- get_campaign_cc()
    ddValidLabels  <- merge(ddValidLabels, campaign_id_cc[, list(campaign_id_cc = campaign_id, cc)], by = "cc", all.x = T)
    # Resolve CAMPAIGN from cid label if it exists
    ddValidLabels[grepl("_cid-",label),campaign_id:=as.numeric(gsub(".*_cid-([0-9]+).*","\\1",label,perl = T))]
    # if not found in label set id by the country
    ddValidLabels[is.na(campaign_id), campaign_id := campaign_id_cc]
    print(sprintf("%s %% of rows have NULL Campaign ID (found by CID or cc in the label). 'Fixing' them with EMEA_other as default"
                  ,round(100*mean(is.na(ddValidLabels$campaign_id)),2)))
    ddValidLabels[is.na(campaign_id), campaign_id := 72] ## HARD CODING EMEA_other HERE!! TODO:: REVIEW THIS
    unique_campaign_id_cc<- unique(campaign_id_cc[,list(campaign_id, campaign_name)])
    ddValidLabels <- merge(ddValidLabels, unique_campaign_id_cc, by = "campaign_id")

    ddValidLabels[grepl("_device-",label),device:=gsub(".*_device-([a-z]+)\\_(.*)","\\1",label,perl = T)]
    ddValidLabels[grepl("_dev-",label),device:=gsub(".*_dev-([a-z]+)\\_(.*)","\\1",label,perl = T)]
    ddValidLabels <- ddValidLabels[is.na(device),device:=tolower(ifelse(bucket=="PC","desktop",bucket))]
    ddValidLabels[! (device %in% deviceList), device:=tolower(ifelse(bucket=="PC","desktop",bucket))]

    ddValidLabels <- ddValidLabels[,los:=as.numeric(NA)]
    ddValidLabels[grepl("_los-",label),los:=as.numeric(gsub(".*_los-([0-9]+).*","\\1",label,perl = T))]
    ddValidLabels <- ddValidLabels[is.na(los),los:=as.numeric(difftime(as.Date(checkout),as.Date(checkin),units = "days"))]
    ddValidLabels <- ddValidLabels[los>los_cap, los:=los_cap]
    ddValidLabels <- ddValidLabels[,los:=formatC(los,width = 2,flag = "0")]

    ddValidLabels <- ddValidLabels[,checkinDOW:=as.character(NA)]
    ddValidLabels <- ddValidLabels[grepl("_dow-",label),checkinDOW:=gsub(".*_dow-(mon|tue|wed|thu|fri|sat|sun).*","\\1",label,perl = T)]
    ddValidLabels <- ddValidLabels[!is.na(checkinDOW),checkinDOW:=paste0(toupper(substr(checkinDOW,1,1)),substr(checkinDOW,2,3))]
    ddValidLabels <- ddValidLabels[is.na(checkinDOW),checkinDOW:=format(as.Date(checkin), "%a")]

    ddValidLabels <- ddValidLabels[,bw:=as.numeric(NA)]
    ddValidLabels <- ddValidLabels[grepl("_bw-",label),bw:=as.numeric(gsub(".*_bw-([0-9]+).*","\\1",label,perl = T))]
    ddValidLabels <- ddValidLabels[is.na(bw),bw:=as.numeric(difftime(as.Date(checkin),as.Date(yyyy_mm_dd),units = "days"))]
    ddValidLabels <- ddValidLabels[,bookWindow:=bookWindow(bw),by=.(bw)]
    ddValidLabels <- ddValidLabels[,bw:=NULL]

    ddValidLabels <- ddValidLabels[,defdate:="NA"]
    ddValidLabels <- ddValidLabels[grepl("_defdate-",label),defdate:=gsub(".*_defdate-([0-1]).*","\\1",label,perl = T)]
    ddValidLabels <- ddValidLabels[! defdate %in% c("0","1","NA"), defdate := "NA"]
    nn = nrow(ddValidLabels[defdate == "NA"])
    # ddValidLabels[,list(nn=sum(ifelse(defdate!="NA",1,0))/.N),by=c("yyyy_mm_dd")]
    ## If amount of NAs < 10% fix unknown default_dates by the prevalence in known ones, otherwise let as NA
    if (nn/nrow(ddValidLabels) < .1) {
        prevalence = nrow(ddValidLabels[defdate == "1"])/nrow(ddValidLabels)
        print(sprintf("%s %% of rows have NULL default dates. 'Fixing' them with %s %% default, as the prevalence in known ones"
                      ,round(100*mean(ddValidLabels$defdate == "NA"),1), round(100*prevalence,1)    ))
        # ex: "2 % of rows have NULL default dates. Fixing them with 34.2 % default, as the prevalence in known ones"
        ddValidLabels <- ddValidLabels[defdate == "NA", row_na := .I]
        choose_def <- sample(1:nn, size = round(nn*prevalence))
        ddValidLabels <- ddValidLabels[!is.na(row_na) & row_na %in% choose_def, defdate := "1"]
        ddValidLabels <- ddValidLabels[!is.na(row_na) & ! row_na %in% choose_def, defdate := "0"]
    }

    ddValidLabels <- ddValidLabels[,yyyy_mm_dd:=as.character(yyyy_mm_dd)]

    # hotel ID
    ddValidLabels <- ddValidLabels[, dest_id := hotel_id]
    ddValidLabels <- ddValidLabels[grepl("gcad-",label),dest_id:=as.numeric(gsub(".*gcad-([0-9]+).*","\\1",label,perl = T))]
    ddValidLabels <- ddValidLabels[grepl("hotel-",label),dest_id:=as.numeric(gsub(".*hotel-([0-9]+).*","\\1",label,perl = T))]
    ddValidLabels <- ddValidLabels[is.na(dest_id), dest_id := hotel_id]

    # Resolve experiment (internal) groups from experiments overview to be able to capture past experiments
    exp_summ <- getDataTableMS(sprintf("select experiment_id, salt, logic, weight, grp_list detail_group, participating_markets
                                       from account_1.experiments_evaluation_overview
                                       where experiment_id = '%s' and salt = '%s'", experiment_id, salt))
    exp_summ[, detail_group := as.numeric(detail_group)]
    exp_summ <- exp_summ[, list(campaign_name = unlist(strsplit(participating_markets, ","))),
                         by=list(experiment_id, salt, logic, weight, detail_group)]
    exp_summ <- merge(exp_summ, unique_campaign_id_cc, by = "campaign_name", all.x = TRUE)

    ddValidLabels <- merge(ddValidLabels, exp_summ[,.(salt=unique(salt), tot_weight=sum(weight)),by="campaign_id"], by="campaign_id", all.x=T)

    # hash each dest_id to appropriate detailed groups
    ddValidLabels <- ddValidLabels[,detail_group:=strtoi(substr(digest(paste0(dest_id,salt),algo = "md5", serialize = F),29,32),16) %% tot_weight
                                   ,by=c("dest_id","salt","tot_weight")]

    detail_group  <- exp_summ[, list(campaign_id, detail_group, logic, experiment_id)]
    ddValidLabels <- merge(ddValidLabels, detail_group, by = c("campaign_id","detail_group"), all.x = T)

    ################### Process clicks and cost

    # traverse all relevant monthly and daily tables XXX -->> moving to hive [2017-12-06]
    # -->> moving back if less data (MySQL is more reliable/recent) [2018-02-26]
    if (as.numeric(as.Date(endDate)-as.Date(baseDate)) < 29) {
        # get from MySQL
        ddClicks <- data.table()
        for (day_ in unique(format(seq(as.Date(max(baseDate,"2016-01-01")),as.Date(endDate)-1,1),"%Y-%m-%d"))) { # day_ = "2018-03-01"
            tryCatch({
                ddClicks <- rbind(ddClicks
                                  , getDataTableMS(sprintf("SELECT '%1$s' yyyy_mm_dd, campaign_id
                                                           , property site, country cc, device, length_of_stay los, check_in_date
                                                           , case when date_type = 'default' then '1' else '0' end defdate
                                                           , hotel_id dest_id
                                                           , sum(clicks) clicks
                                                           , avg(case when ad_position = -1 then NULL when ad_position = 0 then NULL else ad_position end) avg_ad_position
                                                           , sum(impressions) impressions
                                                           , sum(eligible_impressions) eligible_impressions
                                                           , sum(billing_cost_usd) costUSD
                                                           FROM account_1.AccountOne_CampaignStats_%2$s
                                                           WHERE clicks > 0
                                                           and lower(property) in ('%3$s')
                                                           group by campaign_id, property, country, device, length_of_stay, check_in_date, case when date_type = 'default' then '1' else '0' end, hotel_id"
                                                           ,day_,gsub("-","",day_),paste0(siteList,collapse = "','"))))
            }, error = function(e) {
                print(sprintf("error for day %s: %s",day_, e[1]))
            })
        }
    } else {
        # get from hive if many days
        ddClicks <- getDataTableHive(sprintf("SELECT yyyy_mm_dd, campaign_id
                                             , property site, country cc, device, length_of_stay los, check_in_date
                                             , case when date_type = 'default' then '1' else '0' end defdate
                                             , hotel_id dest_id
                                             , sum(clicks) clicks
                                             , avg(case when ad_position = -1 then NULL when ad_position = 0 then NULL else ad_position end) avg_ad_position
                                             , sum(impressions) impressions
                                             , sum(eligible_impressions) eligible_impressions
                                             , sum(billing_cost_usd) costUSD
                                             FROM spmeta.gha_campaign_stats
                                             WHERE yyyy_mm_dd >= '%1$s' and yyyy_mm_dd < '%2$s'
                                             and clicks > 0
                                             and lower(property) in ('%3$s')
                                             GROUP BY yyyy_mm_dd, campaign_id
                                             , property, country, device, length_of_stay, check_in_date
                                             , case when date_type = 'default' then '1' else '0' end
                                             , hotel_id",baseDate,endDate,paste0(siteList,collapse = "','"))
                                     , stringsAsFactors = F)
    }

    if (is.null(ddClicks$yyyy_mm_dd) | nrow(ddClicks) == 0) stop("No data found for these parameters")

    # resolve for campaign id if not exists
    ddClicks <- merge(ddClicks, campaign_id_cc[, list(campaign_id_cc = campaign_id, cc)], by = "cc", all.x = T)
    # if not found in label set id by the country
    ddClicks[is.na(campaign_id), campaign_id := campaign_id_cc]
    print(sprintf("%s %% of rows have NULL Campaign ID (found by CID or cc in the label). 'Fixing' them with EMEA_other as default"
                  ,round(100*mean(is.na(ddClicks$campaign_id)),2)))
    ddClicks[is.na(campaign_id), campaign_id := 72] ## HARD CODING EMEA_other HERE!! TODO:: REVIEW THIS
    ddClicks <- merge(ddClicks, unique_campaign_id_cc, by = "campaign_id")

    setkey(ddClicks,yyyy_mm_dd,site,campaign_id,campaign_name,cc,device,los,check_in_date,defdate)

    # Resolve experiment (internal) groups from experiments_overview
    ddClicks <- merge(ddClicks, exp_summ[,.(salt=unique(salt), tot_weight=sum(weight)),by="campaign_id"], by="campaign_id")
    # hash each dest_id to appropriate detailed groups
    ddClicks <- ddClicks[, detail_group := strtoi(substr(digest(paste0(dest_id,salt), algo = "md5", serialize = F ),29,32),16) %% tot_weight
                         ,by=c("dest_id","salt","tot_weight")]
    ddClicks <- merge(ddClicks, detail_group, by = c("campaign_id","detail_group"), all.x = T)

    # Get exchange rates from MS to convert USD to EUR (fill all dates for eventual ranges with more than on day)
    # Get from 7 days before baseDate in case we're querying for 1 day only which has no rates yet.
    lsDatesXR = format(seq(as.Date(baseDate)-7,Sys.Date()-1,1), "%F") # all expected dates
    XRates <- getDataTableMS(sprintf("select date_from, value from bp_slice.B_FinancePeriodCurrency
                                     where iso_currencycode = 'USD' and date_from >= '%s'",format(as.Date(baseDate)-7,"%F")))
    XRates <- merge(data.table(date_from=lsDatesXR),XRates,by="date_from",all.x=T)
    setkey(XRates,date_from)
    XRates <- XRates[,value:=na.locf(value)] # fill NAs based on previous known value
    ddClicks <- merge(ddClicks,XRates[,list(yyyy_mm_dd=date_from,value)],by="yyyy_mm_dd",all.x=T)
    ddClicks <- ddClicks[, cost := as.numeric(costUSD) * as.numeric(value)]
    ddClicks <- ddClicks[,costUSD:=NULL]
    ddClicks <- ddClicks[,value:=NULL]
    setkey(ddClicks,yyyy_mm_dd,site,cc,campaign_id,device,los,check_in_date, defdate,detail_group)
    # calc DOW and bookWindow
    ddClicks[,checkinDOW:=format(as.Date(check_in_date), "%a")]
    ddClicks[,bw:=as.numeric(difftime(as.Date(check_in_date),as.Date(yyyy_mm_dd),units = "days"))]
    ddClicks[,bookWindow:=bookWindow(bw),by=.(bw)]
    ddClicks[,bw:=NULL]
    ddClicks[,site:=tolower(site)]
    ddClicks <- ddClicks[site %in% siteList]
    ddClicks[,cc:=as.character(cc)]
    ddClicks[,yyyy_mm_dd:=as.character(yyyy_mm_dd)]
    ddClicks[,device:=as.character(device)]
    ddClicks[,defdate:=as.character(defdate)]
    ddClicks[los > los_cap, los := los_cap]
    ddClicks[,los:=formatC(los,width = 2,flag = "0")]

    # if including hotel_country add it to the atomic tables and set it in the key
    if (include_hotel_country) {
        hotel_ccs <- getDataTableMS("select id dest_id, cc1 hotel_cc from bp_slice.B_Hotel")
        ddValidLabels <- merge(ddValidLabels, hotel_ccs, by = "dest_id", all.x = T)
        ddValidLabels[is.na(hotel_cc), hotel_cc := "--"]
        ddClicks <- merge(ddClicks, hotel_ccs, by = "dest_id", all.x = T)
        ddClicks[is.na(hotel_cc), hotel_cc := "--"]
        key_ = c("yyyy_mm_dd","campaign_id","site","campaign_name","device","los","checkinDOW","bookWindow","defdate"
                 ,"detail_group","logic","experiment_id", "salt", "hotel_cc")
    } else {
        key_ = c("yyyy_mm_dd","campaign_id","site","campaign_name","device","los","checkinDOW","bookWindow","defdate"
                 ,"detail_group","logic","experiment_id", "salt")
    }
    # key_ = c("dest_id")
    # aggregate bookings/revenues and clicks/cost by relevant dimensions in key_
    # consider only day/campaign pairs which are available in ddClicks (active campaigns)
    ddSummRes <- ddValidLabels[,list(grossBookings=sum(bookings,na.rm = T)
                                     ,grossCommission=sum(commission_amount_euro,na.rm = T)
                                     ,netBookings=sum(status=="ok")
                                     ,netCommission=sum(ifelse(status=="ok",commission_amount_euro,0),na.rm = T)
                                     ,expectBookings=sum(expectBookings,na.rm = T)
                                     ,expectCommission=sum(expectCommission,na.rm = T)
                                     ,cancellations=sum(cancellations,na.rm = T)
                                     ,comm_canc_asbooked=sum(comm_canc_asbooked,na.rm = T))
                               ,by=key_]
    setkeyv(ddSummRes,key_)
    ddSummClicks <- ddClicks[,list(clicks=sum(clicks,na.rm = T)
                                   ,cost=sum(cost,na.rm = T)
                                   ,avg_ad_position = round(mean(avg_ad_position, na.rm = T),3)
                                   ,impressions = sum(impressions, na.rm = T)
                                   ,eligible_impressions = sum(eligible_impressions, na.rm = T))
                             ,by=key_]
    setkeyv(ddSummClicks,key_)
    # left outer join clicks/cost and then add bookings without clicks/cost if any
    perfDetail <- ddSummRes[ddSummClicks]
    lose_bookings <- ddSummRes[!ddSummClicks]
    if (nrow(lose_bookings) > 0) {
        perfDetail <- rbind(perfDetail, cbind(lose_bookings,clicks = 0, avg_ad_position = NA,
                                              impressions = 0, eligible_impressions = 0, cost=0))
    }
    setkeyv(perfDetail,key_)
    # nulls are zeros
    perfDetail[is.na(grossBookings),grossBookings:=0]
    perfDetail[is.na(grossCommission),grossCommission:=0]
    perfDetail[is.na(netBookings),netBookings:=0]
    perfDetail[is.na(netCommission),netCommission:=0]
    perfDetail[is.na(expectBookings),expectBookings:=0]
    perfDetail[is.na(expectCommission),expectCommission:=0]
    perfDetail[is.na(cancellations),cancellations:=0]
    perfDetail[is.na(comm_canc_asbooked),comm_canc_asbooked:=0]
    # add # occurences
    perfDetail[,nOccur:=.N,by=key_]
    return(perfDetail)
}


#' get_campaign_cc
#' returns campaign ID and status for each cc
#'
#' @return data.table with results
#'
#' @author Erico Santos
#'
get_campaign_cc <- function() {
    campaigns <- getDataTableMS("select id, name, status
                                , CASE WHEN user_location LIKE '%,%' THEN SUBSTRING_INDEX(user_location, ',', 1) END region
                                , CASE WHEN user_location NOT LIKE '%,%' THEN user_location END cc
                                from mrktctl.AccountOneBid_Campaign")
    cc <- getDataTableMS("SELECT upper(country_code) cc, sales_region region from account_1.country_region")
    cc_region <- merge(cc,campaigns[!is.na(cc),.(cc,id,name,status)],by="cc",all.x = T)
    cc_campaign <- merge(cc_region, campaigns[!is.na(region),.(region,id_reg=id,status_reg=status)]
                         ,by="region",all.x = T)[, list(cc,campaign_id=ifelse(is.na(id),id_reg,id)
                                                        ,status=ifelse(is.na(status),status_reg,status))]
    cc_campaign <- merge(cc_campaign, getDataTableMS("select id as campaign_id, name as campaign_name
                                                     from mrktctl.AccountFourBid_Campaign"), by = "campaign_id")
    return(cc_campaign)
}
