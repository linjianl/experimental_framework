
#' build_preStat_test function: 
#' pre experiment stationarity testing for test and control groups for shiny dashboard experiment tab 
#' 
#' @param 
#' ddChart 
#' 
#' @return DT table with KPSS statitics and ADF statistics 
#' 
#' @author Lin Jia
#' 
#' @import data.table
#' 
#' @export
#' 

build_preStat_test <- function(ddChart, negative_red = T){
    if (NA %in% unique(ddChart$group_num)) {
        ddChart <- merge(ddChart[grep("control", group)], ddChart[grep("test", group)], by = c("date"), allow.cartesian =  T)
    } else {
        ddChart <- merge(ddChart[grep("control", group)], ddChart[grep("test", group)], by = c("date", "group_num"), allow.cartesian =  T)
    }
    # only allow comparison with control group and matching control with test
    ddChart <- ddChart[,comparison := paste0(logic.x," X ",logic.y)]
    
    statsTs <- ddChart[, list(RMSE = round(sqrt(mean((value.y-value.x)^2)),3),
                              KPSS_statistics = round(kpss.test(value.y/value.x, null = "Level")$p.value, 3),
                              ADF_statistics = round(adf.test(value.y/value.x, alternative = c("stationary"))$p.value, 3)), 
                       by = comparison]
    setorder(statsTs, ADF_statistics, -KPSS_statistics, RMSE)
    
    ddTable <- DT::datatable(as.data.frame(statsTs), options = list(pageLength=15)) %>%
        formatStyle('ADF_statistics', color = styleInterval(0.05, c("olive","red"))) %>%
        formatStyle('KPSS_statistics', color = styleInterval(0.05, c("red", "olive"))) 
    # if (negative_red)
    #     ddTable <- ddTable %>% formatStyle('mean_diff', color = styleInterval(0, c("red","olive")))
    # else
    #     ddTable <- ddTable %>% formatStyle('mean_diff', color = styleInterval(0, c("olive","red")))
    return(ddTable)
}

#' build_tests_dt function: 
#' t-test statistics for test and control groups for shiny dashboard experiment tab 
#' 
#' @param 
#' ddChart 
#' 
#' @return DT table with Confidence Internval, p-value, t-statistic, absolute difference, and relative difference
#' 
#' @author Lin Jia
#' 
#' @import data.table
#' 
#' @export
#' 

build_tests_dt <- function(ddChart, negative_red = T, dimension = NULL) {
    if (NA %in% unique(ddChart$group_num)) {
        diffs <- merge(ddChart[grep("control", test_control_group)], 
                       ddChart[grep("test", test_control_group)], by = c("date",dimension), allow.cartesian =  T)
    } else {
        diffs <- merge(ddChart[grep("control", test_control_group)], 
                       ddChart[grep("test", test_control_group)], by = c("date", "group_num", dimension), allow.cartesian =  T)
    }
    
    diffs <- diffs[, `:=` (comparison = paste(sub("^([^-]*-[^-]*).*", "\\1", group.x), "X", sub("^([^-]*-[^-]*).*", "\\1", group.y)),
                           logic = paste(logic.x, "X", logic.y))]
    #diffs[, num := .N, by = comparison]
    ttests <- diffs[,list(t_stat = ifelse(length(value.x) == 1, -99, round(t.test(value.y,value.x)$statistic,4))
                          ,CI_from = ifelse(length(value.x) == 1, -99, round(t.test(value.y,value.x)$conf.int[1],2))
                          ,CI_to = ifelse(length(value.x) == 1, -99, round(t.test(value.y,value.x)$conf.int[2],2))
                          ,p_value = ifelse(length(value.x) == 1, -99, round(t.test(value.y,value.x)$p.value,4))
                          ,relative_diff = round(mean(value.x/value.y) - 1, 3) * 100
                          ,mean_abs_diff = round(mean(value.x - value.y),3))
                    ,by= c("comparison", "logic", dimension)]
    setorder(ttests, logic, comparison)
    ddTable <- DT::datatable(as.data.frame(ttests[,list(comparison,relative_diff,mean_abs_diff,t_stat
                                                        ,CI=paste0("95% CI [",CI_from,", ",CI_to,"]")
                                                        ,p_value)])
                             , colnames = c("rel_diff (in %)" = "relative_diff")
                             #, options = list(dom = 't')
    ) %>%
        formatStyle('p_value', color = styleInterval(0.05, c("olive","red")))
    if (negative_red) {
        ddTable <- ddTable %>% formatStyle('mean_abs_diff', color = styleInterval(0, c("red","olive")))
    } else {
        ddTable <- ddTable %>% formatStyle('mean_abs_diff', color = styleInterval(0, c("olive","red")))
    }
    return(ddTable)
}

#' build_CI function: 
#' use causal impact package for test and control groups for shiny dashboard Bayesian experiment tab 
#' 
#' @param 
#' ddChart 
#' 
#' @return causal impact object
#' 
#' @author Lin Jia
#' 
#' @import data.table
#' 
#' @export
#'        

build_CI <- function(ddChart){
    pre.period <- as.Date(c(min(ddChart[grep("pre", group),date]), max(ddChart[grep("pre", group),date])))
    post.period <- as.Date(c(min(ddChart[grep("post", group),date]), max(ddChart[grep("post", group),date])))
    exp_data <- merge(ddChart[grep("test", test_control_group), list(date = as.Date(date), test = value)],
                      ddChart[grep("control", test_control_group), list(date = as.Date(date), control = value)])
    tseries_exp_data <- zoo(cbind(exp_data$test, exp_data$control), exp_data$date)
    impact <- CausalImpact(tseries_exp_data, pre.period, post.period,
                           model.args = list(nseasons = 7, season.duration = 1,
                                             niter = 5000))
    return(impact)
}

#' build_did_dt
#' difference-in-differences for test and control groups for shiny dashboard experiment tab 
#' 
#' @param 
#' ddChart 
#' 
#' @return DT table 
#' 
#' @author Lin Jia
#' 
#' @import data.table
#' 
#' @export
#'  

build_did_dt <- function(ddChart){
    if (NA %in% unique(ddChart$group_num)) {
        diffs <- merge(ddChart[grep("control", test_control_group)], 
                       ddChart[grep("test", test_control_group)], by = c("date"), allow.cartesian =  T)
    } else {
        diffs <- merge(ddChart[grep("control", test_control_group)], 
                       ddChart[grep("test", test_control_group)], by = c("date", "group_num"), allow.cartesian =  T)
    }
    # diffs <- merge(ddChart,ddChart, by="date",allow.cartesian = T)
    # diffs <- diffs[group.x < group.y][test_control_group.x == "control"]
    diffs <- diffs[,comparison:=paste0(logic.x," X ",logic.y)]
    did.stats.total <- data.table()
    for (i_comp in unique(diffs$comparison)){
        diff_group <- diffs[comparison == i_comp]
        diff_group_1 <- diff_group[, list(date, logic.x, group.x, value.x, treat = 0)]
        names(diff_group_1) <- gsub("\\..*", "", names(diff_group_1))
        diff_group_2 <- diff_group[, list(date, logic.y, group.y, value.y, treat = 1)]
        names(diff_group_2) <- gsub("\\..*", "", names(diff_group_2))
        temp <- rbind(diff_group_1, diff_group_2)
        temp[, `:=` (after = ifelse(grepl("pre" , group), 0, 1), 
                     comparison = i_comp)]
        did.stats <- round(summary(lm(value ~ treat * after , data = temp))$coefficients[4,],3)
        did.stats <- as.data.table(cbind(comparison_group = i_comp, t(did.stats)))
        did.stats.total <- rbind(did.stats, did.stats.total)
    }
    ddTable <- DT::datatable(did.stats.total,  options = list(dom = 't')) %>%
        formatStyle('Pr(>|t|)', color = styleInterval(0.05, c("olive","red"))) 
    return(ddTable)
}

#' build_reldiff_descriptives
#' provide relative difference descriptive statistics for test and control groups for shiny dashboard experiment tab 
#' 
#' @param 
#' ddChart 
#' 
#' @return DT table 
#' 
#' @author Lin Jia
#' 
#' @import data.table
#' 
#' @export
#'  

build_reldiff_descriptives <- function (ddChart, negative_red = T) {
    if (NA %in% unique(ddChart$group_num)) {
        ddChart <- merge(ddChart[grep("control", test_control_group)], 
                         ddChart[grep("test", test_control_group)], by = c("date"), allow.cartesian =  T)
    } else {
        ddChart <- merge(ddChart[grep("control", test_control_group)], 
                         ddChart[grep("test", test_control_group)], by = c("date", "group_num"), allow.cartesian =  T)
    }
    
    ddChart[, `:=` (value = value.x / value.y, 
                    comparison = paste(sub("^([^-]*-[^-]*).*", "\\1", group.x), "X", sub("^([^-]*-[^-]*).*", "\\1", group.y)),
                    logic = paste(logic.x, "X", logic.y))]
    diffs <- ddChart[,list(rel_diff = round(mean(value),3) * 100),by=list(logic, comparison)]
    setorder(diffs, logic, comparison)
    ddTable <- DT::datatable(as.data.frame(diffs[,list(comparison,rel_diff)]),
                             colnames =  c('average relative ratio test/control (in %)' = 'rel_diff')
                             , options = list(dom = 't'))
    return(ddTable)
}

#' build_descriptives_dt 
#' build descriptives for various metrics 
#' 
#' @param 
#' - ddChart
#' 
#' @return DT table with results
#' 
#' @author Lin Jia
#' 
#' @import data.table
#' 
#' @export
#' 

build_descriptives_dt <- function (ddChart, negative_red = T) {
    if (NA %in% unique(ddChart$group_num)) {
        diffs <- merge(ddChart[grep("control", test_control_group)], 
                       ddChart[grep("test", test_control_group)], by = c("date"), allow.cartesian =  T)
    } else {
        diffs <- merge(ddChart[grep("control", test_control_group)], 
                       ddChart[grep("test", test_control_group)], by = c("date", "group_num"), allow.cartesian =  T)
    }
    diffs <- diffs[, `:=` (comparison = paste(sub("^([^-]*-[^-]*).*", "\\1", group.x), "X", sub("^([^-]*-[^-]*).*", "\\1", group.y)),
                           logic = paste(logic.x, "X", logic.y))]
    diffs <- diffs[, list(absolute_difference = round(mean(value.y) - mean(value.x), 3) * 100)
                   , by = list(comparison, logic)]
    setorder(diffs, logic, comparison)
    
    ddTable <- DT::datatable(as.data.frame(diffs[,list(comparison,absolute_difference)])
                             , colnames =  c('absolute difference in (%)' = 'absolute_difference')
                             , options = list(dom = 't'))
    # if (negative_red)
    #     ddTable <- ddTable %>% formatStyle('absolute_difference', color = styleInterval(0, c("red","olive")))
    # else
    #     ddTable <- ddTable %>% formatStyle('absolute_difference', color = styleInterval(0, c("olive","red")))
    return(ddTable)
}

