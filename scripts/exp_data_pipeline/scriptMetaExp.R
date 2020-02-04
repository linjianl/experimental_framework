#' script for 
#' 1. updating experiments evaluation table, 
#' 2. getting pre and post experiements data
#'
#'for trivago and kayak and tripadvisor
#' @author Lin Jia
#'

options(warn = -1)

suppressWarnings(suppressMessages({
    library(RMySQL)
    library(data.table)
    library(digest)
}))

# source relevent basic functions 
source("/usr/local/git_tree/metasearch/R/utils/dataManipulation.R")
source("/usr/local/git_tree/metasearch/projects/meta_experiment_framework/exp_lib/R/expFuncs.R")

proc_name = "scriptExpMeta.R"
log_table = NULL # "spmeta.logging_pq"
log_file = sprintf("/shared/ppc/sp_team/logs_cron/projects/expScript_%s.log",format(Sys.Date(), "%Y%m%d"))

print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
               msg = sprintf("##-------------------> Starting Meta Experiment script")))
## TODO:: write every account in a loop 

#### update trivago experiments_evaluation_overview table 
#### update experiments_evaluation_overview table 
print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
               msg = sprintf("##-------------------> Starting Trivago Experiment part")))

start_date  <- getDataTableMS("select max(end_date) date from ppc_trivago.experiments_evaluation_overview")$date
end_date <- format(Sys.Date(), "%F")
if (is.na(start_date)) {
    start_date <- min(GetExpGroupTrivago(end_date)[experiment_testing == 'True', start_date])
}

if (end_date > start_date) {
    tryCatch({
        exp_overview <- GetExpGroupTrivago(end_date)[experiment_testing == 'True']
        if (nrow(exp_overview) != 0) {
            exp_ids       <- getDataTableMS("select distinct experiment_id from ppc_trivago.experiments_evaluation_overview")$experiment_id
            latest_exp_id <- unique(exp_overview$experiment_id)
            
            # if the latest exp id is already in the overview table, meaning the experiment has already started, 
            # then only update the experiment last date 
            if (latest_exp_id %in% exp_ids) {
                source("/usr/local/git_tree/main/lang/R/lib/getDBGrants.r")
                grants <- getDBGrants("ppc_meta", "rw")
                con <- dbConnect(RMySQL::MySQL()
                                 , user=grants$username
                                 , password=grants$password
                                 , host = grants$host
                                 , dbname = grants$database)
                
                for (id in latest_exp_id) {
                    print(dbSendQuery(con, sprintf("update ppc_trivago.experiments_evaluation_overview set end_date = '%s' where experiment_id = '%s'",
                                                   unique(exp_overview[experiment_id == id, end_date]), id)))

                    print(logEvent(proc_name, table_name = log_table, file_name = log_file
                                   ,msg = sprintf("update ppc_trivago.experiments_evaluation_overview set end_date = '%s' where experiment_id = '%s'",
                                                  unique(exp_overview[experiment_id == id, end_date]), id)))
                }
                dbDisconnect(con)
                
            } else {
                # if there is new experiment, put the new experiments information into the table 
                response   <- putDataTableMS(df = exp_overview, table_name = "experiments_evaluation_overview", db_name = "ppc_trivago")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = response))
                
            }
        }
    }, error = function(e) {
        print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                       msg = sprintf("Error in updating ppc_trivago.experiments_evaluation_overview: %s", e[1])))
    })
}
#### TRIVAGO:: performance tables for pre and post experiments data 
startDate = format(as.Date(getDataTableMS("select max(date) date
                                          from ppc_trivago.trivago_summ_exp")$date)+1, "%F")

#TODO: Switch to DataSources, waiting for SPDATA ticket SPD-1972 https://jira.booking.com/jira/browse/SPD-1972
#endDate <- format(Sys.Date(), "%F")
endDate <- format(as.Date(getDataTableMS("select max(period) date from office.CronWorkflowJobQueue
                                         WHERE lower(jobtype) like 'sp/trivago/stats/store_stats_performance_hive_%'
                                         and job_status='done'", db="office")$date), "%F")

print(logEvent(proc_name, table_name = log_table, file_name = log_file
               , msg = sprintf("last date processed for trivago performance data: %s",endDate)))

expOverview <- getDataTableMS("select * from ppc_trivago.experiments_evaluation_overview")
expLogic <-unique(expOverview[,list(experiment_id, salt,start_date, end_date)])

for (expID in expLogic$experiment_id){
    tryCatch({
        # generate correct startDate and endDate for pre and post experiments 
        startDateExp <- format(as.Date(getDataTableMS(sprintf("select max(date) date from ppc_trivago.trivago_summ_exp
                                                          where experiment_id = '%s' and salt = '%s'", expID, expLogic[experiment_id == expID, salt]))$date) + 1, "%F")
        if(is.na(startDateExp)){startDateExp <- expLogic[experiment_id == expID, start_date]}
        
        endDateExp <- expLogic[experiment_id == expID, end_date]
        
        # when endDate is later than startDate, it means there is need to get the data for the experiments,
        # get the minimum between endDateExp and the latest date for getting data 
        if(endDateExp > startDateExp){
            endDateExp <- min(endDateExp, format(as.Date(endDate) + 1, "%F"))
        }
        
        startDatePreExp <- format(as.Date(getDataTableMS(sprintf("select max(date) date from ppc_trivago.trivago_summ_pre_exp
                                                             where experiment_id = ('%s') and salt = ('%s') ", expID, expLogic[experiment_id == expID, salt]))$date) + 1, "%F")
        # check for 42 days before the experiments starts 
        if(is.na(startDatePreExp)) {startDatePreExp <- format(as.Date(expLogic[experiment_id == expID, start_date])- 7*6, "%F")}
        
        endDatePreExp <- expLogic[experiment_id == expID, start_date]
        
        # if pre-experiment data is not completely filled, fill with pre-experiment data
        if (startDatePreExp < endDatePreExp){
            tryCatch({
                perfPreExpDetail<- getPerfExpTrivago(baseDate = startDatePreExp, 
                                                     endDate = endDatePreExp, 
                                                     experiment_id = expID, 
                                                     salt = expLogic[experiment_id == expID, salt])
            }, error = function(e) {
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                               msg = sprintf("Error in getPerfExpTrivago in getting perfPreExpDetail: %s", e[1])))
            })
            if (!is.null(perfPreExpDetail)) {
                response <- putDataTableMS(df = perfPreExpDetail, table_name = "trivago_summ_pre_exp", db_name = "ppc_trivago")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = paste(response, sprintf("for experiment id '%s'", expID))))
            }
        }
        
        if (endDateExp > startDateExp) {
            tryCatch({
                perfExpDetail<-  getPerfExpTrivago(baseDate = startDateExp, 
                                                   endDate = endDateExp, 
                                                   experiment_id = expID, 
                                                   salt = expLogic[experiment_id == expID, salt])
            }, error = function(e) {
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                               msg = sprintf("Error in getPerfExpTrivago for perfExpDetail: %s", e[1])))
            })
            if (!is.null(perfExpDetail)) {
                response <- putDataTableMS(df = perfExpDetail, table_name = "trivago_summ_exp", db_name = "ppc_trivago")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = paste(response, sprintf("for experiment id '%s'", expID))))
            }
        }
    }, error = function(e) {
        print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                       msg = sprintf("Error in getting experiment data for trivago: %s for experiment id %s", e[1], expID)))
    })
}

######### KAYAK ####################################################################################
#### update kayak experiments_overview and experiments_evaluation_overview table 
print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
               msg = sprintf("##-------------------> Starting Kayak Experiment part")))

start_date = format(as.Date(getDataTableMS("select max(date) date from ppc_kayak.experiments_overview")$date)+1, "%F")

end_date = format(Sys.Date(),"%F")

if (end_date > start_date) {
    tryCatch({  
        exp_groups <- GetExpGroupKayak(end_date)[experiment_testing == 'True']
        response   <- putDataTableMS(df = exp_groups, table_name = "experiments_overview", db_name = "ppc_kayak")
        print(logEvent(proc_name, table_name = log_table, file_name = log_file,msg = response))
    }, error = function(e) {
        print(logEvent(proc_name, table_name = log_table, file_name = log_file
                       , msg = sprintf("Error in Kayak experiments_overview history: %s", e[1])))
    })
}

## update experiments evaluation table 
experimentIDOverview   <- getDataTableMS("select distinct experiment_id from ppc_kayak.experiments_overview 
                                         where experiment_testing = 'True'")$experiment_id
experimentIDEvaluation <- getDataTableMS("select distinct experiment_id from ppc_kayak.experiments_evaluation_overview")$experiment_id

diffExpID = setdiff(experimentIDOverview, experimentIDEvaluation)

tryCatch({
    if (length(diffExpID) != 0){
        for (expID in diffExpID){
            experimentOverview <- getDataTableMS(sprintf("select * from ppc_kayak.experiments_overview where experiment_id = '%s'", expID))
            expStartDate <- min(experimentOverview$date)
            expEndDate   <- max(experimentOverview$date)
            experimentOverview <- unique(experimentOverview[, list(grp_list = unlist(strsplit(grp_list, ","))),
                                                            by = list(placement, device, experiment_id, salt, logic, grp, grp_name, locale, test_control_group)])
            experimentOverview[, ':=' (start_date = expStartDate, end_date =  expEndDate, weight = 1)]
            response   <- putDataTableMS(df = experimentOverview, table_name = "experiments_evaluation_overview", db_name = "ppc_kayak")
            print(logEvent(proc_name, table_name = log_table, file_name = log_file,msg = response))
        }
    } else {
        # if there is no new experiment, then only update the latest experiment dates 
        for (expID in experimentIDOverview){
            experimentOverview <- getDataTableMS(sprintf("select experiment_id, max(date) end_date from ppc_kayak.experiments_overview
                                                 where experiment_id = '%s'", expID))
            
            experimentEvaluation <- getDataTableMS(sprintf("select distinct experiment_id, end_date from ppc_kayak.experiments_evaluation_overview
                                                   where experiment_id = '%s' group by experiment_id ", expID))
            if (experimentOverview$end_date >  experimentEvaluation$end_date) {
                source("/usr/local/git_tree/main/lang/R/lib/getDBGrants.r")
                grants <- getDBGrants("ppc_meta", "rw")
                con <- dbConnect(RMySQL::MySQL()
                                 , user=grants$username
                                 , password=grants$password
                                 , host = grants$host
                                 , dbname = grants$database)
                
                print(dbSendQuery(con, sprintf("update ppc_kayak.experiments_evaluation_overview set end_date = '%s' where experiment_id = '%s'",
                                               experimentOverview[experiment_id == expID, end_date], expID)))
                dbDisconnect(con)
            }
        }
    }    
}, error = function(e) {
    print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                   msg = sprintf("Error in updating ppc_kayak.experiments_evaluation_overview: %s", e[1])))
})


#### KAYAK:: performance tables for pre and post experiments data 
# kayak flag:
check_flag_web           <- getDataTableMS("SELECT max(date) last_done FROM ppc_kayak.KayakStats_CoreWeb_Multiplier_2018;")
check_flag_mobile        <- getDataTableMS("SELECT max(date) last_done FROM ppc_kayak.KayakStats_CoreMobile_Multiplier_2018;")
check_flag_web_Whisky    <- getDataTableMS("SELECT max(date) last_done FROM ppc_kayak.KayakStats_WhiskyWeb_2018;")
check_flag_mobile_Whisky <- getDataTableMS("SELECT max(date) last_done FROM ppc_kayak.KayakStats_WhiskyMobile_2018;")
check_flag_Share         <- getDataTableMS("select max(date) last_done FROM ppc_kayak.KayakShare_CoreWhisky")

flag <- rbind(check_flag_web,check_flag_web,check_flag_web_Whisky,check_flag_mobile_Whisky)

## get the end date based on fully available data 
if(all(flag$last_done == check_flag_web$last_done)){
    endDate = format(as.Date(check_flag_web$last_done) + 1, "%F")
    print(logEvent(proc_name, table_name = log_table, file_name = log_file
                   , msg = sprintf("last date processed for kayak performance data: %s",check_flag_web$last_done)))
    
} else{
    endDate = format(min(as.Date(flag$last_done)), "%F")
    print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = "Some tables are not filled"))
    print(logEvent(proc_name, table_name = log_table, file_name = log_file
                   , msg = sprintf("last fully available date processed for kayak performance data: %s",endDate)))
}


expOverview <- getDataTableMS("select * from ppc_kayak.experiments_evaluation_overview")
expLogic    <- unique(expOverview[,list(experiment_id, salt,start_date, end_date)])

for (expID in expLogic$experiment_id){
    tryCatch({
        # generate correct startDate and endDate for pre and post experiments 
        startDateExp <- format(as.Date(getDataTableMS(sprintf("select max(date) date from ppc_kayak.kayak_summ_exp
                                                          where experiment_id = '%s' and salt = '%s'", expID, expLogic[experiment_id == expID, salt]))$date) + 1, "%F")
        if(is.na(startDateExp)){startDateExp <- expLogic[experiment_id == expID, start_date]}
        
        endDateExp <- expLogic[experiment_id == expID, end_date]
        
        # if the endDate is bigger than the startDate, the data is not filled for this experiment until most date, 
        # fill with the minimum of latest available date and end_date of expeirment
        if(endDateExp > startDateExp){
            endDateExp <- format(min(as.Date(endDate), endDateExp), "%F")
        }
        
        startDatePreExp <- format(as.Date(getDataTableMS(sprintf("select max(date) date from ppc_kayak.kayak_summ_pre_exp
                                                             where experiment_id = ('%s') and salt = ('%s') ", expID, expLogic[experiment_id == expID, salt]))$date) + 1, "%F")
        
        if(is.na(startDatePreExp)) {startDatePreExp <- format(as.Date(expLogic[experiment_id == expID, start_date])- 7*6, "%F")}
        
        endDatePreExp <- expLogic[experiment_id == expID, start_date]
        
        # if pre-experiment data is not completely filled, fill with pre-experiment data
        if (startDatePreExp < endDatePreExp){
            tryCatch({
                perfPreExpDetail<- getPerfExpKayak(startDate = startDatePreExp, 
                                                   endDate = endDatePreExp, 
                                                   experiment_id = expID, 
                                                   salt = expLogic[experiment_id == expID, salt])
            }, error = function(e) {
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                               msg = sprintf("Error in getPerfExpKayak in getting perfPreExpDetail: %s", e[1])))
            })
            if (!is.null(perfPreExpDetail)) {
                response <- putDataTableMS(df = perfPreExpDetail, table_name = "kayak_summ_pre_exp", db_name = "ppc_kayak")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = paste(response, sprintf("for experiment id '%s'", expID))))
            }
        }
        if (endDateExp > startDateExp) {
            tryCatch({
                perfExpDetail<-  getPerfExpKayak(startDate = startDateExp, 
                                                 endDate = endDateExp, 
                                                 experiment_id = expID, 
                                                 salt = expLogic[experiment_id == expID, salt])
            }, error = function(e) {
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                               msg = sprintf("Error in getPerfExpKayak for perfExpDetail: %s", e[1])))
            })
            if (!is.null(perfExpDetail)) {
                response <- putDataTableMS(df = perfExpDetail, table_name = "kayak_summ_exp", db_name = "ppc_kayak")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = paste(response, sprintf("for experiment id '%s'", expID))))
            }
        }
    }, error = function(e) {
        print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                       msg = sprintf("Error in getting experiment data for kayak: %s for experiment id %s", e[1], expID)))
    })
    
}

####################################################################################################
## Tripadvisor 

#### update experiments_evaluation_overview table 
print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
               msg = sprintf("##-------------------> Starting Tripadvisor Experiment part")))

start_date  <- getDataTableMS("select max(end_date) date from ppc_trip.experiments_evaluation_overview")$date
if (is.na(start_date)) {
    start_date <- min(GetExpGroupTripadvisor(end_date)[experiment_testing == 'True', start_date])
}
end_date    <- format(Sys.Date(),"%F")

if (end_date > start_date) {
    tryCatch({
        exp_overview <- GetExpGroupTripadvisor(end_date)[experiment_testing == 'True']
        if (nrow(exp_overview) != 0) {
            exp_ids       <- getDataTableMS("select distinct experiment_id from ppc_trip.experiments_evaluation_overview")$experiment_id
            latest_exp_id <- unique(exp_overview$experiment_id)
                
            # if the latest exp id is already in the overview table, meaning the experiment has already started, 
            # then only update the experiment last date 
            if (latest_exp_id %in% exp_ids) {
                source("/usr/local/git_tree/main/lang/R/lib/getDBGrants.r")
                grants <- getDBGrants("ppc_meta", "rw")
                con <- dbConnect(RMySQL::MySQL()
                                 , user=grants$username
                                 , password=grants$password
                                 , host = grants$host
                                 , dbname = grants$database)
                
                for (id in latest_exp_id) {
                    print(dbSendQuery(con, sprintf("update ppc_trip.experiments_evaluation_overview set end_date = '%s' where experiment_id = '%s'",
                                                   unique(exp_overview[experiment_id == id, end_date]), id)))
                    
                    print(logEvent(proc_name, table_name = log_table, file_name = log_file
                                   ,msg = sprintf("update ppc_trip.experiments_evaluation_overview set end_date = '%s' where experiment_id = '%s'",
                                                  unique(exp_overview[experiment_id == id, end_date]), id)))
                }
                dbDisconnect(con)
                
            } else {
                # if there is new experiment, put the new experiments information into the table 
                response   <- putDataTableMS(df = exp_overview, table_name = "experiments_evaluation_overview", db_name = "ppc_trip")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = response))
                
            }
        }
    }, error = function(e) {
        print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                       msg = sprintf("Error in updating ppc_trip.experiments_evaluation_overview: %s", e[1])))
    })
}

### Tripadvisor experiment data ###################################################################

check_flag <- getDataTableMS("SELECT max(date(period)) last_done FROM office.CronWorkflowJobQueue
                              WHERE lower(jobtype) like 'sp/tripadvisor/costfetch/tripad%'
                              AND job_status = 'Done';", db="office")
endDate = format(as.Date(check_flag$last_done)+1, "%F")
print(logEvent(proc_name, table_name = log_table, file_name = log_file
               , msg = sprintf("last date processed for trip cost API: %s",check_flag$last_done)))

expOverview <- getDataTableMS("select * from ppc_trip.experiments_evaluation_overview")
expLogic    <- unique(expOverview[,list(experiment_id, salt,start_date, end_date)])

for (expID in expLogic$experiment_id){
    tryCatch({
        # generate correct startDate and endDate for pre and post experiments 
        startDateExp <- format(as.Date(getDataTableMS(sprintf("select max(date) date from ppc_trip.trip_summ_exp
                                                          where experiment_id = '%s' and salt = '%s'", expID, expLogic[experiment_id == expID, salt]))$date) + 1, "%F")
        if(is.na(startDateExp)){startDateExp <- expLogic[experiment_id == expID, start_date]}
        
        endDateExp <- expLogic[experiment_id == expID, end_date]
        
        # if the endDate is bigger than the startDate, the data is not filled for this experiment until most date, 
        # fill with the minimum of latest available date and end_date of expeirment
        
        if(endDateExp > startDateExp){
            endDateExp <- min(endDateExp, format(as.Date(endDate) + 1, "%F"))
        }
        
        startDatePreExp <- format(as.Date(getDataTableMS(sprintf("select max(date) date from ppc_trip.trip_summ_pre_exp
                                                             where experiment_id = ('%s') and salt = ('%s') ", expID, expLogic[experiment_id == expID, salt]))$date) + 1, "%F")
        
        if(is.na(startDatePreExp)) {startDatePreExp <- format(as.Date(expLogic[experiment_id == expID, start_date])- 7*6, "%F")}
        
        endDatePreExp <- expLogic[experiment_id == expID, start_date]
        
        # if pre-experiment data is not completely filled, fill with pre-experiment data
        if (startDatePreExp < endDatePreExp){
            tryCatch({
                perfPreExpDetail<- getPerfExpTripadvisor(startDate = startDatePreExp, 
                                                         endDate = endDatePreExp, 
                                                         experiment_id = expID, 
                                                         salt = expLogic[experiment_id == expID, salt])
            }, error = function(e) {
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                               msg = sprintf("Error in getPerfExpTripadvisor in getting perfPreExpDetail: %s", e[1])))
            })
            if (!is.null(perfPreExpDetail)) {
                response <- putDataTableMS(df = perfPreExpDetail, table_name = "trip_summ_pre_exp", db_name = "ppc_trip")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = paste(response, sprintf("for experiment id '%s'", expID))))
            }
        }
        if (endDateExp > startDateExp) {
            tryCatch({
                perfExpDetail<-  getPerfExpTripadvisor(startDate = startDateExp, 
                                                       endDate = endDateExp, 
                                                       experiment_id = expID, 
                                                       salt = expLogic[experiment_id == expID, salt])
            }, error = function(e) {
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                               msg = sprintf("Error in getPerfExpTripadvisor for perfExpDetail: %s", e[1])))
            })
            if (!is.null(perfExpDetail)) {
                response <- putDataTableMS(df = perfExpDetail, table_name = "trip_summ_exp", db_name = "ppc_trip")
                print(logEvent(proc_name, table_name = log_table, file_name = log_file, msg = paste(response, sprintf("for experiment id '%s'", expID))))
            }
        }
    }, error = function(e) {
        print(logEvent(proc_name, table_name = log_table, file_name = log_file, 
                       msg = sprintf("Error in getting experiment data for tripadvisor: %s for experiment id %s", e[1], expID)))
    })
}
