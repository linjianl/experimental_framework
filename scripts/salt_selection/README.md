# Meta Experiments Framework

This is the smart salt code for pre-selecting the smart salt per account when running an experiment
and bootstrapping code to etablish the empirical distribution for the corresponding metrics

Before running the smart salt, first go to the folder 
```python
cd ~/git_tree/metasearch/projects/meta_experiment_framework/scripts/salt_selection
```

### Smart Salt
specify the following parameters when you run smart salt

**account**: trivago/tripadvisor/gha/kayak<br/>
**exp_weights**: only needed for tripadvisor, as it is dynamic. For other accounts, it is specified implicitly.<br/>
**run_date**: default to today, usually do not need to specify<br/>
**exp_groups**: what the group size is for each group, if you want a group split of 4, 4, 2 then write as ***--exp_groups 4 4 2***, the total number of groups should be equal to the maximum number of groups allowed. Note that the group size is relative to the total number of groups, a 10% group is GHA, should be written as 1, but a 10% group in trivago, should be written as 2<br/>
**control**: which group is control. If you want group 0 and 1 to be control group, then write as ***--control 0 1***, note that the numbering starts from 0, namely the first group is 0, and the second group is 1.<br/>
**test**:  which group is test. note that this allows for multiple test groups, so if you want to run multiple test groups, say group 1, 2, 3 as test groups then write as ***--test 2 3***, note that the numbering starts from 0, namely the first group is 0, and the second group is 1.<br/>
**pos**: which point of sale (campaign/distribution/locale) do you want to test? Default is all, if you only want to run on some pos, specify by separated by spaces between each pos, e.g. if you want to test for United States, Japan and Great Britain, write as ***--pos US JP GB***. Note that this is only available for tripadvisor and trivago for now.<br/>
**ntrials**: number of randomization needed, default to 2000, usually do not need to specify explicitly.<br/>
**period**: number of days of data to look back before the day starts, default to 42, do not need to specify explicitly.<br/>

Below gives example for how to run the code. If you want to run an experiment in trivago, with a 10%-10%-80% split with the first 10% as test, second 10% as test, third 80% as control in US and UK. Then what the command should be

```python
/opt/spark/current20/bin/spark-submit smart_salt.py  --exp_groups 2 2 16  --control 2  --test 0  1  --account trivago --pos US,UK 2> /dev/null
```

If you want to run an experiment in tripadvisor, with a 50%-50% split with the first 50% as test, second 50% as control and you the total number of groups is 2. Then what the command should be _(notes that the exp_weights need to be specified)_

```python
/opt/spark/current20/bin/spark-submit smart_salt.py  --exp_groups 1 1  --control 1  --test 0   --exp_weights 1 1 --account tripadvisor 2> /dev/null
```
If you want to run an experiment in GHA, with a 50%-50% split with the first 50% as test, second 50% as control. Then what the command should be

```python
/opt/spark/current20/bin/spark-submit smart_salt.py  --exp_groups 5 5  --control 1  --test 0  --account gha 2> /dev/null
```
