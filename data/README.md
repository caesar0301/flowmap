# 基本数据集

### 杭州数据集
* TotalLogs：807067217
* LON: 120.0,120.49997
* LAT: 30.000349,30.499533
* TotalUsers: 1401311
* TotalBS: 807067217
* SampleLogs: 44943776
* SampledUsers: 167852
* SampledBS: 22823

### Senegal数据集

#### ARR Level
* TotalLogs: 561025219
* TotalUsers: 146352
* TotalARRs: 123

#### Site Level
* TotalLogs: 1204451385
* TotalUsers: 320000
* TotalSites: 1609

# 数据质量控制
(Hangzhou)
Sample users whose data meet principles:
1. having more than 75% days of interactions in the given period
2. having unique locations larger than 2
3. having more than 5 observations in observed days on average
4. having more than 6 semi-hour intervals in observed days on average
5. having `staying home` states
Data improvement:
1. add `staying home` states for each user at 3:00 AM

(Senegal)
* Remove users with less than 7 days in a week
* Remove users with less than 6 unique time intervals (3 hours) in a day; ~30% user-days left
* Remove users with less than 5 observations in a day; more than 50% user-days left
* Merge two location with more than 2 transitions within the same interval in a day
* Merge consecutive records at the same location within the same interval into one record (XXX)
