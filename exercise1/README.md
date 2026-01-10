# Exercise 1 – Divvy Trips Analysis

This exercise analyzes historical bike-sharing data from the Divvy system in Chicago.
The objective is to explore the structure of the data, identify potential analyses, and
develop basic processing and aggregation scripts.

**Note:** The raw datasets used in this exercise are not included in the repository due
to their large size. They are automatically downloaded from a public URL when executing downloader.py.

<br><br>

# Answers to the proposed questions
## Look at the data you downloaded. <br> a) What kind of data is? The data downloaded belongs to…
The downloaded data corresponds to a Chicago's bike-sharing system for public transportation and the associated mobile app (Divvy, Chicago).Each dataset represents individual bike trips recorded during a specific quarter. Each row corresponds to a single trip and includes information such as the start and end timestamps, trip duration, origin and destination stations, user type, and, when available, basic demographic attributes (gender and birth year).

Therefore, this is structured, historical, transactional data, generated automatically by the bike-sharing system and its associated digital platforms. The data reflects real user behavior over time and is primarily intended for analytical and reporting purposes, rather than for real-time operational use.

<br>

## b) Can you think of analyses that can be made with this?
This dataset enables multiple types of analysis related to urban mobility, user behavior, and system usage.

From a ***temporal perspective***, it is possible to analyze demand patterns at different granularities (hourly, daily, weekly, or seasonal), identify peak usage periods, and observe how bike usage evolves across quarters and years. This can reveal seasonality effects and long-term trends in the adoption of the service.

From a ***spatial perspective***, the origin and destination station fields allow the study of movement flows within the city. This makes it possible to identify highly used stations, common routes, and areas with higher or lower demand, which is useful for understanding mobility patterns and for operational decisions such as bike redistribution.

The data also supports ***user segmentation analysis***, particularly by comparing subscribers and casual customers. Differences in trip duration, frequency, and usage patterns between these groups can be explored, providing insights into how different types of users interact with the system.

For trips where demographic information is available, ***basic demographic analyses*** can be performed, such as age distribution or gender usage patterns, although this information is limited to registered users.

Finally, the dataset can be used for ***service quality and anomaly detection***, such as identifying unusually long or short trips, detecting inconsistent records, and analyzing system load during peak periods.

<br>

### i. Looking at the fields of the data one can infer…
By analyzing the fields present in the dataset, several conclusions can be inferred about user behavior and system usage.

The presence of start and end timestamps (start_time, end_time) allows the extraction of temporal features such as hour of the day, day of the week, or season of the year. From this, it can be inferred when the bike-sharing system is most intensively used and how demand varies over time.

The trip duration field (tripduration) provides insight into the typical length of trips, enabling the identification of common usage patterns as well as abnormal or anomalous trips that may correspond to system errors, misuse, or exceptional situations.

Fields related to origin and destination stations (from_station_id, from_station_name, to_station_id, to_station_name) make it possible to infer spatial mobility patterns within the city. These fields allow the analysis of frequently used stations, common routes, and imbalances in bike distribution across different areas.

The user type field (usertype) enables a clear distinction between registered subscribers and casual customers. From this, one can infer differences in usage behavior, such as trip frequency, duration, and preferred travel times, between these two groups.

Finally, the presence of demographic fields (gender, birthyear), although partially incomplete, allows limited demographic inference for registered users, such as age distribution and gender-based usage patterns. The absence of these values for some trips also suggests that not all users are required to provide personal information, which reflects differences in the registration process between user types.

<br>

## c) Is the data normalized or denormalized?
The dataset is denormalized.

Each row represents a complete bike trip and includes, in a single record, information related to the trip itself (timestamps and duration), the origin and destination stations, and user attributes. As a consequence, descriptive information such as station names or user characteristics is repeated across many rows.

In a normalized design, this information would be separated into multiple related tables (for example, trips, stations, and users) to reduce redundancy. However, in this case, the data is intentionally stored in a denormalized format, which is common for analytical and historical datasets, as it simplifies data consumption and analysis by avoiding the need for complex joins, at the expense of increased data redundancy.

<br>

## d) It’s needed any processing before we use it?
Yes, several processing steps are required before the data can be reliably used for analysis.

First, the data needs basic type conversion. Date and time fields must be correctly interpreted as timestamps, and numerical fields such as trip duration must be checked to ensure they contain valid values. Without this, time-based analyses would be inaccurate.

Second, the datasets from different quarters need to be made consistent. Since the data comes from multiple files covering different time periods, there may be small differences in column names or formats. These differences must be unified so that all files can be analyzed together.

The data also requires basic cleaning. This includes identifying and handling incorrect records, such as trips with unrealistically short or long durations or missing essential information.

In addition, missing values, especially in demographic fields, need to be taken into account. These values are expected for some users and should be handled carefully depending on the analysis, rather than simply removed.

Finally, it is often useful to create additional fields from the existing data, such as extracting the year or month from the timestamps, in order to facilitate aggregation and trend analysis.  

<br>

## e) Are there null values? In which fields? Measure how many and guess a reason
Yes, the dataset contains null values, mainly concentrated in demographic-related fields, and their presence has been measured programmatically using a dedicated processing script.

Across most quarterly files, missing values appear primarily in the fields related to gender and birth year. For example, in the 2018 Q4 dataset, approximately 7.5% of the records have missing values for gender and 7.2% for birthyear. In 2019 Q1, these percentages decrease slightly to around 5.4% and 4.9%, respectively. However, in later quarters such as 2019 Q2 and 2019 Q3, the proportion of missing demographic values increases significantly, reaching values above 16–17%.

These missing values are not uniformly distributed across all columns. Core trip-related fields such as timestamps, trip duration, and station identifiers are largely complete. An exception is observed in the 2020 Q1 dataset, where a negligible number of missing values (one record) appears in some end-station fields, likely due to isolated data collection or ingestion issues.

The primary reason for the missing demographic values is related to user registration differences. Demographic information is only collected for registered subscribers, while casual users are not required to provide personal details such as gender or birth year. Therefore, the absence of these values is expected and reflects the structure of the service rather than a data quality problem.

In conclusion, the null values present in the dataset are structural and explainable, mainly affecting optional demographic fields, and should be handled according to the type of analysis being performed. Demographic analyses should be restricted to records with available information, whereas most other analyses can safely ignore these missing values.

<br><br>

## Get the mean trip time for each quarter. Track how it evolves over time.
To compute the mean trip time per quarter, consider each CSV file corresponding to a specific quarter as an independent dataset. A processing script (processor.py) was developed to read all CSV files from the downloads directory, identify the appropriate trip duration column for each file, and calculate the average trip duration.

The results were aggregated by quarter and stored in the processed directory in two output files: one containing the mean trip duration per file and another summarizing the mean trip duration per quarter.

The analysis shows a clear seasonal pattern. Mean trip duration is lower during winter quarters (such as 2019 Q1, approximately 7.9 minutes) and increases during spring and summer, reaching its highest value in 2019 Q3 (approximately 9.0 minutes). This behavior is consistent with increased recreational usage and more favorable weather conditions during warmer months. In the following quarter (2019 Q4), the average trip duration decreases again.

For the 2020 Q1 dataset, the mean trip duration could not be computed due to differences in the dataset schema and the absence of a compatible duration column, which highlights the importance of schema validation when working with multi-period data.

<br><br>

## Propose and develop any extra analysis you consider
As an additional analysis, several exploratory studies were performed using the existing columns of the dataset in order to better understand user behavior and system usage patterns.

First, the number of trips was analyzed by hour of the day and user type. This analysis makes it possible to identify daily usage patterns and compare the behavior of registered users and casual users. The results show clear peak hours during the morning and afternoon, which are more pronounced for subscribers, suggesting a strong relationship with commuting patterns. Casual users, on the other hand, present a flatter distribution throughout the day. The results were stored in a CSV file and visualized using a line chart.

Second, the most frequently used start stations were identified by counting the number of trips originating from each station. This analysis highlights the most active areas of the city and potential mobility hubs, such as central or highly connected locations. The top ten start stations were exported to a CSV file and represented graphically using a bar chart.

Finally, the average trip duration by user type was computed. This analysis reveals differences in how different user groups use the bike-sharing system, showing that casual users tend to have longer average trips than subscribers. An additional breakdown by quarter was also generated to observe how these differences evolve over time.

All results and visualizations were saved in the processed directory, following the same structure used in previous steps of the exercise.

<br><br>

## Think of the need of delivery the data. How you will do it?
Once the data has been processed and analyzed, it is necessary to consider how the results should be delivered to different types of stakeholders.

For technical users (such as data engineers or data analysts), the most appropriate delivery format is through structured files, such as CSV files, stored in a dedicated output directory. In this exercise, all computed results are saved in the processed folder, including aggregated metrics (mean trip time by quarter, duration by user type) and intermediate datasets used for further analysis. This approach allows reproducibility, easy integration with other tools, and the possibility of reusing the processed data in future pipelines.

For less technical users or decision-makers, raw CSV files may not be the most convenient format. In these cases, visual outputs provide a clearer and more accessible way to communicate insights. For this reason, key analyses were also delivered as charts and plots, such as:

- Line charts showing the number of trips by hour of day and user type.
- Bar charts highlighting the most frequently used start stations.

These visualizations can be shared as image files or embedded into reports, presentations, or dashboards, making trends and patterns easier to interpret without requiring technical knowledge.

In a real-world scenario, this delivery could be extended further by publishing the processed data and visualizations in a business intelligence tool (for example, Google Data Studio or Power BI), or by exposing the processed datasets through a shared storage system or an API.

In summary, an effective data delivery strategy should adapt to the audience: providing clean, structured datasets for technical users, and clear visual summaries for non-technical stakeholders, ensuring that the insights generated from the data can be easily understood and effectively used.


