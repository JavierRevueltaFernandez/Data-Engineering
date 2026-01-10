# **1. Introduction**
This project focuses on the construction of a complete data engineering pipeline for the analysis of global economic indicators. The main goal is to take real-world data and transform it step by step into a format that can be easily analyzed and visualized, following a clear, reproducible, and structured process.

The economic domain was chosen both out of personal interest and because it naturally fits analytical data modeling. Economic indicators are usually reported over time and across countries, which makes them especially suitable for time-series analysis and comparative studies. In addition, this type of data is widely used in real analytical and business contexts, which makes it a good candidate for practicing data engineering concepts.

The datasets selected for this project focus on two key aspects of the economy: economic performance and labor market conditions. GDP per capita reflects the average level of economic output and living standards, while the unemployment rate provides insight into how the labor market evolves over time. Analyzing these two indicators together allows a more complete view of economic development without introducing unnecessary complexity.

By working with both indicators instead of just one, the project gains analytical richness while remaining within the scope of the course. The resulting pipeline covers the full lifecycle of the data, from raw data ingestion and processing to cloud storage, analytical querying, and final visualization.

<br><br>

# **2. Data sources and initial data understanding**
The datasets used in this project were obtained from the World Bank Open Data platform.
Rather than representing a single dataset, each downloaded ZIP file corresponds to a logical data package that includes both quantitative data and relevant metadata.
The selected datasets focus on macroeconomic indicators related to economic performance and labor market conditions, which makes them suitable for analytical processing and comparison across countries and over time.
<br><br>

## 2.1 Dataset structure and components
Each ZIP file contains three different types of CSV files, each serving a specific role within the data engineering pipeline.

**1. Main Datasets (facts):** <br><br>
    The main CSV files contain the numerical values of the selected indicators:
   - *Unemployment rate (% of total labor force):*
   This indicator measures the percentage of the labor force that is unemployed but actively seeking work. It provides insight into labor market conditions.

   - *GDP per capita (current US dollars):*
   This indicator represents the average economic output per person and is commonly used as an indicator for economic development and living standards.

These datasets are structured by country and year, and represent measurable economic metrics.
From a data engineering perspective, these files can be considered fact data, as they store quantitative values that are later analyzed.

<br>

**2. Country metadata (country dimension):**

Each ZIP file also includes a Metadata_Country CSV file.
These files provide descriptive information about each country, such as country name, region, income group, lending category, and ISO codes.

This information does not represent measurements, but descriptive attributes, which makes it suitable to be modeled as a country dimension and reused across multiple indicators.

<br>

**3. Indicator metadata (indicator dimension):**

Finally, the Metadata_Indicator CSV files describe each economic indicator, including its name, definition, source, and measurement notes.

This metadata adds semantic context to the numerical values and supports data governance, documentation, and interpretability.
For this reason, it can be modeled as an indicator dimension.

<br>

## 2.2 Relationship between the datasets
Although six CSV files are used in total, they are not independent datasets and not all of them play the same role. 
All files are related through common keys and represent different perspectives of the same economic domain.
The main datasets share a common structure based on country and year, which allows them to be integrated consistently. Both GDP per capita and unemployment rate data are reported annually for each country, making it possible to analyze them together without requiring complex transformations.

The metadata files further reinforce this relationship. Country metadata can be reused across both indicators, as it describes stable attributes of each country. Similarly, indicator metadata provides descriptive information that applies to all numerical records associated with a given indicator.

This structure allows the datasets to be integrated into a unified analytical model, where numerical values are enriched with descriptive context. <br>As a result, the datasets can be treated as a coherent data source rather than as isolated files.

<br>

## 2.3 Final dataset selection decision
Using both indicators provides a richer analytical context compared to working with a single dataset. 
Combining GDP per capita and unemployment rate allows the analysis of economic performance together with labor market conditions, without increasing technical complexity.

In addition, both datasets share the same temporal and geographical granularity, which simplifies data integration and enables the design of a simple and consistent analytical model.

Based on these considerations, the final dataset selection includes:
- Two main fact datasets: GDP per capita and unemployment rate
- Country metadata as a unified country dimension
- Indicator metadata as a descriptive indicator dimension

This selection is sufficient to support the objectives of the project while keeping the scope manageable and aligned with the course requirements.

<br><br>

# **3. Data ingestion and initial profiling**
The goal of this section is to load the raw datasets and perform an initial inspection to understand their structure and basic characteristics. At this stage, no data cleaning or transformation is applied.
The objective is to analyze the schema, data types, and the presence of missing values, which will serve as the basis for making subsequent preprocessing decisions.


The initial profiling focuses on the main datasets (facts), which contain the numerical values of the selected economic indicators.

Initial observations after loading the raw fact datasets:
- Both datasets have 266 rows and follow a wide format, where each year is represented as a separate column.
- An extra column named "Unnamed: 69" is present in both files and contains only missing values.
- GDP per capita has data coverage starting from 1960 for many countries, with missing values in earlier years for some cases.
- The unemployment dataset shows no available values from 1960 to 1990, while data becomes available from 1991 onwards for most countries.

<br>

Metadata profiling results:
- The country metadata files from both ZIP packages have the same shape, columns, and identical content. For this reason, a single unified country dimension will be used to avoid duplication.
- The indicator metadata files contain one record per indicator and provide descriptive information (name, source note, and source organization).
- Similar to the fact datasets, the metadata files also include extra "Unnamed" columns with only missing values, which will be removed during preprocessing.

<br><br>

# **4. Data preprocessing and cleaning**
This section transforms the raw datasets into a cleaner, more consistent, and analysis-ready format.
The objective is to prepare the data for integration into a dimensional model while preserving the original semantic meaning of the indicators.

The preprocessing and cleaning steps are deliberately separated into two phases:
- Structural preprocessing, focused on schema standardization and format alignment.
- Data cleaning and quality validation, focused on enforcing integrity constraints and domain rules.

This separation helps distinguish between format-driven transformations and data quality decisions, and makes the pipeline easier to understand, maintain, and justify.

<br>

## 4.1 Structural preprocessing
After the initial profiling phase, a set of structural preprocessing steps was applied in order to standardize the datasets and make them suitable for integration and analysis.

At this stage, the goal is not to alter the analytical meaning of the data, but to ensure that all datasets share a clean, consistent, and well-defined structure. These operations focus on format alignment and schema clarity, and they serve as a prerequisite for later cleaning, validation, and modeling steps.

The main structural preprocessing actions performed are:

1. **Removal of non-informative columns**

    During the initial inspection of the raw CSV files, several columns were identified that do not contain any analytical information.
    In particular, all fact and metadata datasets include columns named Unnamed:*, which are completely empty and originate from the CSV export process.

    These columns do not represent economic variables, identifiers, or descriptive attributes, and keeping them would only introduce noise into the schema.
    For this reason, all columns whose name starts with Unnamed were systematically removed from both fact datasets and metadata tables.

    This operation is safe and non-destructive, as it does not affect any meaningful data values or keys. Its purpose is solely to improve schema clarity and avoid propagating irrelevant columns through later stages of the pipeline.

<br>

2. **Reshaping of the fact datasets from wide to long format**

    The raw World Bank fact datasets are provided in a wide format, where each year is represented as a separate column.
    While this format is convenient for manual inspection, it is not suitable for analytical processing, time-based filtering, or dimensional modeling.

    To address this limitation, both fact datasets were reshaped into a long format, where:

    - Each row represents a single observation.
    - Time is stored explicitly in a year column.
    - All numerical values are stored in a single value column.
    - Country and indicator identifiers are preserved as explicit attributes.

    This transformation produces a standard fact-table structure of the form: (country_code, year, indicator_code, value)

    Such a structure is widely used in analytical systems and data warehouses, as it allows efficient filtering and aggregation by year, consistent joins with time, country, and indicator dimensions, integration of multiple indicators into a single fact table, and consistent handling of missing values across time.

    At this stage, missing values are intentionally preserved as NaN, as they represent genuine gaps in the source data.
    No imputation is performed, as filling missing economic values without strong assumptions could introduce bias or distort subsequent analyses. 

<br>

3. **Selection of a common and consistent time range based on data availability**

    After reshaping the datasets to long format, a common time range was selected to ensure that both indicators can be compared consistently.

    Rather than choosing a time window arbitrarily and to make this decision objective, a coverage-based criterion was used. In this context, coverage refers to the proportion of countries that have a non-missing value for a given year.
    For example, a coverage ratio of 0.80 means that at least 80% of the countries in the dataset contain a valid value for that year.

    For each year, the coverage ratio is computed as: <pre>coverage = (number of countries with a valid value) / (total number of countries)</pre>
    Note: The total number of countries is computed per year as the number of distinct country codes present in that year, which avoids bias caused by countries entering or leaving the dataset over time.

    A minimum coverage threshold of 80% was selected as a compromise between data availability and analytical reliability.
    Years that do not meet this threshold in either dataset are excluded from the final fact tables.

    This approach avoids including years with extremely sparse data, which would otherwise result in large amounts of missing values and unreliable comparisons.
    In particular, the unemployment dataset contains almost no valid observations before 1991, making earlier years unsuitable for joint analysis.

    Based on this objective criterion, the final time range retained for both indicators is: 1991 - 2024

    This ensures temporal consistency across datasets while maximizing the amount of usable information.

<br>

## 4.2 Data cleaning and quality validation
Once the datasets were structurally standardized, a second phase focused on data cleaning and quality validation was applied.
Unlike structural preprocessing, this phase involves explicit decisions about which records are valid, which should be removed, and how to handle implausible values.

The guiding principle throughout this phase is to apply minimal and defensible corrections:

- Rows are removed only when they violate fundamental integrity constraints.
- Implausible numerical values are not guessed or imputed, but explicitly set to missing and documented.
- All cleaning actions are logged to ensure transparency and reproducibility.

The main validation rules applied are described below.


1. **Key integrity and type validation**

    The fact table relies on a composite key consisting of: *country_code*, *year* and *indicator_code*.

    Rows where any of these key fields are missing are removed, as such records cannot be reliably identified or joined with dimension tables.<br>
    In addition, the year column is explicitly converted to a numeric type.
    If a year value cannot be parsed as a number, the corresponding row is removed.
    If the year contains a decimal value (e.g. 1993.5), it is truncated to its integer part, as the data is annual and fractional years do not carry meaningful information in this context.<br>
    Rows with years outside the previously selected range (1991–2024) are also removed, as they fall outside the analytical scope of the project.

    As a defensive data quality measure, leading and trailing whitespace is removed from key fields (country_code and indicator_code) in both fact and dimension tables.
    Although the source data is expected to be clean, invisible whitespace characters are a common source of join mismatches and referential integrity issues in real-world pipelines.<br>
    This normalization step does not alter the semantic meaning of the keys, but ensures that logically identical values are treated consistently during joins and validations.

<br>

2. **Indicator scope enforcement**

    The project explicitly focuses on two indicators:
    - GDP per capita (NY.GDP.PCAP.CD)
    - Unemployment rate (SL.UEM.TOTL.ZS)

    Any rows containing other indicator codes are removed.
    This ensures that the final fact table strictly reflects the intended analytical scope and avoids accidental inclusion of unrelated metrics.

<br>

3. **Duplicate detection**

    Duplicate rows based on the composite key (country_code, year, indicator_code) are identified and removed, keeping only the first occurrence.

    Duplicates are not expected in the source data, but this validation step acts as a safeguard against accidental duplication during transformations or future pipeline extensions.

<br>

4. **Referential integrity checks**

    To guarantee consistency between fact and dimension tables, referential integrity is enforced:

    - Rows whose country_code does not exist in the country dimension are removed.
    - Rows whose indicator_code does not exist in the indicator dimension are removed.

    This ensures that every fact record can be properly contextualized with descriptive information.

<br>

5. **Domain validation rules**

    Finally, basic domain-specific validation rules are applied to numerical values:

    - Unemployment rate values are expected to lie within the range [0, 100].<br>
    Values outside this range are considered invalid and are set to NaN, rather than removing the entire row.

    - GDP per capita values are expected to be strictly positive.<br>
    Zero or negative values are treated as invalid and are also set to NaN.

    In both cases, invalid values are not replaced or imputed.
    Preserving them as missing avoids introducing artificial information while maintaining the structural integrity of the fact table.

<br>

The final outcome of this phase is a fact table that:

- Contains only valid keys and years within scope.
- Enforces referential integrity with dimension tables.
- Preserves missing values explicitly.
- Applies conservative domain rules without speculative imputation.

All cleaning actions are logged, allowing the full data preparation process to be audited and reproduced.

<br><br>

# **5. Dimensional model and processed outputs**
Once the datasets were standardized and validated through data quality rules, the next step is to integrate them into a simple dimensional model. The purpose of this phase is to transform the cleaned data into a structure that is easier to query, aggregate, and extend in analytical contexts.

A dimensional model separates descriptive attributes (dimensions) from numerical measurements (facts). In this project, the two selected World Bank indicators are integrated into a single fact table and enriched through two shared dimensions: a country dimension and an indicator dimension. 

<br>

1. **Country dimension (dim_country)**

    The country dimension is built from the country metadata provided in the World Bank datasets. During the initial profiling phase, both country metadata files (one per ZIP package) were found to have identical structure and identical content. Based on this observation, a single unified country dimension is used throughout the model, avoiding unnecessary duplication.

    Each row in the country dimension represents a single country, uniquely identified by its country code, which acts as the primary key. The remaining columns store descriptive attributes such as geographical region, income group, and additional notes provided by the World Bank.

    Before the dimension is finalized, basic integrity checks are applied. Rows with missing or empty country codes are removed, duplicate country codes are resolved by keeping the first occurrence, and whitespace is trimmed from the key field to prevent subtle join mismatches. These steps ensure that the country dimension can be reliably joined with the fact table and reused consistently across indicators.

<br>

2. **Indicator dimension (dim_indicator)**

    The indicator dimension provides semantic context for the numerical values stored in the fact table. Each World Bank ZIP file includes metadata describing the corresponding indicator, such as its name, definition, and data source.

    To support a unified analytical model, the indicator metadata from both datasets is combined into a single indicator dimension. Each row represents one indicator and is uniquely identified by its indicator code, which serves as the primary key.

    This dimension allows the fact table to remain compact and normalized, while still preserving all descriptive information required to interpret the values correctly. As with the country dimension, minimal cleaning is applied to ensure integrity: null or empty indicator codes are removed, duplicates are eliminated if present, and whitespace is stripped from key fields.

<br>

3. **Fact table (fact_economic_indicators)**

    The core of the dimensional model is the fact table, which stores the numerical observations for both indicators. After reshaping the original datasets into long format and restricting them to the common time range (1991–2024), the two fact datasets are vertically combined into a single table.

    Each row in the fact table represents one annual observation of one indicator for one country. The table follows a simple and explicit structure, consisting of the country code, year, indicator code, and the observed value. Together, the combination of country code, year, and indicator code forms the composite key of the fact table.

    Before being considered final, the fact table is subjected to all data quality rules defined in the previous section. Records with invalid or missing keys are removed, years are validated and constrained to the selected range, indicator scope is enforced, duplicates are eliminated, and referential integrity with both dimensions is checked. Domain-specific rules are applied conservatively: implausible values (such as unemployment rates outside the [0, 100] range or non-positive GDP per capita values) are set to missing rather than imputed or discarded.

    The result is a fact table that is structurally consistent, semantically clear, and suitable for analytical use without introducing artificial assumptions.

<br>

### Star schema overview

This design follows the principles of a star schema, where the fact table is the central structure and dimensions provide contextual information through well-defined keys.

![Star schema](docs\star-schema-diagram.png)

<br>



Once the dimensional model has been fully constructed and validated, the final datasets are written to disk in the data/processed/ directory. This directory contains three CSV files:

- A country dimension table (dim_country).
- An indicator dimension table (dim_indicator).
- A unified fact table containing both economic indicators (fact_economic_indicators).

These files constitute the final output of the data preparation process. By saving the datasets only after all preprocessing, validation, and modeling steps have been completed, the processed outputs can be safely reused for subsequent tasks such as exploratory analysis, visualization, or loading into a database.

All output generation and processing steps are recorded through logging, ensuring that the full data preparation process is traceable and reproducible across executions.

<br><br>

# **6. Cloud storage (Google Cloud Storage)**
After generating the processed datasets locally, the pipeline includes a cloud storage stage to make the outputs accessible outside the local environment and to prepare them for later analytical components. In this project, Google Cloud Storage (GCS) is used as the cloud storage layer.

The main purpose of this step is to move the final processed tables (dimensions and fact table) into a centralized and reliable storage location that can later be consumed by services such as BigQuery and external BI tools. This also makes the pipeline more realistic and aligned with real-world data engineering workflows, where processed outputs are rarely kept only on a local machine.

<br>

The pipeline exports the three CSV files generated in the dimensional modeling stage: the country dimension, the indicator dimension, and the unified fact table. These files represent the final and validated version of the data, as all preprocessing, cleaning, and integrity checks have already been applied at this point.

Exporting only the processed outputs, and not intermediate datasets, is an intentional design decision. The goal is to keep the cloud layer clean and to ensure that any dataset stored in the cloud is immediately ready to be reused for analytical purposes, without requiring additional transformations or quality checks.

<br>

All processed files are uploaded to a dedicated bucket:
- Bucket: wb-economic-pipeline-jrevuelta-001
- Prefix (folder): processed/

Using a prefix inside the bucket provides a simple and scalable structure that keeps the storage layer organized. Even though only processed outputs are uploaded in the current version of the project, this structure would allow future extensions. For example, additional prefixes could be introduced to store raw source extracts, intermediate staging outputs, or execution logs. 

This aligns with common cloud data lake conventions and keeps the project organized as it grows.

<br>

## 6.2 Why GCS is used in the pipeline
Google Cloud Storage is used because it satisfies several key requirementsof a realistic data engineering workflow:
- *Centralized storage*: the processed outputs are no longer tied to a single local machine and can be accessed consistently from different environments.
- *Durability and availability*: data is reliably stored and accessible for later stages.
- *Integration with BigQuery*: GCS is a standard and efficient source for loading data into BigQuery tables, which makes it a natural intermediate layer between local processing and analytical querying.
- *Reproducibility*: once uploaded, the exported outputs can be reused and validated independently from the local execution environment.

In other words, the GCS layer acts as a bridge between the local processing pipeline and the analytical warehouse stage.

<br>

## 6.3 Configuration-based activation

Cloud export is controlled through configuration flags defined in the project configuration file. In particular, the "ENABLE_GCS_EXPORT" flag determines whether the processed outputs are uploaded to Google Cloud Storage or kept only in the local environment.

This design allows the pipeline to be executed in different modes without modifying the core logic of the code. When cloud export is disabled, the pipeline runs entirely locally and produces the processed CSV outputs on disk. When cloud export is enabled, the same pipeline additionally uploads those outputs to GCS.

This approach improves portability and makes the project easier to evaluate. For example, the professor can run the pipeline locally without requiring cloud credentials, and can later enable cloud export to verify the full workflow.

<br>

Once the cloud export stage is enabled and executed successfully, the three processed files become available in the bucket under the defined prefix:
- gs://wb-economic-pipeline-jrevuelta-001/processed/dim_country.csv
- gs://wb-economic-pipeline-jrevuelta-001/processed/dim_indicator.csv
- gs://wb-economic-pipeline-jrevuelta-001/processed/fact_economic_indicators.csv

This completes the cloud storage step and prepares the data for the next stage: loading the same tables into BigQuery as an analytical warehouse.

<br><br>

# **7. Analytical warehouse (BigQuery)**
After the processed datasets are stored in Google Cloud Storage, the pipeline includes an optional analytical warehouse layer based on Google BigQuery. This stage represents the transition from data preparation to analytical consumption, following a common architecture used in real-world data engineering systems.

In a production scenario, cloud storage is typically used as a landing or delivery layer, while analytical queries and aggregations are executed on top of a dedicated data warehouse. BigQuery fulfills this role by providing a fully managed, scalable, and SQL-based analytical engine that can operate directly on structured datasets.

<br>

In this project, the processed tables generated by the pipeline (country dimension, indicator dimension, and unified fact table) are designed to be directly loadable into BigQuery without additional transformations. This is possible because all structural preprocessing, data quality validation, and modeling decisions have already been applied in earlier stages of the pipeline.

Once loaded into BigQuery, the dimensional model enables efficient analytical queries such as aggregations over time, comparisons across countries or regions, and joins between facts and dimensions. Although the project does not focus on complex SQL analysis, the warehouse layer provides a solid foundation for exploratory analysis, reporting, and visualization.

<br>

The BigQuery layer is included as an optional extension of the pipeline rather than as a mandatory component. Its role is simply to show how the processed datasets can be moved one step further and used in an analytical environment once the data preparation phase is complete.

In this sense, BigQuery acts as a natural continuation of the pipeline: the data is first cleaned, validated, and modeled, and only then made available for analysis. This reflects a common workflow in real projects, where data engineering focuses on producing reliable datasets that can later be explored and visualized using analytical tools.

<br>

In summary, the BigQuery layer illustrates how the pipeline could be extended beyond local processing and cloud storage, completing the path from raw data ingestion to scalable analytical querying.

<br><br>

# **8. Semantic layer (analytical view)**
Once the processed data is available in BigQuery, an additional semantic layer is introduced to make the dataset easier to interpret and consume from analytical and visualization tools. This layer is implemented as a SQL view that combines the fact table with the corresponding dimension tables.

The main objective of this step is not to perform new transformations, but to enrich the numerical observations with descriptive context. Instead of working directly with technical identifiers such as country codes or indicator codes, the view exposes human-readable attributes such as country name, region, income group, and indicator description.

The semantic view joins: the fact table containing yearly indicator values, the country dimension providing geographical and socioeconomic attributes, and the indicator dimension providing descriptive information about each metric.<br>
As a result, each record in the view represents a fully enriched observation that can be directly queried or visualized without requiring additional joins.

This approach offers several practical advantages. First, it simplifies analytical queries by hiding join logic and technical keys from end users. Second, it reduces the risk of inconsistent joins when multiple analyses are performed. Finally, it provides a stable and well-defined interface between the data engineering pipeline and downstream consumers.

Although the semantic layer is implemented as a simple SQL view in this project, the same concept is widely used in real-world systems, where semantic layers play a key role in standardizing metrics and ensuring consistent interpretation across teams and tools.

<br><br>

# **9. Data visualization and insights (Looker Studio)**
Once the data has been processed, validated, stored in the cloud, and enriched through the semantic layer, the final step of the pipeline focuses on visualization and exploratory analysis. In this project, Google Looker Studio is used to build an interactive dashboard on top of the analytical view stored in BigQuery.

The goal of this stage is not to perform advanced statistical analysis, but to demonstrate how the engineered data can be effectively consumed, explored, and interpreted by end users. This completes the end-to-end workflow, connecting raw data ingestion with human-readable insights.

Looker Studio was selected because it integrates natively with BigQuery and allows fast creation of interactive dashboards without additional data movement. By connecting directly to the semantic view, the visualizations automatically benefit from the joins and descriptive attributes defined in the previous stage.

## 9.2 Dashboard structure and visualizations
The dashboard is built on top of the enriched analytical view and includes several complementary visualizations designed to explore the data from different perspectives. Screenshots of the dashboard and each chart are included below to document the final result.

### Economic Indicators Dashboard - 1st Part
![Economic Indicators Dashboard](docs\economic-indicators-dashboard-1.png)
### Economic Indicators Dashboard - 2nd Part
![Economic Indicators Dashboard](docs\economic-indicators-dashboard-2.png)

<br><br>

### **1. Global GDP per capita trend over time**

![Global GDP per capita graph](docs\Global-GDP-per-capita-graph.png)

This chart shows the evolution of average GDP per capita over time across all countries. The metric is aggregated as a yearly average, allowing the identification of long-term growth patterns and global economic trends.

The upward trajectory observed over the selected period reflects sustained global economic growth, driven by factors such as technological progress, international trade, and gradual improvements in living standards across many regions.

Although this visualization represents a global average and therefore smooths out regional disparities, it provides a useful high-level perspective on how economic output per person has evolved over time at a worldwide scale.

<br>

### **2. Global unemployment rate trend over time**

![Global unemployment rate](docs\Global-unemployment-rate.png)

This chart displays the average unemployment rate per year across countries, offering a complementary perspective focused on labor market conditions rather than economic output.

Unlike GDP per capita, the unemployment rate does not follow a steady long-term trend. Instead, it exhibits periods of relative stability interspersed with increases and decreases, reflecting the sensitivity of labor markets to economic cycles, financial crises, and structural changes in different economies.

When interpreted together with the GDP per capita trend, this visualization highlights that improvements in economic output do not always translate directly or immediately into better employment outcomes.

<br>

### **3. Comparison of indicators over time (GDP vs unemployment)**

![GDP vs UEM](docs\GDP-vs-UEM.png)

This chart allows for a comparison of the evolution of both indicators over time within the same analytical framework. By using the same time axis, it facilitates the observation of how economic growth and labor market conditions evolve over time, although not necessarily in sync.

The visualization reveals that, while GDP per capita tends to show a long-term upward trend, the unemployment rate exhibits more erratic behavior, with periods of stability alternating with increases and decreases. This difference reinforces the idea that aggregate economic growth does not always translate immediately into improvements in employment.

This chart illustrates the value of integrating multiple indicators within the same analytical model, as it allows for the observation of relationships and contrasts that would not be evident when analyzing each metric in isolation.

<br>

### **4. GDP per capita by income group**

![GDP per capita by income group](docs\GDP-per-capita-by-income-group.png)

This chart breaks down GDP per capita by income groups as defined by the World Bank. Comparing these groups reveals clear structural differences in average economic output and well-being across countries.

High-income countries show significantly higher values, while low- and middle-income countries exhibit lower levels and distinct growth trajectories. This segmentation helps contextualize aggregate values ​​and avoids misleading interpretations based solely on global averages.

The visualization demonstrates how including descriptive attributes, such as income group, enriches the analysis and allows for the interpretation of economic indicators within a broader socioeconomic framework.

<br>

### **5. Unemployment rate by income group**

![Unemployment rate by income group](docs\Unemployment-rate-by-income-group.png)

This chart shows the evolution of the unemployment rate broken down by income group. Unlike GDP per capita, unemployment does not follow a strictly increasing or decreasing pattern, but rather exhibits variations that reflect the specific economic and structural dynamics of each group of countries.

The comparison between groups reveals that unemployment levels and stability differ significantly depending on the economic context. Some groups show greater volatility, while others exhibit more stable patterns over time.

This visualization reinforces the idea that labor market analysis requires contextual segmentation and that labor indicators must be interpreted considering the level of economic development of each country.

<br>

## 9.3 Key insights and observations

While the dashboard is primarily exploratory, several general observations can be made:

- GDP per capita shows a clear upward trend over time when averaged globally. This reflects long-term economic growth driven by technological progress, globalization, productivity improvements, and other factors adjusted for population growth in many countries.
- Unemployment rates exhibit more variability and do not follow the same monotonic pattern. Periods of increased unemployment often coincide with global or regional economic downturns or financial crises, which highlights the sensitivity of labor markets to economic shocks.
- Income group segmentation reveals strong structural differences in both indicators. Higher-income countries consistently display higher GDP per capita levels, while unemployment dynamics vary more significantly across groups.
- Combining multiple indicators provides a more complete picture than analyzing each metric in isolation. While GDP per capita captures overall economic performance, unemployment adds a complementary perspective focused on social and labor conditions, allowing a more balanced interpretation of economic development.

These observations demonstrate that the processed data not only meets technical quality requirements, but also reflects real-world economic facts in a coherent and interpretable way. The dashboard therefore serves as a meaningful analytical layer on top of the engineered data pipeline.

<br><br>

# **10. Pipeline orchestration, logging, and reproducibility**
Once the preprocessing, cleaning, modeling, and export steps were defined, the pipeline was structured as a reproducible end-to-end workflow executed from a single entry point (main.py). The role of this file is to orchestrate the full lifecycle of the data: loading the raw inputs, applying transformations in the correct order, validating integrity rules, producing the processed outputs, exporting them, and optionally publishing them to cloud and analytical services.

This design follows a clear separation of responsibilities:
- Each pipeline stage is implemented as an independent module (ingestion, profiling, preprocessing, modeling, data quality, and cloud export).
- "main.py" acts as the coordinator that defines the execution sequence and the dependencies between stages.
- The pipeline produces deterministic outputs given the same input files and configuration.

<br>

## 10.1 Orchestration flow (main.py)
The pipeline execution starts by configuring logging and resolving project paths. Then, the main datasets (facts) and metadata datasets are loaded from the "data/raw/" directory. Profiling is executed early in the workflow to provide visibility into schema structure and missing-value patterns before any transformation is applied.

After that, structural preprocessing ensures that all datasets share a consistent format:
- Non-informative columns are removed.
- Wide-format fact datasets are reshaped into long format.
- A common year range is selected based on data coverage, and both indicators are filtered accordingly.

Once the data has a consistent structure, the dimensional model is built, consisting of two shared dimensions (country and indicator) and a unified fact table. Before any outputs are published, a final data quality layer enforces integrity constraints and domain rules, ensuring that the processed datasets are safe to reuse for analytics.

After the dimensional model is finalized, the pipeline proceeds to the output and integration stages. The processed datasets are first written to disk in the local data/processed/ directory. From there, optional cloud-related steps can be activated through configuration flags:

- If cloud storage export is enabled, the processed datasets are uploaded to Google Cloud Storage.
- If BigQuery export is enabled, the same datasets are loaded into BigQuery tables, where they can be queried and enriched through analytical views.
- These analytical tables and views are then consumed by the visualization layer (Looker Studio), completing the end-to-end workflow.

This configuration-based orchestration allows the pipeline to be executed in different modes, ranging from a fully local execution to a complete cloud-enabled workflow, without modifying the pipeline logic itself.

<br>

### Pipeline execution flow - 1st Part
![Pipeline execution flow diagram](docs\flow-diagram1.png)
### Pipeline execution flow - 2nd Part
![Pipeline execution flow diagram](docs\flow-diagram2.png)
### Pipeline execution flow - 3rd Part
![Pipeline execution flow diagram](docs\flow-diagram3.png)

<br>

For transparency, the full pipeline steps and intermediate decisions are captured through logs and can be reviewed execution by execution.

<br>

## 10.2 Logging strategy and traceability
Logging is treated as a core part of the pipeline, not as an optional debug feature. Each execution produces a dedicated log file that records:
- which steps were executed,
- dataset shapes before and after key transformations,
- how many rows were removed due to integrity violations,
- how many values were invalidated by domain rules,
- which export stages were enabled,
- and where the final outputs were saved (local paths, cloud buckets, or analytical tables).

To avoid uncontrolled growth of log files across executions, a retention policy is applied: only the last **N** pipeline logs are kept, and older logs are automatically removed. This preserves recent execution history (useful for debugging and reproducibility) while keeping the project lightweight and manageable over time.

As a result, the pipeline can be audited and reproduced reliably: for any execution, the log provides a complete trace of the applied transformations, validation decisions, and integration steps, making the pipeline suitable for both academic evaluation and realistic data engineering scenarios.

<br><br>

# **11. Conclusions and answers to project questions**
This final section brings the project to a close by summarizing the overall work and reflecting on the design decisions made throughout the development of the pipeline. The project was conceived as a complete end-to-end data engineering workflow, starting from raw data ingestion and progressing through preprocessing, validation, dimensional modeling, cloud storage, analytical querying, and final visualization.

Rather than focusing on isolated technical components, the objective was to design a coherent and reproducible pipeline in which each stage serves a clear purpose and feeds naturally into the next one. Special attention was given to data quality, modularity, and clarity, ensuring that the pipeline remains understandable, extensible, and aligned with real-world data engineering practices.

The following subsections address the specific questions proposed in the project assignment and conclude with a general reflection on the lessons learned, the limitations of the current implementation, and possible improvements in a more production-oriented setting.

<br>

## 11.2 Scalability and performance when scaling the raw data (x10, x100, x1000, x10⁶)
To reason about scalability, we can treat the main driver as the number of rows in the fact table. In the current version, the dataset is relatively small (annual values, two indicators, per country). However, multiplying the number of rows by x10, x100, x1000 or x10⁶ helps anticipate what would happen if the same pipeline logic were applied to a much larger domain. Such growth could come from adding more indicators, increasing temporal granularity (for example, monthly or daily data), incorporating additional countries or regions, or extending the pipeline to cover multiple datasets from different domains.

From a practical perspective, the pipeline behaves proportionally to the amount of data processed. Most of the steps performed, such as reshaping the datasets, filtering years, joining fact data with dimensions, and applying validation rules, are applied row by row in a single pass.<br>
This means that when the number of rows increases, execution time and memory usage increase accordingly, but without unexpected jumps in complexity. As a result, the impact of data growth on performance is predictable and can be anticipated when planning infrastructure or execution environments.

This linear behavior also has direct implications for cost. At small scales, local execution is sufficient and infrastructure costs are effectively zero. As data volume grows, longer execution times and higher memory usage translate into higher computational costs, especially if cloud-based resources are used. In such cases, costs increase mainly due to compute time and resource allocation rather than algorithmic inefficiencies.

In practice, for moderate growth scenarios (x10 or x100), the pipeline could still be executed locally without major issues and with minimal cost impact. For larger scales (x1000 or x10⁶), execution time and memory usage would increase significantly, making cloud-based storage and analytical processing more appropriate.

If the data volume were to grow significantly, the pipeline could be evolved incrementally rather than redesigned from scratch. For example, local processing would eventually become less suitable, and distributed or cloud-based processing would be a more appropriate choice. At this point, costs shift from being negligible to being an explicit design concern, and the choice of execution environment becomes critical.

If the data volume were to grow significantly, the pipeline could be evolved incrementally rather than redesigned from scratch. For example, local processing would eventually become less suitable, and distributed or cloud-based processing would be a more appropriate choice. While this introduces infrastructure costs, it also allows the workload to scale in a controlled and predictable way.

Similarly, at large scales, analytical queries and dashboards would need to be optimized to avoid scanning unnecessary data. Techniques such as partitioning, pre-aggregation, or incremental processing would help keep both performance and operational costs under control while preserving the same logical pipeline structure.<br>
These adaptations would allow the pipeline to scale gracefully while preserving the same conceptual design and data model, reinforcing the separation between logical processing and execution infrastructure.

<br>

## 11.3 Cloud migration cost (Google Cloud) for x10, x100, x1000, x10⁶
To estimate the cost of migrating and scaling this pipeline to the cloud, Google Cloud Platform is taken as a reference provider. The analysis focuses on the two main recurring cost components in this type of architecture: storage and analytical query processing. Occasional costs such as data transfer or orchestration overhead are negligible for the scale considered and are therefore omitted.

**1. Storage costs**
In this project, processed datasets are stored in Google Cloud Storage and then loaded into BigQuery for analytical querying. Based on the current processed outputs, the total size of the data is well below 1 MB. Even when considering growth scenarios of x10, x100, or x1000, the total storage footprint remains extremely small.

In this project, processed datasets are stored in Google Cloud Storage and then loaded into BigQuery for analytical querying. Based on the current processed outputs, the total size of the data is well below 1 MB. Even when considering growth scenarios of x10, x100, or x1000, the total storage footprint remains extremely small.

Using BigQuery active storage pricing (approximately $0.02 per GiB per month), storage costs are effectively negligible up to x1000. Even in an extreme x10⁶ scenario, where the fact table would grow to hundreds of millions of rows, total storage would likely remain in the order of a few tens of gigabytes, resulting in monthly storage costs on the order of a few dollars.

This confirms a common pattern in analytical systems: storage costs are rarely the dominant factor, even at large scales.

<br>

**2. Query and processing costs**
The main cost driver in BigQuery is query execution. BigQuery on-demand pricing charges per amount of data scanned, approximately $6.25 per TiB processed. As a result, costs depend primarily on how often large tables are queried and how much data each query scans.

At small and moderate scales (x10 and x100), query costs are effectively negligible. Even repeated dashboard queries would scan very small amounts of data and remain well within free or near-free usage.<rb>
At larger scales (x1000 and especially x10⁶), query costs become more relevant. If dashboards or specific analyses repeatedly scan the full fact table, monthly costs could increase noticeably, even if storage itself remains inexpensive. In these scenarios, inefficient queries, not data volume alone, become the main source of cost.

To ensure that costs remain manageable as data volume grows, several well-established optimization strategies can be applied without changing the logical structure of the pipeline:
- Partitioning the fact table by year to reduce the amount of data scanned by time-based queries.
- Clustering the fact table by commonly filtered fields such as country_code and indicator_code.
- Using pre-aggregated tables or materialized views for BI dashboards instead of querying the raw fact table directly.
- Limiting dashboard queries to the required columns and time ranges.

With these optimizations in place, even large-scale scenarios remain economically viable. The key takeaway is that BigQuery costs scale with query behavior rather than raw data size, and careful design of the analytical layer is sufficient to keep expenses under control.

<br>

## 11.4 Data consumers and data delivery design
Once the data has been processed, validated, and made available through the analytical layer, it is important to consider who can consume this data and how it should be delivered to different types of users. In real-world data engineering projects, the same dataset is often consumed by multiple profiles, each with different technical skills and analytical needs.

In this project, the pipeline is designed to support several categories of data consumers, all of them relying on the same processed and validated datasets as a common foundation.

From an analytical perspective, business analysts and non-technical users are the primary target audience. These users typically interact with the data through dashboards and reports rather than through raw tables. By exposing the data through BigQuery and connecting it to Looker Studio, the pipeline provides a clean and user-friendly access layer where analysts can explore trends, filter by time or income group, and interpret results without needing to write SQL or understand the internal structure of the pipeline. This delivery method prioritizes accessibility, interpretability, and low friction.

At the same time, technical users such as data analysts or data scientists can consume the data directly from BigQuery. For these users, the analytical warehouse acts as a structured and efficient source of information for querying. They can run SQL queries, perform deeper exploratory analysis, or export subsets of the data to external tools for further modeling or experimentation. Because the data has already been cleaned, validated, and enriched with dimensions, these users can focus on analysis rather than data preparation.

Finally, the pipeline also supports system-level consumption. Processed datasets stored in Google Cloud Storage can be accessed by other pipelines, batch jobs, or downstream services. This allows the project to integrate easily into larger data ecosystems, where multiple pipelines or applications depend on shared and standardized datasets.

Overall, the chosen delivery design follows a layered approach:
local processing produces validated outputs, cloud storage ensures availability and durability, the analytical warehouse enables efficient querying, and the visualization layer provides an accessible interface for end users. This separation of concerns makes the data easy to consume by different audiences while preserving a single, consistent source of truth.

<br>

## 11.5 How Artificial Intelligence can support the data engineering pipeline
Artificial Intelligence can play a complementary role in different stages of a data engineering pipeline. Even though this project does not directly implement AI models, several potential integration points can be identified based on the pipeline design and the concepts seen in class.

A useful way to reason about AI integration is to distinguish between three main patterns: AI for the pipeline, AI in the pipeline, and AI on the pipeline output.

Artificial Intelligence can support the pipeline itself in relatively simple ways. For example, AI techniques could be used to help detect unusual situations in the data, such as unexpected changes in values, missing data patterns, or inconsistencies that are difficult to define with fixed rules. In this sense, AI would act as an additional support mechanism to improve data quality monitoring and pipeline reliability, rather than as a core transformation component.

AI can also be used within the pipeline as part of the data transformation process, especially when dealing with more complex or unstructured data. In more advanced scenarios than the one covered in this project, AI models could help extract information from text fields, classify records automatically, or enrich datasets with additional attributes that are not explicitly present in the raw data. In the context of this project, however, the datasets are fully structured and well-defined, so classical rule-based transformations are sufficient. As a result, there is no practical need to introduce AI-based transformations here, but the pipeline design would allow it if the data characteristics or project requirements were to change in the future.

Finally, AI can be applied after the pipeline has produced clean and well-structured outputs. Once data has been processed, validated, and modeled, it becomes suitable for further analytical or predictive use. The resulting fact and dimension tables could serve as a reliable input for future machine learning models, forecasting tasks, or other advanced analyses. In this case, the role of the data engineering pipeline is to ensure that the data is consistent and trustworthy before any AI-based analysis is applied.

In summary, although this project does not implement AI directly, its structure is compatible with different forms of AI integration discussed in class. AI could be used to support data quality, assist in data enrichment, or take advantage of the final processed outputs. Importantly, these possibilities build on top of a solid data engineering foundation rather than replacing it.

<br>

## 11.6 Privacy and data protection considerations
From a privacy perspective, this project does not present significant risks. All datasets used come from the World Bank Open Data platform, which provides openly accessible, aggregated, and anonymized information. The data describes countries and macroeconomic indicators, not individuals, companies, or any form of personally identifiable information.

As a result, there are no concerns related to personal data protection, consent, or sensitive information handling. No private or confidential data is ingested, processed, or stored at any stage of the pipeline. This greatly simplifies both the technical design and the governance requirements of the project.

If the pipeline were to be extended in the future to include micro-level data (for example, individual employment records or company-level economic data), privacy would become a central concern. In such cases, additional measures would be required, including access control, data anonymization, encryption, and compliance with relevant data protection regulations.

In its current form, however, the project operates entirely on public and non-sensitive data, making it safe from a privacy standpoint and suitable for open analytical and educational use.

<br>

## 11.7 Final conclusions
This project demonstrates the design and implementation of a complete end-to-end data engineering pipeline, starting from raw public data and ending with meaningful visual insights. Throughout the development process, the focus has been on building a coherent, reproducible, and well-structured workflow rather than on isolated technical components.

Each stage of the pipeline serves a clear purpose and is logically connected to the next one. Raw data ingestion and profiling provide initial understanding, preprocessing and cleaning ensure data quality and consistency, dimensional modeling enables analytical usability, cloud storage and the analytical warehouse prepare the data for scalable access, and visualization translates the engineered data into human-readable insights. This clear separation of responsibilities makes the pipeline easy to understand, maintain, and extend.

A key strength of the project is the emphasis on data quality and transparency. Validation rules, logging, and reproducibility are treated as first-class concerns rather than as secondary tasks. This reflects real-world data engineering practices, where trust in the data is as important as technical correctness.

While the current implementation operates at a relatively small scale and uses simple economic indicators, the pipeline has been designed with scalability and extensibility in mind. The analysis of potential growth scenarios shows that the same logical structure can be preserved even as data volume increases, with appropriate changes to execution environment and optimization strategies.

The project also illustrates how a well-designed data engineering pipeline can serve multiple types of consumers, from analysts and BI tools to potential downstream machine learning applications. Although advanced analytics and AI are not implemented directly, the pipeline is compatible with such extensions, highlighting the importance of solid data foundations.

In conclusion, this work fulfills the objectives of the assignment by applying core data engineering concepts in a practical and realistic context. It provides a strong foundation for future improvements, such as incremental processing, distributed execution, or richer analytical use cases, and reinforces the idea that robust data engineering is a critical prerequisite for any data-driven system.