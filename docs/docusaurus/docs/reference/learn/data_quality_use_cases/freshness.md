---
sidebar_label: 'Freshness'
title: 'Validate data freshness with GX'
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Data freshness, a critical aspect of data quality, refers to how up-to-date or current the data is relative to the source system or the real-world events it represents. In today's fast-paced business environment, stale data can lead to significant consequences across industries. For example, in e-commerce, outdated inventory data can result in oversold products and frustrated customers. In financial trading, stale market data can lead to missed opportunities and potential compliance issues. These scenarios underscore the critical importance of data freshness validation to ensure accurate insights and timely decision-making.

Great Expectations (GX) offers a powerful set of tools for monitoring and validating data freshness through its freshness-focused Expectations. By integrating these Expectations into your data pipelines, you can establish robust checks that ensure your datasets maintain the expected level of freshness, catch staleness issues early, and prevent downstream problems in your data workflows.

This guide will empower you to effectively manage and validate data freshness using GX, ensuring your organization has high-quality, up-to-date datasets for accurate insights and timely actions.

## Prerequisite knowledge

This article assumes basic familiarity with GX components and workflows. If you're new to GX, start with the [GX Cloud](https://docs.greatexpectations.io/docs/cloud/overview/gx_cloud_overview) and [GX Core](https://docs.greatexpectations.io/docs/core/introduction) overviews to familiarize yourself with key concepts and setup procedures.

## Data preview

The examples in this article use a sample financial transaction dataset that is provided from a public Postgres database table. The sample data is also available in [CSV format](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/freshness_financial_transfers.csv).

| transfer_type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_ts       |
|----------|------------------------|--------------------|-----------------|---------------------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    |
| domestic | 954005011218           | Nelson O’Connell   | 3285.21         | 2024-05-01 05:08    |

The `transfer_ts` column in this dataset captures the timestamp of each financial transaction. By validating the freshness of this column against expected thresholds, we can ensure the data pipeline is delivering timely and up-to-date information for downstream consumers.

This article will demonstrate how to define and implement effective freshness Expectations using GX, focusing on the `transfer_ts` column as a practical example.

## Key freshness Expectations

### ExpectColumnMaxToBeBetween

Expect the column maximum to be between a minimum value and a maximum value.

For the sample financial transaction data, we could validate the freshness of the `transfer_ts` column like:

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/freshness_resources/freshness_expectations.py ExpectColumnMaxToBeBetween"
```

This expects the latest transaction timestamp to be between April 30, 2024 and May 2, 2024, allowing for some small delay in data arrival.

<small>View `ExpectColumnMaxToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_max_to_be_between).</small>

### ExpectColumnMinToBeBetween

Expect the column minimum to be between a minimum value and a maximum value.

Again for the `transfer_ts` column:

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/freshness_resources/freshness_expectations.py ExpectColumnMinToBeBetween"
```

This expects the earliest transaction to have occurred sometime between the start of April 30, 2024 and the start of May 1, 2024.

<small>View `ExpectColumnMinToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_min_to_be_between).</small>

<br/>
<br/>

:::tip[When to use MaxToBeBetween vs MinToBeBetween]
Use ExpectColumnMaxToBeBetween to validate the most recent data point, ensuring your dataset is up-to-date. Use ExpectColumnMinToBeBetween to check the oldest data point, which is useful for verifying data retention policies or identifying unexpectedly old records. Often, using both in combination provides a comprehensive freshness check.
:::

## Example: Setting appropriate freshness thresholds

**Context**: In the sample financial transaction dataset, the `transfer_ts` column captures the timestamp of each transaction. To ensure the data pipeline delivers timely and up-to-date information, we need to validate the freshness of this column against expected thresholds.

**Goal**: Using the `ExpectColumnMaxToBeBetween` and `ExpectColumnMinToBeBetween` Expectations in either GX Core or GX Cloud, validate the freshness of the `transfer_ts` column to ensure transactions fall within the expected time range.

<Tabs
   defaultValue="gx_cloud"
   values={[
      {value: 'gx_core', label: 'GX Core'},
      {value: 'gx_cloud', label: 'GX Cloud'}
   ]}
>

<TabItem value="gx_core" label="GX Core">
Run the following GX Core workflow.

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/freshness_resources/freshness_workflow.py full example code"
```

**Result**:
The validation results will indicate whether the `transfer_ts` column values fall within the expected freshness range, helping ensure the timeliness and reliability of the financial transaction data.

</TabItem>

<TabItem value="gx_cloud" label="GX Cloud">

Use the GX Cloud UI to walk through the following steps.

Use the GX Cloud UI to walk through the following steps.

1. Create a Postgres Data Asset for the `freshness_financial_transfers` table, using the connection string:
   ```
   postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality
   ```

2. Profile the Data Asset.
3. Add an **Expect column max to be between** Expectation to the `transfer_ts` column.
4. Populate the Expectation:
   * Provide a **Min Value** of `2024-05-01 00:00` and a **Max Value** of `2024-05-02 00:00`.
5. Save the Expectation.
6. Add an **Expect column min to be between** Expectation to the `transfer_ts` column.
7. Populate the Expectation:
   * Provide a **Min Value** of `2024-04-30 00:00` and a **Max Value** of `2024-05-01 00:00`.
8. Save the Expectation.
9. Click the **Validate** button to run the freshness Expectations against the Data Asset.
10. Review Validation Results to ensure the `transfer_ts` values fall within the expected range.

</TabItem>
</Tabs>

**GX solution**: GX enables easy validation of data freshness by setting expectations on the min and max values of timestamp columns. These Expectations can be defined and run using either GX Core or GX Cloud, allowing teams to ensure their data pipelines consistently deliver fresh and up-to-date information.

## Factors to consider when setting freshness thresholds

Use this checklist to guide your freshness threshold setting process:

1. **Start with business requirements**
   - [ ] Identify downstream data consumers
   - [ ] Assess impact of stale data
   - [ ] Define acceptable freshness levels with stakeholders

2. **Evaluate data source characteristics**
   - [ ] Determine expected update frequency
   - [ ] Identify sources with inherent delays
   - [ ] Align thresholds with source capabilities

3. **Analyze pipeline complexity**
   - [ ] Map data flow through pipeline
   - [ ] Estimate processing time per stage
   - [ ] Account for total pipeline lag

4. **Incorporate temporal patterns**
   - [ ] Identify cyclical patterns (daily, weekly, etc.)
   - [ ] Determine expected gaps or inactivity
   - [ ] Adjust for known patterns

5. **Ensure regulatory compliance**
   - [ ] Research industry-specific timeliness regulations
   - [ ] Verify thresholds meet mandatory requirements

For example, let's consider a retail case of validating sales transaction data. The business impact of stale data is high - it can lead to stockouts and lost revenue. The source systems (in-store PoS and ecommerce platform) provide near real-time data. The data pipeline has moderate complexity with several transformation steps. There are clear daily/weekly sales cycles, with lower volume overnight and on Sundays.

Balancing these factors, appropriate freshness thresholds for this retail sales data could be:
- During peak hours: latest transaction timestamp within last 30 minutes
- During off-hours: latest transaction timestamp within last 2 hours

The thresholds are aggressive enough to quickly detect stale data that could impact the business, while accommodating source system realities and known temporal patterns. They would be set using `ExpectColumnMaxToBeBetween` in a GX Checkpoint run hourly.

By systematically working through this checklist and considering a holistic set of factors, you can define optimal freshness thresholds for your specific data assets and use cases. GX enables codifying these thresholds and validating them consistently to drive data quality.

## Scenarios

The retail example illustrates the process of setting freshness thresholds based on specific business needs. To further understand the wide-ranging impact of data freshness validation, consider the following scenarios across different industries that you might encounter in your work.

### Social media analytics

**Context**: A marketing agency provides social media monitoring and analytics services to its clients. They collect real-time data from various social media platforms to track brand mentions, sentiment, and engagement metrics.

**GX Solution**: By applying freshness Expectations like `ExpectColumnMaxToBeBetween` and `ExpectColumnMinToBeBetween` to the timestamp columns in the social media data, the agency can verify that they are capturing and analyzing the most recent interactions and conversations. GX enables the agency to set appropriate freshness thresholds based on their clients' needs and the dynamic nature of social media.

### Smart manufacturing and predictive maintenance

**Context**: A manufacturing company has implemented IoT sensors on its production lines to collect real-time data on machine performance, vibration levels, temperature, and other parameters. This data is used to monitor equipment health, predict maintenance needs, and prevent unplanned downtime.

**GX Solution**: GX allows the company to validate the freshness of the sensor data using Expectations like `ExpectColumnMaxToBeBetween` and `ExpectColumnMinToBeBetween`. By setting up these Expectations in their data pipelines, the company can ensure that the data accurately reflects the current state of the machines, enabling predictive maintenance models to identify potential issues early and schedule maintenance proactively.

### Connected vehicles and real-time fleet management

**Context**: A fleet management company equips its vehicles with IoT devices that collect data on location, speed, fuel consumption, and driver behavior. This data is used to optimize routes, monitor vehicle performance, and ensure driver safety.

**GX Solution**: GX empowers the company to set up Expectations like `ExpectColumnMinToBeBetween` and `ExpectColumnMaxToBeBetween` on the timestamp columns in the vehicle data. By incorporating these Expectations into their data pipelines, the company can verify that the data from the connected vehicles is consistently fresh, enabling real-time fleet tracking and quick response to deviations from planned routes or aggressive driving patterns.

## Avoid common freshness analysis pitfalls

As these scenarios demonstrate, data freshness is critical across various domains. However, teams often encounter common pitfalls when implementing freshness validation:

- **Not defining clear SLAs for data freshness**: Teams should establish and document clear service level agreements (SLAs) for how fresh different datasets need to be. Without clear targets, it's easy for freshness to degrade over time.

- **Not considering end-to-end pipeline freshness**: Data can become stale at any stage, from ingestion through transformation. Account for total pipeline lag, not just source system freshness. Add validation checks after key processing steps.

- **Averaging timestamps can conceal issues**: Calculating the average timestamp can make the overall data appear misleadingly fresh. Use min/max values instead to bound the full range.

- **Sampling data inappropriately for freshness checks**: When validating freshness, be sure to check the full dataset or use a statistically valid sampling method. Cherry-picking rows can mask staleness issues.

## The path forward

By understanding these common freshness analysis pitfalls and adhering to best practices, you can ensure that your data pipelines consistently deliver fresh and reliable data to drive accurate insights and timely decision-making. Implementing robust freshness validation using GX empowers you to catch staleness issues early, prevent downstream problems, and maintain the highest standards of data quality.

As you progress on your data quality journey, remember that freshness is just one critical aspect of a comprehensive data quality strategy. By exploring our broader [data quality series](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md), you'll gain valuable insights into how other essential dimensions of data quality, such as accuracy, completeness, and consistency, can be seamlessly integrated into your workflows using GX.
