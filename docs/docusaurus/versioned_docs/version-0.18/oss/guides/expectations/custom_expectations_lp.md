---
sidebar_label: 'Create and manage Custom Expectations'
title: 'Create and manage Custom Expectations'
hide_title: true
id: custom_expectations_lp
description: Create and manage Custom Expectations.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Create Custom Expectations to extend GX functionality and solve unique business requirements.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Create a Custom Column Aggregate Expectation" description="Evaluates a single column and produces an aggregate Metric" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Column Map Expectation" description="Evaluates a single column and performs a yes or no query on every row in the column" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Batch Expectation" description="Evaluates an entire Batch, and answers a semantic question about the Batch" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Column Pair Map Expectation" description="Evaluates a pair of columns and performs a yes or no query about the row-wise relationship between the two columns" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Multicolumn Map Expectation" description="Evaluates a set of columns and performs a yes or no query about the row-wise relationship between the columns" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Regex-Based Column Map Expectation" description="Evaluates a single column and performs a yes or no regex-based query on every row in the column" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Set-Based Column Map Expectation" description="Evaluates a single column and determines if each row in the column belongs to the specified set" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_set_based_column_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Query Expectation" description="Runs Expectations against custom query results and makes intermediate queries to your database" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Parameterized Expectation" description="Inherits classes from existing Expectations and then creates a new customized Expectation" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_parameterized_expectations" icon="/img/custom_expectation_icon.svg" />
</LinkCardGrid>
