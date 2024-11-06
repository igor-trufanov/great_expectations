---
sidebar_label: 'Add features to Custom Expectations'
title: 'Add features to Custom Expectations'
hide_title: true
id: add_features_custom_expectations_lp
description: Add additional functionality to your Custom Expectations.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Add additional functionality to your Custom Expectations.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Add comments to Expectations and display them in Data Docs" description="Add descriptive comments to Expectations and display them in Data Docs" to="/docs/oss/guides/expectations/advanced/how_to_add_comments_to_expectations_and_display_them_in_data_docs" icon="/img/comment_icon.svg" />
  <LinkCard topIcon label="Create example cases for a Custom Expectation" description="Add example cases to document and test the behavior of your Expectation" to="/docs/oss/guides/expectations/features_custom_expectations/how_to_add_example_cases_for_an_expectation" icon="/img/example_cases_icon.svg" />
  <LinkCard topIcon label="Add input validation and type checking to a Custom Expectation" description="Add validation and Type Checking to the input parameters of a Custom Expectation" to="/docs/oss/guides/expectations/features_custom_expectations/how_to_add_input_validation_for_an_expectation" icon="/img/validation_check_icon.svg" />
  <LinkCard topIcon label="Add Spark support for Custom Expectations" description="Add native Spark support for your Custom Expectations" to="/docs/oss/guides/expectations/features_custom_expectations/how_to_add_spark_support_for_an_expectation" icon="/img/spark_icon.png" />
  <LinkCard topIcon label="Add SQLAlchemy support for Custom Expectations" description="Add native SQLAlchemy support for your Custom Expectations" to="/docs/oss/guides/expectations/features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation" icon="/img/sqlalchemy_logo.png" />
  <LinkCard topIcon label="Add custom parameters to Custom Expectations" description="Add custom parameters to Custom Expectations" to="/docs/oss/guides/expectations/creating_custom_expectations/add_custom_parameters" icon="/img/parameter_icon.svg" />
  <LinkCard topIcon label="Add auto-initializing framework support to a Custom Expectation" description="Automates Expectation parameter estimation" to="/docs/oss/guides/expectations/creating_custom_expectations/how_to_add_support_for_the_auto_initializing_framework_to_a_custom_expectation" icon="/img/custom_expectation_icon.svg" />
</LinkCardGrid>