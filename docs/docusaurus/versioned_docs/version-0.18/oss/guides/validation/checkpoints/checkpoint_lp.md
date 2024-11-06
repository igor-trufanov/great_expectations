---
sidebar_label: 'Manage Checkpoints'
title: 'Manage Checkpoints'
hide_title: true
id: checkpoint_lp
description: Add validation data, create and configure Checkpoints, and pass in-mameory DataFrames.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  This is where you'll find information about managing your Checkpoints including adding validation data, creating and configuring Checkpoints, and passing in-memory DataFrames.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Create a new Checkpoint" description="Create a new Checkpoint" to="/docs/oss/guides/validation/checkpoints/how_to_create_a_new_checkpoint" icon="/img/checkpoint_icon.svg" />
  <LinkCard topIcon label="Add validation data or Expectation Suites to a Checkpoint" description="Add validation data or Expectation Suites to an existing Checkpoint" to="/docs/oss/guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint" icon="/img/validate_icon.svg" />
  <LinkCard topIcon label="Validate data with Expectations and Checkpoints" description="Pass an in-memory DataFrame to an existing Checkpoint" to="/docs/oss/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint" icon="/img/dataframe_checkpoint_icon.svg" />
  <LinkCard topIcon label="Deploy a scheduled Checkpoint with cron" description="Deploy a scheduled Checkpoint with cron" to="/docs/oss/guides/validation/advanced/how_to_deploy_a_scheduled_checkpoint_with_cron" icon="/img/deploy_icon.svg" />
</LinkCardGrid>