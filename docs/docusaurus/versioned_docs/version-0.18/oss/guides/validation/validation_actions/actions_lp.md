---
sidebar_label: 'Configure Actions'
title: 'Configure Actions'
hide_title: true
id: actions_lp
description: Configure Actions to send Validation Result notifications, update Data Docs, and store Validation Results.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  This is where you'll find information about using Actions to send Validation Result notifications, update Data Docs, and store Validation Results.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Trigger Email as an Action" description="Create an Action that sends an email with Validation Result information, including Validation success or failure" to="/docs/oss/guides/validation/validation_actions/how_to_trigger_email_as_a_validation_action" icon="/img/email_action_icon.svg" />
  <LinkCard topIcon label="Collect OpenLineage metadata" description="Use an Action to emit results to an OpenLineage backend" to="/docs/oss/guides/validation/validation_actions/how_to_collect_openlineage_metadata_using_a_validation_action" icon="/img/metadata_icon.svg" />
  <LinkCard topIcon label="Trigger Opsgenie notifications" description="Use an Action to create Opsgenie alert notifications" to="/docs/oss/guides/validation/validation_actions/how_to_trigger_opsgenie_notifications_as_a_validation_action" icon="/img/opsgenie_icon.svg" />
  <LinkCard topIcon label="Trigger Slack notifications" description="Use an Action to create Slack notifications for Validation Results" to="/docs/oss/guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action" icon="/img/slack_icon.svg" />
  <LinkCard topIcon label="Get Data Docs URLs for use in custom Validation Actions" description="Create a custom Validation Action that includes a link to Data Docs" to="/docs/oss/guides/validation/advanced/how_to_get_data_docs_urls_for_custom_validation_actions" icon="/img/data_doc_link_icon.svg" />
</LinkCardGrid>