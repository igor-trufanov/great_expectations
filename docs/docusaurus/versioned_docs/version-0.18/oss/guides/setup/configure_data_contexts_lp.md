---
sidebar_label: 'Configure Data Contexts'
title: 'Configure Data Contexts'
hide_title: true
id: configure_data_contexts_lp
description: Instantiate and convert a Data Context.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  This is where you'll find information for instantiating and converting a Data Context.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Instantiate a Data Context" description="Instantiate a Data Context so that you can continue working with previously defined GX configurations" to="/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context" icon="/img/instantiate_icon.svg" />
  <LinkCard topIcon label="Convert a Data Context" description="Convert an Ephemeral Data Context to a Filesystem Data Context" to="/docs/oss/guides/setup/configuring_data_contexts/how_to_convert_an_ephemeral_data_context_to_a_filesystem_data_context" icon="/img/convert_icon.svg" />
  <LinkCard topIcon label="Configure credentials" description="Populate credentials with an environment variable, a YAML file, or a secret manager" to="/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials" icon="/img/configure_icon.svg" />
</LinkCardGrid>