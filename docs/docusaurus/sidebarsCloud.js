module.exports = {
    gx_cloud: [
        {
            type: 'category',
            label: 'GX Cloud overview',
            link: { type: 'doc', id: 'overview/gx_cloud_overview' },
            items: [
                {
                    type: 'link',
                    label: 'GX Cloud concepts',
                    href: '/cloud/overview/gx_cloud_overview#gx-cloud-concepts',
                },
                {
                    type: 'link',
                    label: 'GX Cloud workflow',
                    href: '/cloud/overview/gx_cloud_overview#gx-cloud-workflow',
                },
                {
                    type: 'link',
                    label: 'GX Cloud architecture',
                    href: '/cloud/overview/gx_cloud_overview#gx-cloud-architecture',
                },
            ]
        },
        {
            type: 'category',
            label: 'Deploy GX Cloud',
            link: { type: 'doc', id: 'deploy/deploy_lp' },
            items: [
                'deploy/deployment_patterns',
                'deploy/deploy_gx_agent',
            ]
        },
        {
            type: 'category',
            label: 'Connect GX Cloud',
            link: { type: 'doc', id: 'connect/connect_lp' },
            items: [
                'connect/connect_postgresql',
                'connect/connect_snowflake',
                'connect/connect_databrickssql',
                'connect/connect_airflow',
                'connect/connect_python',
            ]
        },
        {
            type: 'category',
            label: 'Manage Data Assets',
            link: { type: 'doc', id: 'data_assets/manage_data_assets' },
            items: [
                {
                    type: 'link',
                    label: 'Create a Data Asset',
                    href: '/cloud/data_assets/manage_data_assets#create-a-data-asset',
                },
                {
                    type: 'link',
                    label: 'View Data Asset metrics',
                    href: '/cloud/data_assets/manage_data_assets#view-data-asset-metrics',
                },
                {
                    type: 'link',
                    label: 'Add an Expectation to a Data Asset column',
                    href: '/cloud/data_assets/manage_data_assets#add-an-expectation-to-a-data-asset-column',
                },
                {
                    type: 'link',
                    label: 'Add a Data Asset to an Existing Data Source',
                    href: '/cloud/data_assets/manage_data_assets#add-a-data-asset-to-an-existing-data-source',
                },
                {
                    type: 'link',
                    label: 'Edit Data Source settings',
                    href: '/cloud/data_assets/manage_data_assets#edit-data-source-settings',
                },
                {
                    type: 'link',
                    label: 'Edit a Data Asset',
                    href: '/cloud/data_assets/manage_data_assets#edit-a-data-asset',
                },
                {
                    type: 'link',
                    label: 'Data Source credential management',
                    href: '/cloud/data_assets/manage_data_assets#data-source-credential-management',
                },
                {
                    type: 'link',
                    label: 'Delete a Data Asset',
                    href: '/cloud/data_assets/manage_data_assets#delete-a-data-asset',
                },
            ]
        },
        {
            type: 'category',
            label: 'Manage Expectations',
            link: { type: 'doc', id: 'expectations/manage_expectations' },
            items: [
                {
                    type: 'link',
                    label: 'Available Expectations',
                    href: '/cloud/expectations/manage_expectations#available-expectations',
                },
                {
                    type: 'link',
                    label: 'Custom SQL Expectations',
                    href: '/cloud/expectations/manage_expectations#custom-sql-expectations',
                },
                {
                    type: 'link',
                    label: 'Dynamic Parameters',
                    href: '/cloud/expectations/manage_expectations#dynamic-parameters',
                },
                {
                    type: 'link',
                    label: 'Add an Expectation',
                    href: '/cloud/expectations/manage_expectations#add-an-expectation',
                },
                {
                    type: 'link',
                    label: 'Edit an Expectation',
                    href: '/cloud/expectations/manage_expectations#edit-an-expectation',
                },
                {
                    type: 'link',
                    label: 'View Expectation history',
                    href: '/cloud/expectations/manage_expectations#view-expectation-history',
                },
                {
                    type: 'link',
                    label: 'Delete an Expectation',
                    href: '/cloud/expectations/manage_expectations#delete-an-expectation',
                },
            ]
        },
        {
            type: 'category',
            label: 'Manage Expectation Suites',
            link: { type: 'doc', id: 'expectation_suites/manage_expectation_suites' },
            items: [
                {
                    type: 'link',
                    label: 'Create an Expectation Suite ',
                    href: '/cloud/expectation_suites/manage_expectation_suites#create-an-expectation-suite',
                },
                {
                    type: 'link',
                    label: 'Edit an Expectation Suite',
                    href: '/cloud/expectation_suites/manage_expectation_suites#edit-an-expectation-suite-name',
                },
                {
                    type: 'link',
                    label: 'Delete an Expectation Suite',
                    href: '/cloud/expectation_suites/manage_expectation_suites#delete-an-expectation-suite',
                },
            ]
        },
        {
            type: 'category',
            label: 'Manage Validations',
            link: { type: 'doc', id: 'validations/manage_validations' },
            items: [
                {
                    type: 'link',
                    label: 'Run a Validation',
                    href: '/cloud/validations/manage_validations#run-a-validation',
                },
                {
                    type: 'link',
                    label: 'Run a Validation on a Data Asset containing partitions',
                    href: '/cloud/validations/manage_validations#run-a-validation-on-a-data-asset-containing-partitions',
                },
                {
                    type: 'link',
                    label: 'View Validation run history',
                    href: '/cloud/validations/manage_validations#view-validation-run-history',
                },
            ]
        },
        {
            type: 'category',
            label: 'Manage schedules',
            link: { type: 'doc', id: 'schedules/manage_schedules' },
            items: [
                {
                    type: 'link',
                    label: 'Add a schedule',
                    href: '/cloud/schedules/manage_schedules#create-a-schedule-for-an-existing-expectation-suite',
                },
                {
                    type: 'link',
                    label: 'Edit a schedule',
                    href: '/cloud/schedules/manage_schedules#edit-a-schedule',
                },
                {
                    type: 'link',
                    label: 'Disable a schedule',
                    href: '/cloud/schedules/manage_schedules#disable-a-schedule',
                },
            ]
        },
        {
            type: 'category',
            label: 'Manage alerts',
            link: { type: 'doc', id: 'alerts/manage_alerts' },
            items: [
                {
                    type: "link",
                    label: "Email alert default settings",
                    href: "/cloud/alerts/manage_alerts#email-alert-default-settings"
                },
                {
                    type: "link",
                    label: "Update an email alert",
                    href: "/cloud/alerts/manage_alerts#update-an-email-alert"
                },
            ]
        },
        {
            type: 'category',
            label: 'Manage users and access tokens',
            link: { type: 'doc', id: 'users/manage_users' },
            items: [
                {
                    type: 'link',
                    label: 'Roles and responsibilities',
                    href: '/cloud/users/manage_users#roles-and-responsibilities',
                },
                {
                    type: 'link',
                    label: 'Invite a user',
                    href: '/cloud/users/manage_users#invite-a-user',
                },
                {
                    type: 'link',
                    label: 'Edit a user role',
                    href: '/cloud/users/manage_users#edit-a-user-role',
                },
                {
                    type: 'link',
                    label: 'Delete a user',
                    href: '/cloud/users/manage_users#delete-a-user',
                },
                {
                    type: 'link',
                    label: 'Create a user access token',
                    href: '/cloud/users/manage_users#create-a-user-access-token',
                },
                {
                    type: 'link',
                    label: 'Create an organization access token',
                    href: '/cloud/users/manage_users#create-an-organization-access-token',
                },
                {
                    type: 'link',
                    label: 'Delete a user or organization access token',
                    href: '/cloud/users/manage_users#delete-a-user-or-organization-access-token',
                },
            ]
        },
        {
            type: 'link',
            label: 'Request a demo for GX Cloud',
            href: 'https://www.greatexpectations.io/demo',
            className: 'request-demo-sidebar',
        },
    ]
}

