from tests.integration.integration_test_fixture import IntegrationTestFixture

sqlite_integration_tests = []

connecting_to_your_data: list[IntegrationTestFixture] = []

partition_data: list[IntegrationTestFixture] = []

sample_data: list[IntegrationTestFixture] = []

sqlite_integration_tests += connecting_to_your_data
sqlite_integration_tests += partition_data
sqlite_integration_tests += sample_data
