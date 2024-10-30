import setuptools


def get_requirements() -> list[str]:
    with open("requirements.txt") as f:
        requirements = f.read().splitlines()
    return requirements


setuptools.setup(
    name="time_series_expectations",
    version="0.1.0",
    install_requires=get_requirements(),
)
