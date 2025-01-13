from setuptools import find_packages, setup

setup(
    name="dagster_weather",
    packages=find_packages(exclude=["dagster_weather_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
