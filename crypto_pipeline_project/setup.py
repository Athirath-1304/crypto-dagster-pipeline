from setuptools import find_packages, setup

setup(
    name="crypto_pipeline_project",
    packages=find_packages(exclude=["crypto_pipeline_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
