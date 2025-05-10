from setuptools import setup, find_packages

setup(
    name="fogverse",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiokafka==0.12.0",
        "async-timeout==5.0.1",
        "confluent-kafka==2.8.2",
        "numpy==2.2.3",
        "opencv-python==4.11.0.86",
        "packaging==24.2",
        "PyYAML==6.0.2",
        "setuptools==75.8.2",
        "typing_extensions==4.12.2"
    ],
)
