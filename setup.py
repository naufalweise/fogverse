from setuptools import setup, find_packages

setup(
    name="fogverse",
    version="0.0.2",
    description="A framework for distributed streaming and processing applications.",
    packages=find_packages(),
    install_requires=[
        "aiokafka",
        "confluent-kafka",
        "numpy",
        "opencv-python",
        "pyyaml",
    ],
    python_requires=">=3.8",
)
